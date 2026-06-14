use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions,
};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;

use crate::processor::{DetectedTrade, TradeOrigin, TradeType};

// ============================================
// 已知 DEX Program IDs
// ============================================
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPSWAP_PROGRAM: &str = "PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP";
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

// Pump.fun discriminators (Anchor sighash)
// buy  = sha256("global:buy")[..8]
// sell = sha256("global:sell")[..8]
const PUMPFUN_BUY_DISC: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const PUMPFUN_BUY_EXACT_SOL_IN_DISC: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
const PUMPFUN_SELL_DISC: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

// PumpSwap: 使用统一的 swap 指令，通过 token 方向判断 buy/sell
// swap discriminator = sha256("global:swap")[..8]
// 注意：这个值需要从链上交易验证，下面是 placeholder
const PUMPSWAP_SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

// Raydium CPMM Anchor discriminators
// swap_base_input  = sha256("global:swap_base_input")[..8]
// swap_base_output = sha256("global:swap_base_output")[..8]
const CPMM_SWAP_BASE_INPUT: [u8; 8] = [143, 190, 90, 218, 196, 30, 51, 222];
const CPMM_SWAP_BASE_OUTPUT: [u8; 8] = [55, 217, 98, 86, 163, 74, 180, 173];

// WSOL Mint
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// gRPC 订阅器，监听目标钱包的交易
pub struct GrpcSubscriber {
    grpc_url: String,
    grpc_token: Option<String>,
    target_wallets: Arc<RwLock<Vec<Pubkey>>>,
    subscription_notify: Arc<Notify>,
}

impl GrpcSubscriber {
    pub fn new(grpc_url: String, grpc_token: Option<String>, target_wallets: Vec<Pubkey>) -> Self {
        Self {
            grpc_url,
            grpc_token,
            target_wallets: Arc::new(RwLock::new(Self::normalize_wallets(target_wallets))),
            subscription_notify: Arc::new(Notify::new()),
        }
    }

    fn normalize_wallets(mut wallets: Vec<Pubkey>) -> Vec<Pubkey> {
        wallets.sort_by_key(|wallet| wallet.to_string());
        wallets.dedup();
        wallets
    }

    pub fn current_target_wallets(&self) -> Vec<Pubkey> {
        self.target_wallets.read().unwrap().clone()
    }

    pub fn update_target_wallets(&self, wallets: Vec<Pubkey>) -> bool {
        let normalized = Self::normalize_wallets(wallets);
        let mut current = self.target_wallets.write().unwrap();
        if *current == normalized {
            return false;
        }
        *current = normalized;
        drop(current);
        self.subscription_notify.notify_waiters();
        true
    }

    // ================================================================
    // 主入口：连接 Shyft gRPC 并持续接收交易流
    // ================================================================

    /// 启动 gRPC 订阅，将检测到的目标钱包 DEX 交易发送到 channel
    /// 连接断开后返回 Err，由调用方 (main.rs) 负责重连
    pub async fn subscribe(&self, tx_sender: mpsc::UnboundedSender<DetectedTrade>) -> Result<()> {
        while self.current_target_wallets().is_empty() {
            self.subscription_notify.notified().await;
        }

        let initial_wallets = self.current_target_wallets();
        info!(
            "Connecting to Shyft RabbitStream pre-exec at {} for {} target wallets",
            self.grpc_url,
            initial_wallets.len()
        );

        let mut client = GeyserGrpcClient::build_from_shared(self.grpc_url.clone())?
            .x_token(self.grpc_token.clone())?
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(60))
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await
            .context("Failed to connect to gRPC")?;

        info!("Shyft RabbitStream connected successfully");

        let subscribe_request = Self::build_subscribe_request(&initial_wallets);

        debug!("Sending gRPC subscription request...");
        let (mut subscribe_tx, mut stream) = client
            .subscribe_with_request(Some(subscribe_request))
            .await
            .context("Failed to create gRPC subscription")?;

        info!("RabbitStream subscription active, listening for target wallet transactions...");

        let ping_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });

        let mut total_received: u64 = 0;
        let mut total_matched: u64 = 0;
        let mut diagnosed = false;
        let start_time = Instant::now();
        let mut last_wallets = initial_wallets;
        let mut resub_interval = tokio::time::interval(Duration::from_millis(200));
        resub_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                msg = stream.next() => {
                    let Some(msg) = msg else {
                        break;
                    };

                    match msg {
                        Ok(update) => {
                            let recv_time = Instant::now();

                            match update.update_oneof {
                                Some(UpdateOneof::Transaction(tx_update)) => {
                                    total_received += 1;

                                    if total_received % 100 == 0 {
                                        let elapsed = start_time.elapsed().as_secs().max(1);
                                        debug!(
                                            "Stats: received={}, matched={}, uptime={}s, rate={:.1}/s",
                                            total_received,
                                            total_matched,
                                            elapsed,
                                            total_received as f64 / elapsed as f64,
                                        );
                                    }

                                    if let Some(ref tx_info) = tx_update.transaction {
                                        let meta = tx_info.meta.as_ref();
                                        let slot = tx_update.slot;

                                        if !diagnosed {
                                            diagnosed = true;
                                            if meta.is_some() {
                                                warn!(
                                                    "RabbitStream diagnostics: meta present -> processed-level stream | slot={}",
                                                    slot,
                                                );
                                            } else {
                                                info!(
                                                    "RabbitStream diagnostics: meta absent -> pre-exec stream | slot={}",
                                                    slot,
                                                );
                                            }
                                        }

                                        if let Some(ref tx_data) = tx_info.transaction {
                                            let message = tx_data.message.as_ref();

                                            match self.parse_transaction(tx_info, tx_data, message, meta, recv_time) {
                                                Ok(Some(trade)) => {
                                                    total_matched += 1;
                                                    let parse_latency = recv_time.elapsed();

                                                    info!(
                                                        "DETECTED: {} {} | wallet: {}..{} | mint: {} | sol: {:.4} | parse={}us | sig: {}..{}",
                                                        trade.trade_type,
                                                        if trade.is_buy { "BUY" } else { "SELL" },
                                                        &trade.source_wallet.to_string()[..4],
                                                        &trade.source_wallet.to_string()[trade.source_wallet.to_string().len() - 4..],
                                                        trade
                                                            .token_mint
                                                            .map(|mint| mint.to_string())
                                                            .unwrap_or_else(|| {
                                                                format!(
                                                                    "{}..{}",
                                                                    &trade.signature[..6],
                                                                    &trade.signature[trade.signature.len() - 4..]
                                                                )
                                                            }),
                                                        trade.sol_amount_lamports as f64 / 1e9,
                                                        parse_latency.as_micros(),
                                                        &trade.signature[..8],
                                                        &trade.signature[trade.signature.len()-4..],
                                                    );

                                                    if tx_sender.send(trade).is_err() {
                                                        error!("Trade channel closed, exiting subscriber");
                                                        return Ok(());
                                                    }
                                                }
                                                Ok(None) => {}
                                                Err(e) => {
                                                    debug!("Parse error (non-fatal): {}", e);
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(UpdateOneof::Ping(_)) => {
                                    debug!("gRPC keepalive ping");
                                }
                                Some(UpdateOneof::Pong(_)) => {
                                    debug!("gRPC pong");
                                }
                                Some(other) => {
                                    debug!(
                                        "gRPC other update type: {:?}",
                                        std::mem::discriminant(&other)
                                    );
                                }
                                None => {
                                    debug!("gRPC empty update");
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "gRPC stream error after {} messages ({} matched): {}",
                                total_received, total_matched, e
                            );
                            return Err(e.into());
                        }
                    }
                }
                _ = resub_interval.tick() => {
                    let current_wallets = self.current_target_wallets();
                    if current_wallets != last_wallets {
                        info!(
                            "Updating RabbitStream wallet subscription: {} -> {} wallets",
                            last_wallets.len(),
                            current_wallets.len()
                        );
                        let new_request = Self::build_subscribe_request(&current_wallets);
                        if let Err(e) = subscribe_tx.send(new_request).await {
                            error!("Failed to update gRPC wallet subscription: {}", e);
                        } else {
                            last_wallets = current_wallets;
                        }
                    }
                }
                _ = self.subscription_notify.notified() => {
                    let current_wallets = self.current_target_wallets();
                    if current_wallets != last_wallets {
                        info!(
                            "Updating RabbitStream wallet subscription: {} -> {} wallets",
                            last_wallets.len(),
                            current_wallets.len()
                        );
                        let new_request = Self::build_subscribe_request(&current_wallets);
                        if let Err(e) = subscribe_tx.send(new_request).await {
                            error!("Failed to update gRPC wallet subscription: {}", e);
                        } else {
                            last_wallets = current_wallets;
                        }
                    }
                }
            }
        }

        ping_task.abort();
        let elapsed = start_time.elapsed();
        warn!(
            "gRPC stream ended after {} messages ({} matched) | uptime: {:.1}s",
            total_received,
            total_matched,
            elapsed.as_secs_f64()
        );
        Ok(())
    }

    fn build_subscribe_request(target_wallets: &[Pubkey]) -> SubscribeRequest {
        let account_keys: Vec<String> = target_wallets.iter().map(|w| w.to_string()).collect();

        info!(
            "Subscribing to {} wallets: [{}]",
            account_keys.len(),
            account_keys
                .iter()
                .map(|k| {
                    let s = k.as_str();
                    format!("{}..{}", &s[..4], &s[s.len() - 4..])
                })
                .collect::<Vec<_>>()
                .join(", ")
        );

        let mut transactions = HashMap::new();
        transactions.insert(
            "target_wallets".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,   // RabbitStream 不过滤
                failed: None, // RabbitStream 预执行阶段不知道是否失败
                // account_include: 交易涉及任意一个目标钱包就推送（OR 逻辑）
                account_include: account_keys,
                account_exclude: vec![],
                // account_required 留空！设置后是 AND 逻辑，2个钱包时几乎不可能匹配
                account_required: vec![],
                signature: None,
            },
        );

        debug!(
            "订阅参数: vote=None, failed=None, commitment=None, accounts={}",
            transactions
                .get("target_wallets")
                .map(|t| t.account_include.len())
                .unwrap_or(0)
        );

        SubscribeRequest {
            transactions,
            // RabbitStream 不支持 commitment 过滤（预执行阶段无 commitment 概念）
            commitment: None,
            accounts: HashMap::new(),
            slots: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            accounts_data_slice: vec![],
            ping: None,
            ..Default::default()
        }
    }

    // ================================================================
    // 交易解析核心逻辑
    // ================================================================

    /// 从 gRPC proto Transaction 序列化为 Solana wire format
    /// 用于 Jito Backrun Bundle: [target_tx_bytes, our_tx_bytes]
    /// 使用 yellowstone-grpc-proto 官方转换函数，避免手动重建导致的序列化偏差
    /// 从 gRPC proto Transaction 序列化为 Solana wire format（用于 Jito Backrun Bundle）
    /// 仅在匹配成功后调用，避免非匹配交易的序列化开销
    fn serialize_transaction_from_proto(
        tx_data: &yellowstone_grpc_proto::prelude::Transaction,
    ) -> Option<Vec<u8>> {
        // 使用 yellowstone 官方的 proto → VersionedTransaction 转换
        let versioned_tx =
            match yellowstone_grpc_proto::convert_from::create_tx_versioned(tx_data.clone()) {
                Ok(tx) => tx,
                Err(e) => {
                    warn!("Backrun: proto→VersionedTransaction 转换失败: {}", e);
                    return None;
                }
            };

        // 序列化为 wire format（去掉冗余的反序列化验证，省 ~100µs）
        match bincode::serialize(&versioned_tx) {
            Ok(bytes) => {
                debug!("Backrun: 目标交易序列化 {}bytes", bytes.len());
                Some(bytes)
            }
            Err(e) => {
                warn!("Backrun: VersionedTransaction 序列化失败: {}", e);
                None
            }
        }
    }

    /// 解析 gRPC 接收到的原始交易
    ///
    /// 两轮扫描:
    ///   1. 顶层指令 (outer instructions) — 直接调用 DEX 的交易
    ///   2. 内部指令 (inner instructions / CPI) — 通过聚合器间接调用 DEX
    ///      例如 Jupiter route → 实际 swap 藏在 inner instructions 里
    fn parse_transaction(
        &self,
        tx_info: &yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
        tx_data: &yellowstone_grpc_proto::prelude::Transaction,
        message: Option<&yellowstone_grpc_proto::prelude::Message>,
        meta: Option<&yellowstone_grpc_proto::prelude::TransactionStatusMeta>,
        recv_time: Instant,
    ) -> Result<Option<DetectedTrade>> {
        let message = message.context("Missing transaction message")?;
        let target_wallets = self.current_target_wallets();

        // 提取目标交易的原始字节（用于构建 Jito Backrun Bundle）
        // 延迟到匹配成功后再序列化（避免非匹配交易的序列化开销）
        // raw_transaction_bytes 在匹配成功时通过 serialize_transaction_from_proto 填充

        // 提取 signature (base58)
        let signature = if !tx_info.signature.is_empty() {
            bs58::encode(&tx_info.signature).into_string()
        } else {
            "unknown".to_string()
        };

        // ============================================
        // 构建完整的 account keys 列表
        // = message.account_keys (static)
        // + meta.loaded_writable_addresses (ALT writable)
        // + meta.loaded_readonly_addresses (ALT readonly)
        //
        // 这一步很关键：很多交易使用 Address Lookup Table (ALT)
        // 指令中的 account index 可能指向 ALT 中的地址
        // ============================================
        let mut account_keys: Vec<Pubkey> = message
            .account_keys
            .iter()
            .filter_map(|k| {
                if k.len() == 32 {
                    <[u8; 32]>::try_from(k.as_slice())
                        .ok()
                        .map(Pubkey::new_from_array)
                } else {
                    None
                }
            })
            .collect();

        // 追加 ALT 解析出的额外地址
        // RabbitStream 下 ALT 地址为空是正常情况（预执行阶段无 meta）
        if let Some(m) = meta {
            let writable = &m.loaded_writable_addresses;
            let readonly = &m.loaded_readonly_addresses;
            if !writable.is_empty() || !readonly.is_empty() {
                for addr in writable.iter().chain(readonly.iter()) {
                    if addr.len() == 32 {
                        if let Ok(arr) = <[u8; 32]>::try_from(addr.as_slice()) {
                            account_keys.push(Pubkey::new_from_array(arr));
                        }
                    }
                }
            }
        }

        // 识别哪个目标钱包参与了这笔交易
        let source_wallet = match account_keys
            .iter()
            .find(|k| target_wallets.contains(k))
            .copied()
        {
            Some(w) => w,
            None => return Ok(None), // 目标钱包不在这笔交易中
        };

        // ============================================
        // 第一轮：扫描顶层指令 (outer instructions)
        // ============================================
        for ix in &message.instructions {
            let program_idx = ix.program_id_index as usize;
            if program_idx >= account_keys.len() {
                continue;
            }
            let program_id = account_keys[program_idx];

            if let Some(mut trade) = self.try_parse_instruction(
                &program_id,
                &ix.data,
                &ix.accounts,
                &account_keys,
                &signature,
                source_wallet,
                recv_time,
            )? {
                // 延迟序列化：仅在匹配成功后才序列化目标交易（省 ~400µs/非匹配交易）
                trade.raw_transaction_bytes =
                    Self::serialize_transaction_from_proto(tx_data).unwrap_or_default();
                // meta 为空 = 预执行（交易尚未被 leader 执行，可 Backrun）
                trade.is_pre_execution = meta.is_none();
                trade.execution_failed = Self::meta_failed(meta);
                self.enrich_trade_from_meta(&mut trade, message, meta, &account_keys);
                return Ok(Some(trade));
            }
        }

        // ============================================
        // 第 1.5 轮：CPI 检测 — account_keys 中发现 Pump.fun 但外部指令不匹配
        // 场景：通过 Sandwich Bot / 聚合器间接调用 Pump.fun
        // 预执行阶段无 meta（无 inner instructions），但 account_keys 完整
        // 通过 account_keys 中的 Pump.fun 程序 ID + WSOL ATA 推断 buy/sell
        // ============================================
        let pumpfun_pubkey = Pubkey::from_str(PUMPFUN_PROGRAM).unwrap();
        if account_keys.contains(&pumpfun_pubkey) && meta.is_none() {
            // account_keys 中有 Pump.fun 程序 → CPI 调用
            // 提取 mint：扫描 account_keys 中的非系统地址，验证 bonding curve PDA
            if let Some(cpi_trade) = self.try_detect_cpi_pumpfun(
                &account_keys,
                &signature,
                source_wallet,
                meta,
                tx_data,
                recv_time,
            ) {
                return Ok(Some(cpi_trade));
            }
        }

        // ============================================
        // 第二轮：扫描 inner instructions (CPI 调用)
        // 场景：通过 Jupiter 等聚合器间接调用 Pump.fun
        // ============================================
        if let Some(m) = meta {
            for inner_group in &m.inner_instructions {
                for inner_ix in &inner_group.instructions {
                    let program_idx = inner_ix.program_id_index as usize;
                    if program_idx >= account_keys.len() {
                        continue;
                    }
                    let program_id = account_keys[program_idx];

                    if let Some(mut trade) = self.try_parse_instruction(
                        &program_id,
                        &inner_ix.data,
                        &inner_ix.accounts,
                        &account_keys,
                        &signature,
                        source_wallet,
                        recv_time,
                    )? {
                        trade.raw_transaction_bytes =
                            Self::serialize_transaction_from_proto(tx_data).unwrap_or_default();
                        // inner instructions 来自 meta，所以交易已执行，不可 Backrun
                        trade.is_pre_execution = false;
                        trade.execution_failed = Self::meta_failed(meta);
                        self.enrich_trade_from_meta(&mut trade, message, meta, &account_keys);
                        return Ok(Some(trade));
                    }
                }
            }
        }

        Ok(None) // 没有找到已知 DEX 的 swap 指令
    }

    // ================================================================
    // 单条指令解析
    // ================================================================

    /// 尝试解析单条指令，判断是否为已知 DEX 的 swap 操作
    /// 同时适用于顶层指令和 inner instructions
    fn try_parse_instruction(
        &self,
        program_id: &Pubkey,
        data: &[u8],
        account_indices: &[u8],
        all_account_keys: &[Pubkey],
        signature: &str,
        source_wallet: Pubkey,
        recv_time: Instant,
    ) -> Result<Option<DetectedTrade>> {
        let program_str = program_id.to_string();

        // 只匹配 Pump.fun 内盘，其他 DEX 全部跳过
        let trade_type = match program_str.as_str() {
            PUMPFUN_PROGRAM => Some(TradeType::Pumpfun),
            _ => None,
        };

        let trade_type = match trade_type {
            Some(t) => t,
            None => return Ok(None), // 非 Pump.fun，跳过
        };

        // 解析 buy/sell 方向
        let is_buy = self.detect_buy_or_sell(data, &trade_type, account_indices, all_account_keys);

        // 提取指令涉及的 account keys（用于后续 processor 重建指令）
        let instruction_account_slots: Vec<Option<Pubkey>> = account_indices
            .iter()
            .map(|&idx| all_account_keys.get(idx as usize).copied())
            .collect();
        let instruction_accounts: Vec<Pubkey> = if instruction_account_slots
            .iter()
            .all(|account| account.is_some())
        {
            instruction_account_slots
                .iter()
                .filter_map(|account| *account)
                .collect()
        } else {
            Vec::new()
        };

        let sol_amount_lamports = if is_buy {
            Self::extract_buy_lamports(&trade_type, data)
        } else {
            0
        };
        let token_mint = Self::extract_token_mint(&trade_type, &instruction_account_slots);
        let token_program = Self::extract_token_program(
            &trade_type,
            &instruction_account_slots,
            all_account_keys,
            token_mint.as_ref(),
        );

        let mut trade = DetectedTrade {
            signature: signature.to_string(),
            source_wallet,
            trade_type,
            trade_origin: TradeOrigin::Direct,
            is_buy,
            program_id: *program_id,
            instruction_data: data.to_vec(),
            instruction_accounts,
            all_account_keys: all_account_keys.to_vec(),
            detected_at: recv_time,
            sol_amount_lamports,
            raw_transaction_bytes: Vec::new(), // 由 parse_transaction 填充
            is_pre_execution: false,           // 由 parse_transaction 根据 meta 设置
            execution_failed: false,
            token_mint: None, // 直接调用场景由 main.rs extract_token_info 提取
            token_program,
        };
        trade.token_mint = token_mint;

        Ok(Some(trade))
    }

    // ================================================================
    // CPI 检测：通过 account_keys 识别经 Sandwich Bot 路由的 Pump.fun 交易
    // ================================================================

    /// 从 account_keys 中检测经 CPI 调用的 Pump.fun 交易
    /// 原理：即使外部程序不是 Pump.fun，account_keys 中一定包含 Pump.fun 程序ID
    ///       + bonding curve PDA + mint，可以在预执行阶段识别
    fn try_detect_cpi_pumpfun(
        &self,
        account_keys: &[Pubkey],
        signature: &str,
        source_wallet: Pubkey,
        meta: Option<&yellowstone_grpc_proto::prelude::TransactionStatusMeta>,
        tx_data: &yellowstone_grpc_proto::prelude::Transaction,
        recv_time: Instant,
    ) -> Option<DetectedTrade> {
        let pumpfun_pubkey = Pubkey::from_str(PUMPFUN_PROGRAM).ok()?;
        let wsol_mint = Pubkey::from_str(WSOL_MINT).ok()?;
        let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").ok()?;
        let token_2022 = Pubkey::from_str(TOKEN_2022_PROGRAM).ok()?;
        let target_wallets = self.current_target_wallets();

        // 在 account_keys 中寻找 mint：
        // Pump.fun bonding curve PDA = find_program_address([b"bonding-curve", mint], pumpfun_program)
        // 扫描所有非系统地址，验证其 bonding curve PDA 是否也在 account_keys 中
        let mut found_mint: Option<Pubkey> = None;
        let system_addresses = [
            pumpfun_pubkey,
            wsol_mint,
            solana_sdk::system_program::id(),
            token_program,
            token_2022,
            Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").ok()?,
            Pubkey::from_str("SysvarRent111111111111111111111111111111111").ok()?,
            Pubkey::from_str("11111111111111111111111111111111").ok()?,
            Pubkey::from_str("ComputeBudget111111111111111111111111111111").ok()?,
            // Pump.fun 已知固定地址
            Pubkey::from_str("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf").ok()?, // GLOBAL
            Pubkey::from_str("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9i").ok()?,  // FEE_RECIPIENT
            Pubkey::from_str("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1").ok()?, // EVENT_AUTHORITY
        ];

        for candidate in account_keys {
            // 跳过已知系统/固定地址
            if system_addresses.contains(candidate) {
                continue;
            }
            // 跳过目标钱包自己
            if target_wallets.contains(candidate) {
                continue;
            }

            // 验证：candidate 作为 mint，其 bonding curve PDA 是否也在 account_keys 中
            let (bc_pda, _) = Pubkey::find_program_address(
                &[b"bonding-curve", candidate.as_ref()],
                &pumpfun_pubkey,
            );
            if account_keys.contains(&bc_pda) {
                found_mint = Some(*candidate);
                break;
            }
        }

        let mint = found_mint?;
        let (bonding_curve, _) =
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &pumpfun_pubkey);
        let detected_token_program = Self::detect_pumpfun_token_program(
            account_keys,
            &mint,
            &bonding_curve,
            &token_program,
            &token_2022,
        );

        // 判断 buy/sell：检查目标钱包的 WSOL ATA 是否在 account_keys 中
        let wsol_ata =
            spl_associated_token_account::get_associated_token_address(&source_wallet, &wsol_mint);
        let is_buy = account_keys.contains(&wsol_ata);

        // 构建 instruction_accounts（从 account_keys 中提取 Pump.fun 相关账户）
        // CPI 场景下无法精确还原 instruction 的 account 顺序
        // 用完整 account_keys 传递给后续处理器
        info!(
            "CPI DETECTED: Pump.fun {} via wrapper | wallet: {}..{} | mint: {} | tp: {} | sig: {}..{}",
            if is_buy { "BUY" } else { "SELL" },
            &source_wallet.to_string()[..4],
            &source_wallet.to_string()[source_wallet.to_string().len() - 4..],
            mint,
            &detected_token_program.to_string()[..12],
            &signature[..8],
            &signature[signature.len() - 4..],
        );

        Some(DetectedTrade {
            signature: signature.to_string(),
            source_wallet,
            trade_type: TradeType::Pumpfun,
            trade_origin: TradeOrigin::WrapperCpi,
            is_buy,
            program_id: pumpfun_pubkey,
            instruction_data: Vec::new(), // CPI 场景无法提取 Pump.fun 指令数据
            instruction_accounts: Vec::new(),
            all_account_keys: account_keys.to_vec(),
            detected_at: recv_time,
            sol_amount_lamports: 0, // CPI 场景无法提取 SOL 金额
            raw_transaction_bytes: Self::serialize_transaction_from_proto(tx_data)
                .unwrap_or_default(),
            is_pre_execution: meta.is_none(),
            execution_failed: Self::meta_failed(meta),
            token_mint: Some(mint), // CPI 检测已通过 PDA 验证识别了 mint
            token_program: Some(detected_token_program),
        })
    }

    fn extract_token_mint(
        trade_type: &TradeType,
        instruction_account_slots: &[Option<Pubkey>],
    ) -> Option<Pubkey> {
        match trade_type {
            TradeType::Pumpfun => instruction_account_slots.get(2).copied().flatten(),
            _ => None,
        }
    }

    fn extract_token_program(
        trade_type: &TradeType,
        instruction_account_slots: &[Option<Pubkey>],
        all_account_keys: &[Pubkey],
        token_mint: Option<&Pubkey>,
    ) -> Option<Pubkey> {
        match trade_type {
            TradeType::Pumpfun => {
                if let Some(token_program) =
                    instruction_account_slots.get(8).copied().flatten()
                {
                    return Some(token_program);
                }

                let mint = token_mint.copied()?;
                let bonding_curve = instruction_account_slots.get(3).copied().flatten()?;
                let token_program = Pubkey::from_str(
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                )
                .ok()?;
                let token_2022 = Pubkey::from_str(TOKEN_2022_PROGRAM).ok()?;

                Some(Self::detect_pumpfun_token_program(
                    all_account_keys,
                    &mint,
                    &bonding_curve,
                    &token_program,
                    &token_2022,
                ))
            }
            _ => None,
        }
    }

    fn detect_pumpfun_token_program(
        account_keys: &[Pubkey],
        mint: &Pubkey,
        bonding_curve: &Pubkey,
        token_program: &Pubkey,
        token_2022: &Pubkey,
    ) -> Pubkey {
        let associated_legacy =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                bonding_curve,
                mint,
                token_program,
            );
        if account_keys.contains(&associated_legacy) {
            return *token_program;
        }

        let associated_2022 =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                bonding_curve,
                mint,
                token_2022,
            );
        if account_keys.contains(&associated_2022) {
            return *token_2022;
        }

        if account_keys.contains(token_2022) && !account_keys.contains(token_program) {
            return *token_2022;
        }

        *token_program
    }

    // ================================================================
    // Buy / Sell 方向检测
    // ================================================================

    /// 根据指令数据 discriminator + account 布局判断交易方向
    ///
    /// 返回 true = BUY (SOL → Token), false = SELL (Token → SOL)
    fn detect_buy_or_sell(
        &self,
        data: &[u8],
        trade_type: &TradeType,
        account_indices: &[u8],
        all_account_keys: &[Pubkey],
    ) -> bool {
        if data.len() < 8 {
            return true; // 数据不足，默认 buy
        }

        let disc = &data[..8];

        match trade_type {
            // ========================================
            // Pump.fun
            // 有明确的 buy/sell 两个独立指令
            // ========================================
            TradeType::Pumpfun => {
                if disc == PUMPFUN_BUY_DISC || disc == PUMPFUN_BUY_EXACT_SOL_IN_DISC {
                    true
                } else if disc == PUMPFUN_SELL_DISC {
                    false
                } else {
                    debug!("Unknown Pumpfun discriminator: {:?}", disc);
                    true
                }
            }

            // ========================================
            // PumpSwap
            // 统一 swap 指令，通过 instruction data 中的
            // base_amount_in / quote_amount_in 判断方向:
            //   quote_in > 0, base_in == 0 → SOL 买入 token (BUY)
            //   base_in > 0, quote_in == 0 → token 卖出换 SOL (SELL)
            //
            // data 布局:
            //   [0..8]   discriminator
            //   [8..16]  base_amount_in  (u64, token 数量)
            //   [16..24] quote_amount_in (u64, SOL lamports)
            // ========================================
            TradeType::PumpSwap => {
                if data.len() >= 24 {
                    let base_in = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
                    let quote_in = u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]));

                    if quote_in > 0 && base_in == 0 {
                        true // SOL → Token = BUY
                    } else if base_in > 0 && quote_in == 0 {
                        false // Token → SOL = SELL
                    } else {
                        debug!(
                            "PumpSwap direction ambiguous: base_in={}, quote_in={}",
                            base_in, quote_in
                        );
                        true // 默认 buy
                    }
                } else {
                    debug!("PumpSwap data too short: {} bytes", data.len());
                    true
                }
            }

            // ========================================
            // Raydium AMM V4
            // 第一个字节是指令 index:
            //   9  = SwapBaseIn
            //   11 = SwapBaseOut
            //
            // account 布局:
            //   index 15 = user_source_token_account
            //   index 16 = user_destination_token_account
            //
            // 如果 user_source 是 WSOL ATA → buy
            // 如果 user_source 是 token ATA → sell
            // ========================================
            TradeType::RaydiumAmm => {
                let ix_index = data[0];
                match ix_index {
                    9 | 11 => {
                        // SwapBaseIn / SwapBaseOut
                        self.is_sol_input(account_indices, all_account_keys, 15)
                    }
                    _ => {
                        debug!("Raydium AMM unknown instruction index: {}", ix_index);
                        true
                    }
                }
            }

            // ========================================
            // Raydium CPMM
            // Anchor discriminator 区分指令类型
            //
            // account 布局:
            //   index 4 = user_input_token_account
            //   index 5 = user_output_token_account
            //
            // 如果 user_input 是 WSOL ATA → buy
            // ========================================
            TradeType::RaydiumCpmm => {
                if disc == CPMM_SWAP_BASE_INPUT || disc == CPMM_SWAP_BASE_OUTPUT {
                    self.is_sol_input(account_indices, all_account_keys, 4)
                } else {
                    debug!("Unknown CPMM discriminator: {:?}", disc);
                    true
                }
            }
        }
    }

    /// 判断指定位置的 account 是否是 WSOL ATA
    ///
    /// 原理: 检查目标钱包的 WSOL ATA (由 wallet + WSOL mint 确定性派生)
    ///       是否匹配 swap 指令中 source token account 的位置
    ///
    /// 优点: 零网络请求，纯本地计算
    /// 缺点: 只能匹配目标钱包自己的 WSOL ATA
    ///        如果是 wrap SOL 到临时账户再 swap 的情况可能误判
    fn is_sol_input(
        &self,
        account_indices: &[u8],
        all_account_keys: &[Pubkey],
        source_position: usize,
    ) -> bool {
        // 安全检查
        if source_position >= account_indices.len() {
            return true; // 兜底默认 buy
        }
        let source_idx = account_indices[source_position] as usize;
        if source_idx >= all_account_keys.len() {
            return true;
        }

        let source_account = all_account_keys[source_idx];
        let wsol_mint = Pubkey::from_str(WSOL_MINT).unwrap();

        // 检查是否匹配任何目标钱包的 WSOL ATA
        let target_wallets = self.current_target_wallets();
        for wallet in &target_wallets {
            let wsol_ata =
                spl_associated_token_account::get_associated_token_address(wallet, &wsol_mint);
            if source_account == wsol_ata {
                return true; // source 是 WSOL ATA → 用 SOL 买入 → BUY
            }
        }

        // source 不是 WSOL ATA → 大概率是 token ATA → SELL
        false
    }

    fn extract_buy_lamports(trade_type: &TradeType, data: &[u8]) -> u64 {
        if data.len() < 24 {
            return 0;
        }

        match trade_type {
            TradeType::Pumpfun => {
                let disc = &data[..8];
                if disc == PUMPFUN_BUY_DISC {
                    u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]))
                } else if disc == PUMPFUN_BUY_EXACT_SOL_IN_DISC {
                    u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]))
                } else {
                    0
                }
            }
            TradeType::PumpSwap => u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8])),
            _ => 0,
        }
    }

    fn meta_failed(meta: Option<&yellowstone_grpc_proto::prelude::TransactionStatusMeta>) -> bool {
        meta.and_then(|status| status.err.as_ref()).is_some()
    }

    fn enrich_trade_from_meta(
        &self,
        trade: &mut DetectedTrade,
        message: &yellowstone_grpc_proto::prelude::Message,
        meta: Option<&yellowstone_grpc_proto::prelude::TransactionStatusMeta>,
        all_account_keys: &[Pubkey],
    ) {
        if !trade.is_buy || trade.sol_amount_lamports > 0 {
            return;
        }

        let Some(meta) = meta else {
            return;
        };

        if let Some(lamports) = Self::derive_buy_lamports_from_meta(
            meta,
            message,
            all_account_keys,
            trade.source_wallet,
        ) {
            trade.sol_amount_lamports = lamports;
        }
    }

    fn derive_buy_lamports_from_meta(
        meta: &yellowstone_grpc_proto::prelude::TransactionStatusMeta,
        message: &yellowstone_grpc_proto::prelude::Message,
        all_account_keys: &[Pubkey],
        source_wallet: Pubkey,
    ) -> Option<u64> {
        let mut transfer_total = 0u64;

        for ix in &message.instructions {
            transfer_total = transfer_total.saturating_add(Self::extract_system_transfer_lamports(
                ix.program_id_index as usize,
                &ix.accounts,
                &ix.data,
                all_account_keys,
                source_wallet,
            ));
        }

        for inner_group in &meta.inner_instructions {
            for inner_ix in &inner_group.instructions {
                transfer_total =
                    transfer_total.saturating_add(Self::extract_system_transfer_lamports(
                        inner_ix.program_id_index as usize,
                        &inner_ix.accounts,
                        &inner_ix.data,
                        all_account_keys,
                        source_wallet,
                    ));
            }
        }

        if transfer_total > 0 {
            return Some(transfer_total);
        }

        let source_index = all_account_keys.iter().position(|k| *k == source_wallet)?;
        let pre = *meta.pre_balances.get(source_index)?;
        let post = *meta.post_balances.get(source_index)?;
        let delta = pre.saturating_sub(post);
        if delta == 0 {
            return None;
        }

        Some(delta.saturating_sub(meta.fee))
    }

    fn extract_system_transfer_lamports(
        program_index: usize,
        account_indices: &[u8],
        data: &[u8],
        all_account_keys: &[Pubkey],
        source_wallet: Pubkey,
    ) -> u64 {
        if program_index >= all_account_keys.len()
            || all_account_keys[program_index] != solana_sdk::system_program::id()
            || account_indices.is_empty()
            || data.len() < 12
        {
            return 0;
        }

        let source_index = account_indices[0] as usize;
        if source_index >= all_account_keys.len() || all_account_keys[source_index] != source_wallet
        {
            return 0;
        }

        let instruction = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4]));
        if instruction != 2 {
            return 0;
        }

        u64::from_le_bytes(data[4..12].try_into().unwrap_or([0; 8]))
    }
}

// ================================================================
// 单元测试
// ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_subscriber() -> GrpcSubscriber {
        GrpcSubscriber::new("https://test.grpc.shyft.to".to_string(), None, vec![])
    }

    #[test]
    fn test_pumpfun_buy_discriminator() {
        let sub = make_subscriber();
        let mut data = vec![0u8; 24];
        data[..8].copy_from_slice(&PUMPFUN_BUY_DISC);

        assert!(sub.detect_buy_or_sell(&data, &TradeType::Pumpfun, &[], &[]));
    }

    #[test]
    fn test_pumpfun_sell_discriminator() {
        let sub = make_subscriber();
        let mut data = vec![0u8; 24];
        data[..8].copy_from_slice(&PUMPFUN_SELL_DISC);

        assert!(!sub.detect_buy_or_sell(&data, &TradeType::Pumpfun, &[], &[]));
    }

    #[test]
    fn test_pumpswap_buy_direction() {
        let sub = make_subscriber();
        let mut data = vec![0u8; 24];
        data[..8].copy_from_slice(&PUMPSWAP_SWAP_DISC);
        data[8..16].copy_from_slice(&0u64.to_le_bytes()); // base_in = 0
        data[16..24].copy_from_slice(&1_000_000u64.to_le_bytes()); // quote_in > 0

        assert!(sub.detect_buy_or_sell(&data, &TradeType::PumpSwap, &[], &[]));
    }

    #[test]
    fn test_pumpswap_sell_direction() {
        let sub = make_subscriber();
        let mut data = vec![0u8; 24];
        data[..8].copy_from_slice(&PUMPSWAP_SWAP_DISC);
        data[8..16].copy_from_slice(&500_000u64.to_le_bytes()); // base_in > 0
        data[16..24].copy_from_slice(&0u64.to_le_bytes()); // quote_in = 0

        assert!(!sub.detect_buy_or_sell(&data, &TradeType::PumpSwap, &[], &[]));
    }

    #[test]
    fn test_raydium_amm_swap_base_in() {
        let sub = make_subscriber();
        let data = [9u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        // 没有足够 account 数据时默认 buy (true)
        assert!(sub.detect_buy_or_sell(&data, &TradeType::RaydiumAmm, &[], &[]));
    }

    #[test]
    fn test_raydium_cpmm_swap_base_input() {
        let sub = make_subscriber();
        let mut data = vec![0u8; 24];
        data[..8].copy_from_slice(&CPMM_SWAP_BASE_INPUT);

        // 没有足够 account 数据时默认 buy (true)
        assert!(sub.detect_buy_or_sell(&data, &TradeType::RaydiumCpmm, &[], &[]));
    }

    #[test]
    fn test_short_data_defaults_to_buy() {
        let sub = make_subscriber();
        let data = [1u8, 2, 3]; // 不足 8 字节

        assert!(sub.detect_buy_or_sell(&data, &TradeType::Pumpfun, &[], &[]));
        assert!(sub.detect_buy_or_sell(&data, &TradeType::PumpSwap, &[], &[]));
        assert!(sub.detect_buy_or_sell(&data, &TradeType::RaydiumAmm, &[], &[]));
        assert!(sub.detect_buy_or_sell(&data, &TradeType::RaydiumCpmm, &[], &[]));
    }
}
