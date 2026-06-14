use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;

use crate::processor::pumpfun::BondingCurveState;

const ACCOUNT_GRPC_CONNECT_TIMEOUT_SECS: u64 = 10;
const ACCOUNT_GRPC_STREAM_TIMEOUT_SECS: u64 = 60;
const ACCOUNT_GRPC_HTTP2_KEEPALIVE_SECS: u64 = 15;
const ACCOUNT_GRPC_KEEPALIVE_TIMEOUT_SECS: u64 = 5;
const ACCOUNT_GRPC_TCP_KEEPALIVE_SECS: u64 = 30;

// ============================================
// 账户变化事件
// ============================================

/// bonding curve 账户变化事件
#[derive(Debug, Clone)]
pub struct BondingCurveUpdate {
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub state: BondingCurveState,
    pub slot: u64,
}

/// ATA 余额变化事件（用于交易确认）
#[derive(Debug, Clone)]
pub struct AtaBalanceUpdate {
    pub ata_address: Pubkey,
    pub mint: Pubkey,
    pub amount: u64,
    pub slot: u64,
}

/// 统一的账户更新事件
#[derive(Debug, Clone)]
pub enum AccountUpdate {
    BondingCurve(BondingCurveUpdate),
    AtaBalance(AtaBalanceUpdate),
}

// ============================================
// 实时状态缓存
// ============================================

/// 缓存 bonding curve 实时状态（gRPC 推送更新）
#[derive(Clone)]
pub struct BondingCurveCache {
    /// mint → BondingCurveState
    cache: Arc<DashMap<Pubkey, BondingCurveState>>,
}

impl BondingCurveCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, mint: &Pubkey) -> Option<BondingCurveState> {
        self.cache.get(mint).map(|v| v.clone())
    }

    pub fn update(&self, mint: &Pubkey, state: BondingCurveState) {
        self.cache.insert(*mint, state);
    }

    pub fn remove(&self, mint: &Pubkey) {
        self.cache.remove(mint);
    }
}

/// ATA 余额缓存（gRPC 推送更新，卖出时直接读取）
#[derive(Clone)]
pub struct AtaBalanceCache {
    /// mint → token amount
    cache: Arc<DashMap<Pubkey, u64>>,
}

impl AtaBalanceCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, mint: &Pubkey) -> Option<u64> {
        self.cache.get(mint).map(|v| *v)
    }

    pub fn update(&self, mint: &Pubkey, amount: u64) {
        self.cache.insert(*mint, amount);
    }

    pub fn remove(&self, mint: &Pubkey) {
        self.cache.remove(mint);
    }
}

// ============================================
// gRPC 账户订阅器
// ============================================

/// 动态订阅 Solana 账户变化（bonding curve + ATA）
/// 通过 gRPC 推送实时更新，替代轮询
pub struct AccountSubscriber {
    grpc_url: String,
    grpc_token: Option<String>,
    bonding_curve_to_mint: Arc<DashMap<Pubkey, Pubkey>>,
    ata_to_mint: Arc<DashMap<Pubkey, Pubkey>>,
    mint_last_touched: Arc<DashMap<Pubkey, Instant>>,
    subscribed_accounts: Arc<DashMap<Pubkey, AccountType>>,
    subscription_notify: Arc<Notify>,
    bc_cache: BondingCurveCache,
    ata_cache: AtaBalanceCache,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccountType {
    BondingCurve,
    Ata,
}

impl AccountSubscriber {
    pub fn new(
        grpc_url: String,
        grpc_token: Option<String>,
        bc_cache: BondingCurveCache,
        ata_cache: AtaBalanceCache,
    ) -> Self {
        Self {
            grpc_url,
            grpc_token,
            bonding_curve_to_mint: Arc::new(DashMap::new()),
            ata_to_mint: Arc::new(DashMap::new()),
            mint_last_touched: Arc::new(DashMap::new()),
            subscribed_accounts: Arc::new(DashMap::new()),
            subscription_notify: Arc::new(Notify::new()),
            bc_cache,
            ata_cache,
        }
    }

    /// 添加 bonding curve 账户到订阅列表
    pub fn track_bonding_curve(&self, mint: Pubkey, bonding_curve: Pubkey) {
        self.bonding_curve_to_mint.insert(bonding_curve, mint);
        self.mint_last_touched.insert(mint, Instant::now());
        self.subscribed_accounts
            .insert(bonding_curve, AccountType::BondingCurve);
        debug!(
            "Tracking bonding curve: {} for mint: {}",
            &bonding_curve.to_string()[..12],
            &mint.to_string()[..12],
        );
        self.subscription_notify.notify_one();
    }

    /// 添加 ATA 账户到订阅列表（用于确认买入是否成功）
    pub fn track_ata(&self, mint: Pubkey, ata_address: Pubkey) {
        self.ata_to_mint.insert(ata_address, mint);
        self.mint_last_touched.insert(mint, Instant::now());
        self.subscribed_accounts
            .insert(ata_address, AccountType::Ata);
        debug!(
            "Tracking ATA: {} for mint: {}",
            &ata_address.to_string()[..12],
            &mint.to_string()[..12],
        );
        self.subscription_notify.notify_one();
    }

    /// 停止追踪某个代币相关的所有账户
    pub fn untrack_mint(&self, mint: &Pubkey) {
        // 移除 bonding curve
        let mut bc_to_remove = Vec::new();
        for entry in self.bonding_curve_to_mint.iter() {
            if entry.value() == mint {
                bc_to_remove.push(*entry.key());
            }
        }
        for bc in &bc_to_remove {
            self.bonding_curve_to_mint.remove(bc);
            self.subscribed_accounts.remove(bc);
        }

        // 移除 ATA
        let mut ata_to_remove = Vec::new();
        for entry in self.ata_to_mint.iter() {
            if entry.value() == mint {
                ata_to_remove.push(*entry.key());
            }
        }
        for ata in &ata_to_remove {
            self.ata_to_mint.remove(ata);
            self.subscribed_accounts.remove(ata);
        }

        // 移除缓存
        self.bc_cache.remove(mint);
        self.ata_cache.remove(mint);
        self.mint_last_touched.remove(mint);

        debug!(
            "Untracked all accounts for mint: {}",
            &mint.to_string()[..12]
        );
    }

    /// 获取当前所有订阅的账户地址
    pub fn prune_stale_candidate_mints(
        &self,
        active_mints: &HashSet<Pubkey>,
        max_idle: Duration,
    ) -> usize {
        let stale_mints: Vec<Pubkey> = self
            .mint_last_touched
            .iter()
            .filter_map(|entry| {
                let mint = *entry.key();
                if active_mints.contains(&mint) || entry.value().elapsed() < max_idle {
                    None
                } else {
                    Some(mint)
                }
            })
            .collect();

        for mint in &stale_mints {
            self.untrack_mint(mint);
        }

        stale_mints.len()
    }

    fn get_subscribed_addresses(&self) -> Vec<String> {
        self.subscribed_accounts
            .iter()
            .map(|entry| entry.key().to_string())
            .collect()
    }

    /// 启动 gRPC 账户订阅流
    /// 持续接收账户变化推送，更新缓存并发送事件
    /// 每秒检查是否有新账户需要订阅，动态更新 gRPC 订阅
    pub async fn subscribe(&self, update_tx: mpsc::UnboundedSender<AccountUpdate>) -> Result<()> {
        // 等待直到有账户需要订阅；新账户注册后立即唤醒，避免 5s 轮询延迟
        while self.subscribed_accounts.is_empty() {
            self.subscription_notify.notified().await;
        }

        let accounts = self.get_subscribed_addresses();
        info!(
            "Account subscriber connecting to gRPC for {} accounts",
            accounts.len(),
        );

        let mut client = GeyserGrpcClient::build_from_shared(self.grpc_url.clone())?
            .x_token(self.grpc_token.clone())?
            .connect_timeout(Duration::from_secs(ACCOUNT_GRPC_CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(ACCOUNT_GRPC_STREAM_TIMEOUT_SECS))
            .http2_keep_alive_interval(Duration::from_secs(
                ACCOUNT_GRPC_HTTP2_KEEPALIVE_SECS,
            ))
            .keep_alive_timeout(Duration::from_secs(
                ACCOUNT_GRPC_KEEPALIVE_TIMEOUT_SECS,
            ))
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(Duration::from_secs(
                ACCOUNT_GRPC_TCP_KEEPALIVE_SECS,
            )))
            .tcp_nodelay(true)
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await
            .context("Account subscriber: failed to connect gRPC")?;

        let request = Self::build_subscribe_request(&accounts);

        let (mut subscribe_tx, mut stream) = client
            .subscribe_with_request(Some(request))
            .await
            .context("Account subscriber: failed to subscribe")?;

        info!("Account subscription active");

        // 记录上次订阅的账户数，用于检测新增
        let mut last_subscribed_count = accounts.len();

        // 定时检查新账户的 interval
        let mut resub_interval = tokio::time::interval(Duration::from_millis(200));
        resub_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(update)) => {
                            if let Some(UpdateOneof::Account(account_info)) = update.update_oneof {
                                self.handle_account_update(account_info, &update_tx);
                            }
                        }
                        Some(Err(e)) => {
                            error!(
                                "Account subscriber stream error (tracked_accounts={}): {}",
                                last_subscribed_count,
                                e
                            );
                            return Err(e.into());
                        }
                        None => {
                            warn!("Account subscriber stream ended");
                            return Ok(());
                        }
                    }
                }
                _ = resub_interval.tick() => {
                    let current_accounts = self.get_subscribed_addresses();
                    if current_accounts.len() != last_subscribed_count {
                        debug!(
                            "动态更新账户订阅: {} → {} 个账户",
                            last_subscribed_count, current_accounts.len(),
                        );
                        let new_request = Self::build_subscribe_request(&current_accounts);
                        if let Err(e) = subscribe_tx.send(new_request).await {
                            error!("Failed to update subscription: {}", e);
                        }
                        last_subscribed_count = current_accounts.len();
                    }
                }
            }
        }
    }

    /// 构建 gRPC 订阅请求
    fn build_subscribe_request(accounts: &[String]) -> SubscribeRequest {
        let mut accounts_filter = HashMap::new();
        accounts_filter.insert(
            "tracked_accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: accounts.to_vec(),
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );

        SubscribeRequest {
            accounts: accounts_filter,
            commitment: Some(CommitmentLevel::Processed as i32),
            transactions: HashMap::new(),
            slots: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            accounts_data_slice: vec![],
            ping: None,
            ..Default::default()
        }
    }

    /// 处理单个账户更新事件
    fn handle_account_update(
        &self,
        account_info: yellowstone_grpc_proto::prelude::SubscribeUpdateAccount,
        update_tx: &mpsc::UnboundedSender<AccountUpdate>,
    ) {
        if let Some(account) = account_info.account {
            let pubkey_bytes = &account.pubkey;
            if pubkey_bytes.len() != 32 {
                return;
            }
            let pubkey =
                Pubkey::new_from_array(<[u8; 32]>::try_from(pubkey_bytes.as_slice()).unwrap());

            let slot = account_info.slot;

            // 判断账户类型并处理
            if let Some(account_type) = self.subscribed_accounts.get(&pubkey) {
                match *account_type {
                    AccountType::BondingCurve => {
                        if let Some(mint) = self.bonding_curve_to_mint.get(&pubkey) {
                            match BondingCurveState::from_account_data(&account.data) {
                                Ok(state) => {
                                    // 更新缓存
                                    self.bc_cache.update(&mint, state.clone());

                                    let _ = update_tx.send(AccountUpdate::BondingCurve(
                                        BondingCurveUpdate {
                                            mint: *mint,
                                            bonding_curve: pubkey,
                                            state,
                                            slot,
                                        },
                                    ));
                                }
                                Err(e) => {
                                    debug!(
                                        "Failed to parse bonding curve {}: {}",
                                        &pubkey.to_string()[..12],
                                        e
                                    );
                                }
                            }
                        }
                    }
                    AccountType::Ata => {
                        if let Some(mint) = self.ata_to_mint.get(&pubkey) {
                            // Token account: mint(32) + owner(32) + amount(8)
                            let amount = if account.data.len() >= 72 {
                                u64::from_le_bytes(
                                    account.data[64..72].try_into().unwrap_or([0; 8]),
                                )
                            } else {
                                0
                            };

                            // 更新 ATA 余额缓存
                            self.ata_cache.update(&mint, amount);

                            let _ = update_tx.send(AccountUpdate::AtaBalance(AtaBalanceUpdate {
                                ata_address: pubkey,
                                mint: *mint,
                                amount,
                                slot,
                            }));
                        }
                    }
                }
            }
        }
    }
}
