use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    system_program,
};
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account_idempotent,
};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::processor::{DetectedTrade, MirrorInstruction, TradeProcessor, TradeType};

// ============================================
// Pump.fun 常量
// ============================================
const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPFUN_GLOBAL: &str = "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf";
const PUMPFUN_FEE_RECIPIENT: &str = "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV";
const PUMPFUN_EVENT_AUTHORITY: &str = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1";
const PUMP_FEE_PROGRAM: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
// fee_config PDA = seeds["fee_config", pump_program_id] under fee_program
// 已验证正确地址: 8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt
const PUMP_FEE_CONFIG_PDA: &str = "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt";

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const RENT_SYSVAR: &str = "SysvarRent111111111111111111111111111111111";
const MAYHEM_FEE_RECIPIENTS: [&str; 8] = [
    "GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS",
    "4budycTjhs9fD6xw62VBducVTNgMgJJ5BgtKq7mAZwn6",
    "8SBKzEQU4nLSzcwF4a74F2iaUDQyTfjGndn6qUWBnrpR",
    "4UQeTP1T39KZ9Sfxzo3WR5skgsaP6NZa87BAkuazLEKH",
    "8sNeir4QsLsJdYpc9RZacohhK1Y5FLU3nC5LXgYB4aa6",
    "Fh9HmeLNUMVCvejxCtCL2DbYaRyBFVJ5xrWkLnMH6fdk",
    "463MEnMeGyJekNZFQSTUABBEbLnvMTALbT6ZmsxAbAdq",
    "6AUH3WEHucYZyC61hqpqYUWVto5qA5hjHuNQ32GNnNxA",
];

// PumpSwap (AMM for migrated tokens) - 2025-01 update
// After bonding curve completes, tokens migrate to PumpSwap for DEX trading
// NOTE: Tokens migrate when bonding curve is "complete". If the bonding curve
// account owner is NOT PumpFun program, the token has migrated - skip PumpFun trade.
const _PUMPSWAP_PROGRAM_ID: &str = "pLgM3rWHN3W4Hxb3cKxE7b1K3qJN3vRU4QvT8wXyYz"; // placeholder - derive dynamically

// Pump.fun buy discriminator: sha256("global:buy")[..8]
const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
// Pump.fun buyExactSolIn discriminator: sha256("global:buy_exact_sol_in")[..8]
const BUY_EXACT_SOL_IN_DISCRIMINATOR: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
// Pump.fun sell discriminator: sha256("global:sell")[..8]
const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
// initUserVolumeAccumulator discriminator
const INIT_UVA_DISCRIMINATOR: [u8; 8] = [94, 6, 202, 115, 255, 96, 232, 183];
// extend discriminator (扩展旧格式 bonding curve)
const EXTEND_DISCRIMINATOR: [u8; 8] = [234, 102, 194, 203, 150, 72, 62, 229];
const PUMPFUN_BUY_ACCOUNT_LABELS: [&str; 17] = [
    "global",
    "fee_recipient",
    "mint",
    "bonding_curve",
    "associated_bonding_curve",
    "user_ata",
    "user",
    "system_program",
    "token_program",
    "creator_vault",
    "event_authority",
    "program",
    "global_volume_accumulator",
    "user_volume_accumulator",
    "fee_config",
    "fee_program",
    "bonding_curve_v2",
];

/// Pump.fun 标准总供应量: 10 亿 tokens
pub const PUMP_TOTAL_SUPPLY: f64 = 1_000_000_000.0;

/// Pump.fun bonding curve 状态（从链上账户反序列化）
/// 2026.03 IDL 布局:
///   [0..8]    discriminator
///   [8..16]   virtual_token_reserves
///   [16..24]  virtual_sol_reserves
///   [24..32]  real_token_reserves
///   [32..40]  real_sol_reserves
///   [40..48]  token_total_supply
///   [48]      complete
///   [49..81]  creator (Pubkey)
///   [81]      is_mayhem_mode
///   [82]      is_cashback
#[derive(Debug, Clone)]
pub struct BondingCurveState {
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub token_total_supply: u64,
    pub complete: bool,
    pub creator: Option<Pubkey>,
    pub is_mayhem_mode: bool,
    pub is_cashback: bool,
}

enum TargetBuyInstruction {
    Buy {
        token_amount: u64,
        max_sol_cost: u64,
    },
    BuyExactSolIn {
        sol_amount: u64,
    },
}

impl BondingCurveState {
    pub fn from_account_data(data: &[u8]) -> Result<Self> {
        if data.len() < 49 {
            anyhow::bail!("Bonding curve data too short: {} bytes", data.len());
        }
        let creator = if data.len() >= 81 {
            Some(Pubkey::new_from_array(
                <[u8; 32]>::try_from(&data[49..81]).unwrap_or([0; 32]),
            ))
        } else {
            None
        };
        let is_mayhem_mode = if data.len() > 81 {
            data[81] != 0
        } else {
            false
        };
        let is_cashback = if data.len() > 82 {
            data[82] != 0
        } else {
            false
        };

        Ok(Self {
            virtual_token_reserves: u64::from_le_bytes(data[8..16].try_into()?),
            virtual_sol_reserves: u64::from_le_bytes(data[16..24].try_into()?),
            real_token_reserves: u64::from_le_bytes(data[24..32].try_into()?),
            real_sol_reserves: u64::from_le_bytes(data[32..40].try_into()?),
            token_total_supply: u64::from_le_bytes(data[40..48].try_into()?),
            complete: data[48] != 0,
            creator,
            is_mayhem_mode,
            is_cashback,
        })
    }

    /// 计算 token 单价 (SOL/token)
    /// 公式: virtualSolReserves / virtualTokenReserves / 1000
    /// /1000 原因: virtualTokenReserves 含 6 位小数(1e6), virtualSolReserves 含 9 位(1e9), 差 1e3
    pub fn price_sol(&self) -> f64 {
        if self.virtual_token_reserves == 0 {
            return 0.0;
        }
        self.virtual_sol_reserves as f64 / self.virtual_token_reserves as f64 / 1000.0
    }

    /// 计算市值 (SOL)
    /// 公式: price_sol × PUMP_TOTAL_SUPPLY (10 亿)
    pub fn market_cap_sol(&self) -> f64 {
        self.price_sol() * PUMP_TOTAL_SUPPLY
    }

    /// 计算用 sol_amount lamports 能买到多少 token
    pub fn sol_to_token_quote(&self, sol_amount: u64) -> u64 {
        if self.virtual_sol_reserves == 0 {
            return 0;
        }
        // AMM constant product: (x + dx) * (y - dy) = x * y
        // dy = y * dx / (x + dx)
        let numerator = (self.virtual_token_reserves as u128) * (sol_amount as u128);
        let denominator = (self.virtual_sol_reserves as u128) + (sol_amount as u128);
        (numerator / denominator) as u64
    }

    /// 计算卖出 token_amount 能获得多少 SOL lamports
    pub fn token_to_sol_quote(&self, token_amount: u64) -> u64 {
        if self.virtual_token_reserves == 0 {
            return 0;
        }
        let numerator = (self.virtual_sol_reserves as u128) * (token_amount as u128);
        let denominator = (self.virtual_token_reserves as u128) + (token_amount as u128);
        (numerator / denominator) as u64
    }
}

fn max_pump_raw_supply() -> u64 {
    (PUMP_TOTAL_SUPPLY as u64) * 1_000_000
}

fn first_pubkey_mismatch(
    actual: &[Pubkey],
    expected: &[Pubkey],
) -> Option<(usize, Pubkey, Pubkey)> {
    actual
        .iter()
        .zip(expected.iter())
        .enumerate()
        .find_map(|(index, (actual_key, expected_key))| {
            if actual_key != expected_key {
                Some((index, *actual_key, *expected_key))
            } else {
                None
            }
        })
}

pub struct PumpfunProcessor {
    rpc_client: Arc<RpcClient>,
}

impl PumpfunProcessor {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }

    fn select_fee_recipient(&self, curve_state: &BondingCurveState) -> Result<Pubkey> {
        if curve_state.is_mayhem_mode {
            return Pubkey::from_str(MAYHEM_FEE_RECIPIENTS[0])
                .map_err(|err| anyhow::anyhow!("invalid mayhem fee recipient: {}", err));
        }

        Pubkey::from_str(PUMPFUN_FEE_RECIPIENT)
            .map_err(|err| anyhow::anyhow!("invalid pump fee recipient: {}", err))
    }

    fn build_buy_account_keys_standard(
        &self,
        user: &Pubkey,
        mint: &Pubkey,
        user_ata: &Pubkey,
        token_program_id: &Pubkey,
        curve_state: &BondingCurveState,
    ) -> Result<[Pubkey; 17]> {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
        let fee_recipient = self.select_fee_recipient(curve_state)?;
        let creator = curve_state
            .creator
            .ok_or_else(|| anyhow::anyhow!("bonding curve missing creator"))?;
        let (bonding_curve, _) =
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id);
        let associated_bonding_curve =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &bonding_curve,
                mint,
                token_program_id,
            );
        let creator_vault =
            Pubkey::find_program_address(&[b"creator-vault", creator.as_ref()], &program_id).0;
        let global_volume_accumulator =
            Pubkey::find_program_address(&[b"global_volume_accumulator"], &program_id).0;
        let user_volume_accumulator =
            Pubkey::find_program_address(&[b"user_volume_accumulator", user.as_ref()], &program_id)
                .0;
        let bonding_curve_v2 =
            Pubkey::find_program_address(&[b"bonding-curve-v2", mint.as_ref()], &program_id).0;

        Ok([
            Pubkey::from_str(PUMPFUN_GLOBAL).unwrap(),
            fee_recipient,
            *mint,
            bonding_curve,
            associated_bonding_curve,
            *user_ata,
            *user,
            system_program::id(),
            *token_program_id,
            creator_vault,
            Pubkey::from_str(PUMPFUN_EVENT_AUTHORITY).unwrap(),
            program_id,
            global_volume_accumulator,
            user_volume_accumulator,
            Pubkey::from_str(PUMP_FEE_CONFIG_PDA).unwrap(),
            Pubkey::from_str(PUMP_FEE_PROGRAM).unwrap(),
            bonding_curve_v2,
        ])
    }

    pub fn validate_direct_mirror_buy_accounts(
        &self,
        mint: &Pubkey,
        user_ata: &Pubkey,
        token_program_id: &Pubkey,
        source_wallet: &Pubkey,
        mirror_accounts: &[Pubkey],
        curve_state: Option<&BondingCurveState>,
        config: &AppConfig,
    ) -> Result<()> {
        if mirror_accounts.len() != PUMPFUN_BUY_ACCOUNT_LABELS.len() {
            anyhow::bail!(
                "unexpected direct mirror account count: expected {}, got {}",
                PUMPFUN_BUY_ACCOUNT_LABELS.len(),
                mirror_accounts.len()
            );
        }

        let replaced =
            Self::replace_user_pdas(mirror_accounts, source_wallet, &config.pubkey, user_ata);

        if let Some(curve_state) = curve_state {
            let expected = self.build_buy_account_keys_standard(
                &config.pubkey,
                mint,
                user_ata,
                token_program_id,
                curve_state,
            )?;

            if let Some((index, actual, expected_key)) =
                first_pubkey_mismatch(&replaced, &expected)
            {
                anyhow::bail!(
                    "{} mismatch at [{}]: expected {}, got {}",
                    PUMPFUN_BUY_ACCOUNT_LABELS[index],
                    index,
                    expected_key,
                    actual
                );
            }

            return Ok(());
        }

        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
        let (bonding_curve, _) =
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id);
        let associated_bonding_curve =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &bonding_curve,
                mint,
                token_program_id,
            );
        let global_volume_accumulator =
            Pubkey::find_program_address(&[b"global_volume_accumulator"], &program_id).0;
        let user_volume_accumulator = Pubkey::find_program_address(
            &[b"user_volume_accumulator", config.pubkey.as_ref()],
            &program_id,
        )
        .0;
        let bonding_curve_v2 =
            Pubkey::find_program_address(&[b"bonding-curve-v2", mint.as_ref()], &program_id).0;

        let partial_expectations = [
            (0usize, Pubkey::from_str(PUMPFUN_GLOBAL).unwrap()),
            (2usize, *mint),
            (3usize, bonding_curve),
            (4usize, associated_bonding_curve),
            (5usize, *user_ata),
            (6usize, config.pubkey),
            (7usize, system_program::id()),
            (8usize, *token_program_id),
            (10usize, Pubkey::from_str(PUMPFUN_EVENT_AUTHORITY).unwrap()),
            (11usize, program_id),
            (12usize, global_volume_accumulator),
            (13usize, user_volume_accumulator),
            (14usize, Pubkey::from_str(PUMP_FEE_CONFIG_PDA).unwrap()),
            (15usize, Pubkey::from_str(PUMP_FEE_PROGRAM).unwrap()),
            (16usize, bonding_curve_v2),
        ];

        for (index, expected_key) in partial_expectations {
            let actual = replaced[index];
            if actual != expected_key {
                anyhow::bail!(
                    "{} mismatch at [{}]: expected {}, got {}",
                    PUMPFUN_BUY_ACCOUNT_LABELS[index],
                    index,
                    expected_key,
                    actual
                );
            }
        }

        Ok(())
    }

    fn build_buy_instruction_standard(
        &self,
        user: &Pubkey,
        mint: &Pubkey,
        user_ata: &Pubkey,
        token_program_id: &Pubkey,
        curve_state: &BondingCurveState,
        token_amount: u64,
        max_sol_cost: u64,
    ) -> Result<Instruction> {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
        let account_keys =
            self.build_buy_account_keys_standard(user, mint, user_ata, token_program_id, curve_state)?;

        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&BUY_DISCRIMINATOR);
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&max_sol_cost.to_le_bytes());

        let accounts = vec![
            AccountMeta::new_readonly(account_keys[0], false),
            AccountMeta::new(account_keys[1], false),
            AccountMeta::new_readonly(account_keys[2], false),
            AccountMeta::new(account_keys[3], false),
            AccountMeta::new(account_keys[4], false),
            AccountMeta::new(account_keys[5], false),
            AccountMeta::new(account_keys[6], true),
            AccountMeta::new_readonly(account_keys[7], false),
            AccountMeta::new_readonly(account_keys[8], false),
            AccountMeta::new(account_keys[9], false),
            AccountMeta::new_readonly(account_keys[10], false),
            AccountMeta::new_readonly(program_id, false),
            AccountMeta::new_readonly(account_keys[12], false),
            AccountMeta::new(account_keys[13], false),
            AccountMeta::new_readonly(account_keys[14], false),
            AccountMeta::new_readonly(account_keys[15], false),
            AccountMeta::new_readonly(account_keys[16], false),
        ];

        Ok(Instruction {
            program_id,
            accounts,
            data,
        })
    }

    /// 预取 bonding curve 状态（后台调用，结果写入缓存）
    pub async fn prefetch_bonding_curve(
        &self,
        bonding_curve: &Pubkey,
    ) -> Result<BondingCurveState> {
        self.fetch_and_validate_bonding_curve(bonding_curve).await
    }

    pub async fn is_bonding_curve_migrated(&self, bonding_curve: &Pubkey) -> Result<bool> {
        let pumpfun_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
        let bc = *bonding_curve;
        let rpc = self.rpc_client.clone();

        let result = tokio::task::spawn_blocking(move || {
            rpc.get_account_with_commitment(
                &bc,
                solana_sdk::commitment_config::CommitmentConfig::processed(),
            )
        })
        .await
        .context("bonding curve owner check task panicked")?;

        match result {
            Ok(response) => match response.value {
                Some(account) => Ok(account.owner != pumpfun_id),
                None => anyhow::bail!("bonding curve account not found"),
            },
            Err(e) => Err(anyhow::anyhow!("RPC get_account failed: {}", e)),
        }
    }

    /// 从交易的 account keys 中提取 token mint 和 bonding curve 地址
    fn extract_pumpfun_accounts(&self, trade: &DetectedTrade) -> Result<(Pubkey, Pubkey, Pubkey)> {
        // Pump.fun buy/sell 指令的 account 布局:
        // 0: global
        // 1: fee_recipient
        // 2: mint
        // 3: bonding_curve
        // 4: associated_bonding_curve
        // 5: associated_user
        // 6: user
        // ...
        let accounts = &trade.instruction_accounts;
        if accounts.len() < 7 {
            anyhow::bail!(
                "Pumpfun instruction has {} accounts, expected >= 7",
                accounts.len()
            );
        }

        let mint = accounts[2];
        let bonding_curve = accounts[3];
        let associated_bonding_curve = accounts[4];

        Ok((mint, bonding_curve, associated_bonding_curve))
    }

    /// 一次 RPC 调用同时验证 owner 并解析 bonding curve 状态
    /// 合并原来的 validate_bonding_curve_pda + fetch_bonding_curve，省掉一次网络往返
    async fn fetch_and_validate_bonding_curve(
        &self,
        bonding_curve: &Pubkey,
    ) -> Result<BondingCurveState> {
        let pumpfun_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        // 最多重试 3 次，指数退避
        let mut last_err = None;
        for attempt in 0..3u32 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    200 * 2u64.pow(attempt - 1),
                ))
                .await;
            }

            let bc = *bonding_curve;
            let rpc = self.rpc_client.clone();

            // 使用 get_account_with_commitment(processed) 替代默认 confirmed
            // 因为 gRPC 在 Processed 级别检测到交易，此时 confirmed 可能还没同步到
            let result = tokio::task::spawn_blocking(move || {
                rpc.get_account_with_commitment(
                    &bc,
                    solana_sdk::commitment_config::CommitmentConfig::processed(),
                )
            })
            .await
            .context("bonding curve RPC task panicked")?;

            match result {
                Ok(response) => match response.value {
                    Some(account) => {
                        if account.owner != pumpfun_id {
                            info!(
                                "Bonding curve {} owner is {} (not PumpFun), token migrated",
                                &bonding_curve.to_string()[..12],
                                account.owner,
                            );
                            anyhow::bail!("Bonding curve migrated to PumpSwap");
                        }
                        return BondingCurveState::from_account_data(&account.data);
                    }
                    None => {
                        last_err = Some(format!("account not found (attempt #{})", attempt + 1,));
                        debug!(
                            "Bonding curve {} not found (attempt #{}, commitment=processed)",
                            &bonding_curve.to_string()[..12],
                            attempt + 1,
                        );
                    }
                },
                Err(e) => {
                    last_err = Some(format!("RPC error (attempt #{}): {}", attempt + 1, e));
                    warn!(
                        "RPC get_account failed for {} (attempt #{}): {}",
                        &bonding_curve.to_string()[..12],
                        attempt + 1,
                        e,
                    );
                }
            }
        }

        anyhow::bail!(
            "bonding curve {} fetch failed after 3 retries: {}",
            &bonding_curve.to_string()[..12],
            last_err.unwrap_or_else(|| "unknown".to_string()),
        )
    }

    /// 查找并替换 mirror_accounts 中所有用户特定的 PDA
    /// 原理: 尝试多种 PDA seed 组合，如果 source_wallet 派生出的地址匹配某个 mirror_account，
    /// 则用 our_wallet 重新派生替换
    fn replace_user_pdas(
        mirror_accounts: &[Pubkey],
        source_wallet: &Pubkey,
        our_wallet: &Pubkey,
        our_ata: &Pubkey,
    ) -> Vec<Pubkey> {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        // 所有可能的用户特定 PDA seed 模式
        let seed_patterns: &[&[u8]] = &[
            b"user_volume_accumulator",
            b"user-stats",
            b"user",
            b"fee",
            b"volume",
            b"user_fee",
            b"user_account",
        ];

        // 预计算 source_wallet 的所有可能 PDA
        let mut source_pdas: Vec<(Pubkey, Pubkey)> = Vec::new(); // (source_pda, our_pda)
        for seed in seed_patterns {
            let (source_pda, _) =
                Pubkey::find_program_address(&[*seed, source_wallet.as_ref()], &program_id);
            let (our_pda, _) =
                Pubkey::find_program_address(&[*seed, our_wallet.as_ref()], &program_id);
            source_pdas.push((source_pda, our_pda));
        }

        mirror_accounts
            .iter()
            .enumerate()
            .map(|(i, acct)| {
                match i {
                    5 => *our_ata,    // user_ata
                    6 => *our_wallet, // user (signer)
                    _ => {
                        // 检查是否匹配任何用户 PDA
                        for (source_pda, our_pda) in &source_pdas {
                            if acct == source_pda {
                                debug!(
                                    "替换用户 PDA [{}]: {} → {}",
                                    i,
                                    &source_pda.to_string()[..12],
                                    &our_pda.to_string()[..12],
                                );
                                return *our_pda;
                            }
                        }
                        *acct
                    }
                }
            })
            .collect()
    }

    /// 构建 Pump.fun buy 指令
    /// 使用目标钱包的账户列表，自动检测并替换所有用户特定的账户
    fn build_buy_instruction_from_mirror(
        &self,
        user: &Pubkey,
        user_ata: &Pubkey,
        source_wallet: &Pubkey,
        mirror_accounts: &[Pubkey],
        token_amount: u64,
        max_sol_cost: u64,
    ) -> Instruction {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&BUY_DISCRIMINATOR);
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&max_sol_cost.to_le_bytes());

        // 替换所有用户特定的账户
        let replaced = Self::replace_user_pdas(mirror_accounts, source_wallet, user, user_ata);

        let accounts: Vec<AccountMeta> = replaced
            .iter()
            .enumerate()
            .map(|(i, acct)| {
                match i {
                    6 => AccountMeta::new(*acct, true), // signer
                    // writable: fee(1), bc(3), abc(4), ata(5), user(6), creator_vault(9), accumulator(13)
                    1 | 3 | 4 | 5 | 9 | 13 => AccountMeta::new(*acct, false),
                    _ => AccountMeta::new_readonly(*acct, false),
                }
            })
            .collect();

        Instruction {
            program_id,
            accounts,
            data,
        }
    }

    /// 构建 Pump.fun sell 指令
    /// mirror_accounts 只取 [2]mint, [3]bc, [4]assoc_bc
    /// token_program 和 creator 从 prefetch/bc_cache 动态获取
    /// is_cashback 控制是否追加 user_volume_accumulator
    pub fn build_sell_instruction_from_mirror(
        &self,
        user: &Pubkey,
        user_ata: &Pubkey,
        mirror_accounts: &[Pubkey],
        token_amount: u64,
        min_sol_output: u64,
        token_program_id: &Pubkey,
        creator: &Pubkey,
        is_cashback: bool,
    ) -> Instruction {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SELL_DISCRIMINATOR);
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&min_sol_output.to_le_bytes());

        // PDA 派生
        let creator_vault =
            Pubkey::find_program_address(&[b"creator-vault", creator.as_ref()], &program_id).0;

        // 从 mirror_accounts 只取代币相关地址（位置 [2-4] 跨所有 buy 变体稳定）
        let mint = mirror_accounts[2];
        let bonding_curve = mirror_accounts[3];
        let assoc_bc = mirror_accounts[4];

        // bonding_curve_v2 = PDA 派生（必需的 remaining account）
        let bonding_curve_v2 =
            Pubkey::find_program_address(&[b"bonding-curve-v2", mint.as_ref()], &program_id).0;

        // 14 固定账户（2026 Pump.fun sell IDL）
        let mut accounts = vec![
            AccountMeta::new_readonly(Pubkey::from_str(PUMPFUN_GLOBAL).unwrap(), false), // 0  global
            AccountMeta::new(Pubkey::from_str(PUMPFUN_FEE_RECIPIENT).unwrap(), false), // 1  fee_recipient
            AccountMeta::new_readonly(mint, false),                                    // 2  mint
            AccountMeta::new(bonding_curve, false), // 3  bonding_curve
            AccountMeta::new(assoc_bc, false),      // 4  assoc_bonding_curve
            AccountMeta::new(*user_ata, false),     // 5  user_ata
            AccountMeta::new(*user, true),          // 6  user (signer)
            AccountMeta::new_readonly(system_program::id(), false), // 7  system_program
            AccountMeta::new(creator_vault, false), // 8  creator_vault (PDA)
            AccountMeta::new_readonly(*token_program_id, false), // 9  token_program
            AccountMeta::new_readonly(Pubkey::from_str(PUMPFUN_EVENT_AUTHORITY).unwrap(), false), // 10 event_authority
            AccountMeta::new_readonly(program_id, false), // 11 program
            AccountMeta::new_readonly(Pubkey::from_str(PUMP_FEE_CONFIG_PDA).unwrap(), false), // 12 fee_config
            AccountMeta::new_readonly(Pubkey::from_str(PUMP_FEE_PROGRAM).unwrap(), false), // 13 fee_program
        ];

        // remaining accounts（和 sell_standard 一致）
        if is_cashback {
            let (user_vol_acc, _) = Pubkey::find_program_address(
                &[b"user_volume_accumulator", user.as_ref()],
                &program_id,
            );
            accounts.push(AccountMeta::new(user_vol_acc, false));
        }
        accounts.push(AccountMeta::new_readonly(bonding_curve_v2, false));

        Instruction {
            program_id,
            accounts,
            data,
        }
    }

    /// 构建 Pump.fun sell 指令
    fn build_sell_instruction(
        &self,
        user: &Pubkey,
        mint: &Pubkey,
        bonding_curve: &Pubkey,
        associated_bonding_curve: &Pubkey,
        token_amount: u64,
        min_sol_output: u64,
        token_program_id: &Pubkey,
    ) -> Instruction {
        let user_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            user,
            mint,
            token_program_id,
        );
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SELL_DISCRIMINATOR);
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&min_sol_output.to_le_bytes());

        let accounts = vec![
            AccountMeta::new_readonly(Pubkey::from_str(PUMPFUN_GLOBAL).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(PUMPFUN_FEE_RECIPIENT).unwrap(), false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new(*bonding_curve, false),
            AccountMeta::new(*associated_bonding_curve, false),
            AccountMeta::new(user_ata, false),
            AccountMeta::new(*user, true),
            AccountMeta::new_readonly(system_program::id(), false),
            AccountMeta::new_readonly(*token_program_id, false),
            AccountMeta::new_readonly(Pubkey::from_str(RENT_SYSVAR).unwrap(), false),
            AccountMeta::new_readonly(Pubkey::from_str(PUMPFUN_EVENT_AUTHORITY).unwrap(), false),
            AccountMeta::new_readonly(program_id, false),
        ];

        Instruction {
            program_id,
            accounts,
            data,
        }
    }

    /// 使用 mirror_accounts 构建卖出指令
    /// 新版 PumpFun 买入卖出的账户布局一致（都是 14 个账户），
    /// 只是 discriminator 和参数不同，因此直接复用 mirror_accounts + replace_user_pdas
    pub async fn sell_with_mirror(
        &self,
        mint: &Pubkey,
        token_amount: u64,
        user_ata: &Pubkey,
        source_wallet: &Pubkey,
        mirror_accounts: &[Pubkey],
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        let bonding_curve = if mirror_accounts.len() > 3 {
            mirror_accounts[3]
        } else {
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id).0
        };

        let curve_state = self
            .fetch_and_validate_bonding_curve(&bonding_curve)
            .await?;
        let expected_sol = curve_state.token_to_sol_quote(token_amount);
        let min_sol_output = expected_sol - (expected_sol * config.slippage_bps / 10_000);

        debug!(
            "Pump.fun 卖出: {} tokens → ~{:.4} SOL (min: {:.4})",
            token_amount,
            expected_sol as f64 / 1e9,
            min_sol_output as f64 / 1e9,
        );

        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SELL_DISCRIMINATOR);
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&min_sol_output.to_le_bytes());

        // 替换 mirror_accounts 中的用户特定账户（和买入一样的逻辑）
        let replaced =
            Self::replace_user_pdas(mirror_accounts, source_wallet, &config.pubkey, user_ata);

        let accounts: Vec<AccountMeta> = replaced
            .iter()
            .enumerate()
            .map(|(i, acct)| {
                match i {
                    6 => AccountMeta::new(*acct, true), // signer
                    // writable: fee(1), bc(3), abc(4), ata(5), creator_vault(9), accumulator(13)
                    1 | 3 | 4 | 5 | 9 | 13 => AccountMeta::new(*acct, false),
                    _ => AccountMeta::new_readonly(*acct, false),
                }
            })
            .collect();

        let sell_ix = Instruction {
            program_id,
            accounts,
            data,
        };

        Ok(MirrorInstruction {
            swap_instructions: vec![sell_ix],
            pre_instructions: vec![],
            post_instructions: vec![],
            token_mint: *mint,
            sol_amount: expected_sol,
        })
    }

    /// 标准卖出：严格按照 2026.03 IDL 构建
    ///
    /// 如果有 buy_mirror_accounts（来自买入交易），从中提取已验证的地址
    /// （fee_config、fee_program、bonding_curve_v2、event_authority、creator_vault）
    /// 避免自行推导 PDA 可能出错
    ///
    /// sell 指令 14 accounts + remaining:
    ///  0.  global
    ///  1.  fee_recipient        (writable)
    ///  2.  mint
    ///  3.  bonding_curve        (writable)
    ///  4.  associated_bonding_curve (writable)
    ///  5.  associated_user      (writable)
    ///  6.  user                 (signer, writable)
    ///  7.  system_program
    ///  8.  creator_vault        (writable)
    ///  9.  token_program
    /// 10.  event_authority
    /// 11.  program
    /// 12.  fee_config
    /// 13.  fee_program
    /// remaining: [if cashback: user_volume_accumulator], bonding_curve_v2
    ///
    /// buy 的 mirror_accounts 布局（用于提取地址）:
    ///  [9]  creator_vault
    /// [10]  event_authority
    /// [14]  fee_config
    /// [15]  fee_program
    /// [16]  bonding_curve_v2
    pub async fn sell_standard(
        &self,
        mint: &Pubkey,
        token_amount: u64,
        config: &AppConfig,
        buy_mirror_accounts: Option<&[Pubkey]>,
    ) -> Result<MirrorInstruction> {
        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        let (bonding_curve, _) =
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id);

        // 获取完整的 bonding curve 数据（包含 creator、is_cashback）
        let rpc = self.rpc_client.clone();
        let bc = bonding_curve;
        let bc_data = tokio::task::spawn_blocking(move || rpc.get_account_data(&bc))
            .await
            .context("spawn_blocking failed")?
            .context("获取 bonding curve 失败")?;

        let curve_state = BondingCurveState::from_account_data(&bc_data)?;
        if curve_state.complete {
            anyhow::bail!("bonding curve 已完成（已迁移）");
        }

        let creator = curve_state
            .creator
            .ok_or_else(|| anyhow::anyhow!("bonding curve 无 creator 字段（数据太短）"))?;

        // 检测 token program：查 mint 账户 owner
        let rpc2 = self.rpc_client.clone();
        let m = *mint;
        let mint_info = tokio::task::spawn_blocking(move || rpc2.get_account(&m))
            .await
            .context("spawn_blocking failed")?
            .context("获取 mint 账户失败")?;

        let token_2022 = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap();
        let token_program_id = if mint_info.owner == token_2022 {
            token_2022
        } else {
            Pubkey::from_str(TOKEN_PROGRAM).unwrap()
        };

        let expected_sol = curve_state.token_to_sol_quote(token_amount);
        let min_sol_output = expected_sol - (expected_sol * config.slippage_bps / 10_000);

        // 用户 ATA 和 BC 的 ATA
        let user_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &config.pubkey,
            mint,
            &token_program_id,
        );
        let assoc_bc = spl_associated_token_account::get_associated_token_address_with_program_id(
            &bonding_curve,
            mint,
            &token_program_id,
        );

        // ============================================
        // 从 buy mirror_accounts 提取已验证的地址（最可靠）
        // 如果没有 mirror_accounts，回退到 PDA 推导
        // ============================================
        let mirror_len = buy_mirror_accounts.map_or(0, |m| m.len());
        let has_mirror = mirror_len >= 16;
        if mirror_len > 0 {
            debug!("mirror_accounts 长度: {} | 需要 >= 16", mirror_len);
        }

        let creator_vault = if has_mirror {
            buy_mirror_accounts.unwrap()[9]
        } else {
            Pubkey::find_program_address(&[b"creator-vault", creator.as_ref()], &program_id).0
        };

        let event_authority = if has_mirror {
            buy_mirror_accounts.unwrap()[10]
        } else {
            Pubkey::find_program_address(&[b"__event_authority"], &program_id).0
        };

        // fee_config 和 fee_program 是全局常量，直接用已验证的地址
        let fee_config = Pubkey::from_str(PUMP_FEE_CONFIG_PDA).unwrap();
        let fee_program_addr = Pubkey::from_str(PUMP_FEE_PROGRAM).unwrap();

        let bonding_curve_v2 = if has_mirror && buy_mirror_accounts.unwrap().len() > 16 {
            buy_mirror_accounts.unwrap()[16]
        } else {
            Pubkey::find_program_address(&[b"bonding-curve-v2", mint.as_ref()], &program_id).0
        };

        info!(
            "卖出(IDL): {} tokens → ~{:.4} SOL | cashback: {} | bc_len: {} | creator_vault: {} | bc_v2: {} | tp: {}",
            token_amount, expected_sol as f64 / 1e9,
            curve_state.is_cashback, bc_data.len(),
            &creator_vault.to_string()[..12],
            &bonding_curve_v2.to_string()[..12],
            if token_program_id == Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap() { "Token2022" } else { "TokenLegacy" },
        );

        // ============================================
        // 构建 pre_instructions
        // ============================================
        let mut pre_instructions = vec![];

        // bonding curve extend（旧格式 < 151 字节需要先 extend）
        const BONDING_CURVE_NEW_SIZE: usize = 151;
        if bc_data.len() < BONDING_CURVE_NEW_SIZE {
            info!(
                "Bonding curve 需要 extend: {} → {}",
                bc_data.len(),
                BONDING_CURVE_NEW_SIZE
            );
            let extend_ix = Instruction {
                program_id,
                accounts: vec![
                    AccountMeta::new(bonding_curve, false),
                    AccountMeta::new(config.pubkey, true),
                    AccountMeta::new_readonly(system_program::id(), false),
                    AccountMeta::new_readonly(event_authority, false),
                    AccountMeta::new_readonly(program_id, false),
                ],
                data: EXTEND_DISCRIMINATOR.to_vec(),
            };
            pre_instructions.push(extend_ix);
        }

        // ============================================
        // 构建 sell data
        // ============================================
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SELL_DISCRIMINATOR);
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&min_sol_output.to_le_bytes());

        // 14 固定账户
        let mut accounts = vec![
            AccountMeta::new_readonly(Pubkey::from_str(PUMPFUN_GLOBAL).unwrap(), false), // 0
            AccountMeta::new(Pubkey::from_str(PUMPFUN_FEE_RECIPIENT).unwrap(), false),   // 1
            AccountMeta::new_readonly(*mint, false),                                     // 2
            AccountMeta::new(bonding_curve, false),                                      // 3
            AccountMeta::new(assoc_bc, false),                                           // 4
            AccountMeta::new(user_ata, false),                                           // 5
            AccountMeta::new(config.pubkey, true),                                       // 6
            AccountMeta::new_readonly(system_program::id(), false),                      // 7
            AccountMeta::new(creator_vault, false),                                      // 8
            AccountMeta::new_readonly(token_program_id, false),                          // 9
            AccountMeta::new_readonly(event_authority, false),                           // 10
            AccountMeta::new_readonly(program_id, false),                                // 11
            AccountMeta::new_readonly(fee_config, false),                                // 12
            AccountMeta::new_readonly(fee_program_addr, false),                          // 13
        ];

        // remaining accounts
        if curve_state.is_cashback {
            let (user_vol_acc, _) = Pubkey::find_program_address(
                &[b"user_volume_accumulator", config.pubkey.as_ref()],
                &program_id,
            );

            // 检查 UVA 是否已初始化
            let rpc3 = self.rpc_client.clone();
            let uva = user_vol_acc;
            let uva_exists =
                tokio::task::spawn_blocking(move || rpc3.get_account_data(&uva).is_ok())
                    .await
                    .unwrap_or(false);

            if !uva_exists {
                info!("初始化 user_volume_accumulator (cashback 模式)");
                let init_uva_ix = Instruction {
                    program_id,
                    accounts: vec![
                        AccountMeta::new(config.pubkey, true),
                        AccountMeta::new_readonly(config.pubkey, false),
                        AccountMeta::new(user_vol_acc, false),
                        AccountMeta::new_readonly(system_program::id(), false),
                        AccountMeta::new_readonly(event_authority, false),
                        AccountMeta::new_readonly(program_id, false),
                    ],
                    data: INIT_UVA_DISCRIMINATOR.to_vec(),
                };
                pre_instructions.push(init_uva_ix);
            }

            accounts.push(AccountMeta::new(user_vol_acc, false));
        }
        accounts.push(AccountMeta::new_readonly(bonding_curve_v2, false));

        let sell_ix = Instruction {
            program_id,
            accounts,
            data,
        };

        Ok(MirrorInstruction {
            swap_instructions: vec![sell_ix],
            pre_instructions,
            post_instructions: vec![],
            token_mint: *mint,
            sol_amount: expected_sol,
        })
    }

    /// RPC 路径：通过 mint 构建买入指令（缓存未命中时使用）
    /// 需要 mirror_accounts 来构建正确的账户布局
    pub async fn buy_with_mirror(
        &self,
        mint: &Pubkey,
        user_ata: &Pubkey,
        token_program_id: &Pubkey,
        source_wallet: &Pubkey,
        mirror_accounts: &[Pubkey],
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        let bonding_curve = if mirror_accounts.len() > 3 {
            mirror_accounts[3]
        } else {
            let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id).0
        };

        info!(
            "Pump.fun 买入: mint={} | bc={} | {} accounts",
            &mint.to_string()[..12],
            &bonding_curve.to_string()[..12],
            mirror_accounts.len(),
        );

        let curve_state = self
            .fetch_and_validate_bonding_curve(&bonding_curve)
            .await?;

        if curve_state.complete {
            anyhow::bail!("bonding curve 已完成（已迁移外盘）");
        }

        let sol_amount = config.buy_lamports();
        let token_amount = curve_state.sol_to_token_quote(sol_amount);
        self.validate_buy_amount(token_amount, Some(&curve_state))?;
        let max_sol_cost = sol_amount + (sol_amount * config.slippage_bps / 10_000);

        let buy_ix = self.build_buy_instruction_from_mirror(
            &config.pubkey,
            user_ata,
            source_wallet,
            mirror_accounts,
            token_amount,
            max_sol_cost,
        );

        let create_ata_ix = create_associated_token_account_idempotent(
            &config.pubkey,
            &config.pubkey,
            mint,
            token_program_id,
        );

        Ok(MirrorInstruction {
            swap_instructions: vec![buy_ix],
            pre_instructions: vec![create_ata_ix],
            post_instructions: vec![],
            token_mint: *mint,
            sol_amount,
        })
    }

    /// 使用预取的 bonding curve 状态直接构建买入指令（零 RPC）
    /// 共识触发时如果缓存已有状态，用此方法跳过 RPC
    /// 使用缓存的 bonding curve 状态 + mirror_accounts 构建买入（零 RPC）
    fn validate_buy_amount(
        &self,
        token_amount: u64,
        curve_state: Option<&BondingCurveState>,
    ) -> Result<()> {
        if token_amount == 0 {
            anyhow::bail!("buy quote returned zero tokens");
        }

        if token_amount > max_pump_raw_supply() {
            anyhow::bail!(
                "buy quote abnormal: token amount {} exceeds max supply {}",
                token_amount,
                max_pump_raw_supply()
            );
        }

        if let Some(curve_state) = curve_state {
            if curve_state.real_token_reserves > 0 && token_amount > curve_state.real_token_reserves
            {
                anyhow::bail!(
                    "buy quote abnormal: token amount {} exceeds real reserves {}",
                    token_amount,
                    curve_state.real_token_reserves
                );
            }
        }

        Ok(())
    }

    pub fn buy_standard_from_cached_state(
        &self,
        mint: &Pubkey,
        user_ata: &Pubkey,
        token_program_id: &Pubkey,
        curve_state: &BondingCurveState,
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        if curve_state.complete {
            anyhow::bail!("bonding curve 宸插畬鎴愶紙宸茶縼绉诲鐩橈級");
        }

        let sol_amount = config.buy_lamports();
        let token_amount = curve_state.sol_to_token_quote(sol_amount);
        self.validate_buy_amount(token_amount, Some(curve_state))?;
        let max_sol_cost = sol_amount + (sol_amount * config.slippage_bps / 10_000);

        debug!(
            "鎶ヤ环(鏍囧噯): {} SOL 鈫?{} tokens | mode=native",
            config.buy_sol_amount,
            token_amount,
        );

        let buy_ix = self.build_buy_instruction_standard(
            &config.pubkey,
            mint,
            user_ata,
            token_program_id,
            curve_state,
            token_amount,
            max_sol_cost,
        )?;

        let create_ata_ix = create_associated_token_account_idempotent(
            &config.pubkey,
            &config.pubkey,
            mint,
            token_program_id,
        );

        Ok(MirrorInstruction {
            swap_instructions: vec![buy_ix],
            pre_instructions: vec![create_ata_ix],
            post_instructions: vec![],
            token_mint: *mint,
            sol_amount,
        })
    }

    fn parse_target_buy_instruction(
        &self,
        target_instruction_data: &[u8],
    ) -> Result<TargetBuyInstruction> {
        if target_instruction_data.len() < 24 {
            anyhow::bail!(
                "target instruction data too short: {} bytes",
                target_instruction_data.len()
            );
        }

        let disc: [u8; 8] = target_instruction_data[..8]
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid target instruction discriminator"))?;

        if disc == BUY_DISCRIMINATOR {
            let token_amount =
                u64::from_le_bytes(target_instruction_data[8..16].try_into().unwrap_or([0; 8]));
            let max_sol_cost =
                u64::from_le_bytes(target_instruction_data[16..24].try_into().unwrap_or([0; 8]));

            if token_amount == 0 || max_sol_cost == 0 {
                anyhow::bail!(
                    "invalid target buy data: tokens={}, sol={}",
                    token_amount,
                    max_sol_cost
                );
            }

            Ok(TargetBuyInstruction::Buy {
                token_amount,
                max_sol_cost,
            })
        } else if disc == BUY_EXACT_SOL_IN_DISCRIMINATOR {
            let sol_amount =
                u64::from_le_bytes(target_instruction_data[8..16].try_into().unwrap_or([0; 8]));

            if sol_amount == 0 {
                anyhow::bail!("invalid target buy_exact_sol_in data: sol=0");
            }

            Ok(TargetBuyInstruction::BuyExactSolIn { sol_amount })
        } else {
            anyhow::bail!("unsupported target buy discriminator: {:?}", disc);
        }
    }

    pub fn target_instruction_requires_curve(&self, target_instruction_data: &[u8]) -> bool {
        matches!(
            self.parse_target_buy_instruction(target_instruction_data),
            Ok(TargetBuyInstruction::BuyExactSolIn { .. })
        )
    }

    pub fn buy_from_cached_state(
        &self,
        mint: &Pubkey,
        user_ata: &Pubkey,
        token_program_id: &Pubkey,
        source_wallet: &Pubkey,
        mirror_accounts: &[Pubkey],
        curve_state: &BondingCurveState,
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        if curve_state.complete {
            anyhow::bail!("bonding curve 已完成（已迁移外盘）");
        }

        let sol_amount = config.buy_lamports();
        let token_amount = curve_state.sol_to_token_quote(sol_amount);
        let max_sol_cost = sol_amount + (sol_amount * config.slippage_bps / 10_000);

        debug!(
            "报价(缓存): {} SOL → {} tokens | {} accounts",
            config.buy_sol_amount,
            token_amount,
            mirror_accounts.len(),
        );

        let buy_ix = self.build_buy_instruction_from_mirror(
            &config.pubkey,
            user_ata,
            source_wallet,
            mirror_accounts,
            token_amount,
            max_sol_cost,
        );

        let create_ata_ix = create_associated_token_account_idempotent(
            &config.pubkey,
            &config.pubkey,
            mint,
            token_program_id,
        );

        Ok(MirrorInstruction {
            swap_instructions: vec![buy_ix],
            pre_instructions: vec![create_ata_ix],
            post_instructions: vec![],
            token_mint: *mint,
            sol_amount,
        })
    }

    /// 🚀 从目标钱包的指令数据直接构建买入（零 RPC，零缓存依赖）
    ///
    /// RabbitStream 优化: 目标交易在预执行阶段到达，BC 状态尚未改变。
    /// 从目标的 instruction_data 提取价格信息，直接计算我们的买入参数。
    ///
    /// 目标指令 data 布局:
    ///   [0..8]   buy discriminator
    ///   [8..16]  token_amount (预期获得的 raw tokens)
    ///   [16..24] max_sol_cost (最大 SOL 花费, lamports, 含滑点)
    ///
    /// 推算: approx_price = max_sol_cost / token_amount
    ///       our_tokens = our_buy_lamports / approx_price
    pub fn buy_from_target_instruction(
        &self,
        mint: &Pubkey,
        user_ata: &Pubkey,
        token_program_id: &Pubkey,
        source_wallet: &Pubkey,
        mirror_accounts: &[Pubkey],
        target_instruction_data: &[u8],
        config: &AppConfig,
    ) -> Result<(MirrorInstruction, u64)> {
        let (target_token_amount, target_max_sol) = match self
            .parse_target_buy_instruction(target_instruction_data)?
        {
            TargetBuyInstruction::Buy {
                token_amount,
                max_sol_cost,
            } => (token_amount, max_sol_cost),
            TargetBuyInstruction::BuyExactSolIn { sol_amount } => {
                anyhow::bail!(
                        "buy_exact_sol_in target fallback unsupported without bonding curve cache: sol={}",
                        sol_amount
                    );
            }
        };

        // 从目标指令反推 bonding curve 虚拟储备，再用 AMM 公式精确计算
        //
        // AMM: dy = y * dx / (x + dx)
        //   dy = target_token_amount, dx = target_sol (≈ max_sol / 1.3 去滑点估算)
        //   反推: x (virtual_sol) = dx * (y - dy) / dy
        //   但 y 未知，用 Pump.fun 初始参数估算:
        //     初始 virtual_token = 1,073,000,000 * 1e6 (raw)
        //     初始 virtual_sol   = 30 * 1e9 (30 SOL in lamports)
        //
        // 更简单的方法: 用 target 数据构建 synthetic BondingCurveState
        // price = target_max_sol / target_token_amount (含滑点，偏高)
        // virtual_sol / virtual_token = price / 1000 (price_sol 公式的逆)
        // 选一个合理的 virtual_sol (如 30 SOL)，推算 virtual_token
        // 然后用标准 sol_to_token_quote 精确计算

        // 从目标数据估算价格（lamports per raw token）
        let price_per_raw = target_max_sol as f64 / target_token_amount as f64;

        // 用价格反推虚拟储备
        // vSol / vToken = price_per_raw，设 vSol = 30 SOL
        let estimated_v_sol: u128 = 30_000_000_000; // 30 SOL
        let estimated_v_token = if price_per_raw > 0.0 {
            (estimated_v_sol as f64 / price_per_raw) as u128
        } else {
            1u128
        };

        // 用 AMM 常数乘积公式精确计算（和 sol_to_token_quote 相同逻辑）
        let our_sol_amount = config.buy_lamports();
        let numerator = estimated_v_token * (our_sol_amount as u128);
        let denominator = estimated_v_sol + (our_sol_amount as u128);
        let our_token_amount = (numerator / denominator).max(1) as u64;
        self.validate_buy_amount(our_token_amount, None)?;
        let our_max_sol_cost = our_sol_amount + (our_sol_amount * config.slippage_bps / 10_000);

        debug!(
            "报价(目标推算): {} SOL → {} raw tokens | 目标: {} SOL → {} raw tokens | {} accounts",
            config.buy_sol_amount,
            our_token_amount,
            target_max_sol as f64 / 1e9,
            target_token_amount,
            mirror_accounts.len(),
        );

        let buy_ix = self.build_buy_instruction_from_mirror(
            &config.pubkey,
            user_ata,
            source_wallet,
            mirror_accounts,
            our_token_amount,
            our_max_sol_cost,
        );

        let create_ata_ix = create_associated_token_account_idempotent(
            &config.pubkey,
            &config.pubkey,
            mint,
            token_program_id,
        );

        Ok((
            MirrorInstruction {
                swap_instructions: vec![buy_ix],
                pre_instructions: vec![create_ata_ix],
                post_instructions: vec![],
                token_mint: *mint,
                sol_amount: our_sol_amount,
            },
            our_token_amount,
        ))
    }
}

#[async_trait::async_trait]
impl TradeProcessor for PumpfunProcessor {
    fn trade_type(&self) -> TradeType {
        TradeType::Pumpfun
    }

    async fn build_mirror_instructions(
        &self,
        trade: &DetectedTrade,
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        let mint = trade
            .token_mint
            .or_else(|| trade.instruction_accounts.get(2).copied())
            .ok_or_else(|| anyhow::anyhow!("Pumpfun instruction missing token mint"))?;
        let token_prog = trade
            .token_program
            .or_else(|| trade.instruction_accounts.get(8).copied())
            .unwrap_or_else(|| Pubkey::from_str(TOKEN_PROGRAM).unwrap());
        let user_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &config.pubkey,
            &mint,
            &token_prog,
        );

        if trade.is_buy {
            self.buy_with_mirror(
                &mint,
                &user_ata,
                &token_prog,
                &trade.source_wallet,
                &trade.instruction_accounts,
                config,
            )
            .await
        } else {
            let balance = self
                .rpc_client
                .get_token_account_balance(&user_ata)
                .map(|b| b.amount.parse::<u64>().unwrap_or(0))
                .unwrap_or(0);
            if balance == 0 {
                anyhow::bail!("No tokens to sell for mint {}", mint);
            }
            self.sell_with_mirror(
                &mint,
                balance,
                &user_ata,
                &trade.source_wallet,
                &trade.instruction_accounts,
                config,
            )
            .await
        }
    }
}
