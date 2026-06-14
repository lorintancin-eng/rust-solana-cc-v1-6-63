use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account_idempotent,
};
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

use crate::config::AppConfig;
use crate::processor::{DetectedTrade, MirrorInstruction, TradeProcessor, TradeType};

const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Raydium AMM V4 swap_base_in 指令 index
const SWAP_BASE_IN: u8 = 9;

pub struct RaydiumAmmProcessor {
    rpc_client: Arc<RpcClient>,
}

impl RaydiumAmmProcessor {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }

    /// 从交易指令提取 Raydium AMM 账户
    /// Raydium AMM V4 SwapBaseIn account layout:
    ///  0: token_program
    ///  1: amm_id
    ///  2: amm_authority
    ///  3: amm_open_orders
    ///  4: amm_target_orders (可能已废弃，但仍需传入)
    ///  5: pool_coin_token_account
    ///  6: pool_pc_token_account
    ///  7: serum_program_id
    ///  8: serum_market
    ///  9: serum_bids
    /// 10: serum_asks
    /// 11: serum_event_queue
    /// 12: serum_coin_vault
    /// 13: serum_pc_vault
    /// 14: serum_vault_signer
    /// 15: user_source_token_account
    /// 16: user_destination_token_account
    /// 17: user_owner (signer)
    fn extract_accounts(&self, trade: &DetectedTrade) -> Result<RaydiumAmmAccounts> {
        let accts = &trade.instruction_accounts;
        if accts.len() < 18 {
            anyhow::bail!("Raydium AMM: expected >= 18 accounts, got {}", accts.len());
        }

        Ok(RaydiumAmmAccounts {
            amm_id: accts[1],
            amm_authority: accts[2],
            amm_open_orders: accts[3],
            amm_target_orders: accts[4],
            pool_coin_token_account: accts[5],
            pool_pc_token_account: accts[6],
            serum_program_id: accts[7],
            serum_market: accts[8],
            serum_bids: accts[9],
            serum_asks: accts[10],
            serum_event_queue: accts[11],
            serum_coin_vault: accts[12],
            serum_pc_vault: accts[13],
            serum_vault_signer: accts[14],
        })
    }

    /// 从 pool 的 coin/pc 账户推断 token mint
    fn detect_token_mint(&self, accts: &RaydiumAmmAccounts) -> Result<Pubkey> {
        // 查询 pool_coin_token_account 的 mint
        let account_data = self
            .rpc_client
            .get_account(&accts.pool_coin_token_account)
            .context("Failed to fetch pool coin account")?;

        // SPL Token account data: first 32 bytes = mint
        if account_data.data.len() < 32 {
            anyhow::bail!("Invalid token account data");
        }
        let mint = Pubkey::new_from_array(account_data.data[..32].try_into()?);
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();

        // 如果 coin 是 WSOL，那 token mint 是 pc；反之亦然
        if mint == wsol {
            let pc_data = self
                .rpc_client
                .get_account(&accts.pool_pc_token_account)
                .context("Failed to fetch pool pc account")?;
            Ok(Pubkey::new_from_array(pc_data.data[..32].try_into()?))
        } else {
            Ok(mint)
        }
    }
}

#[derive(Debug)]
struct RaydiumAmmAccounts {
    amm_id: Pubkey,
    amm_authority: Pubkey,
    amm_open_orders: Pubkey,
    amm_target_orders: Pubkey,
    pool_coin_token_account: Pubkey,
    pool_pc_token_account: Pubkey,
    serum_program_id: Pubkey,
    serum_market: Pubkey,
    serum_bids: Pubkey,
    serum_asks: Pubkey,
    serum_event_queue: Pubkey,
    serum_coin_vault: Pubkey,
    serum_pc_vault: Pubkey,
    serum_vault_signer: Pubkey,
}

#[async_trait::async_trait]
impl TradeProcessor for RaydiumAmmProcessor {
    fn trade_type(&self) -> TradeType {
        TradeType::RaydiumAmm
    }

    async fn build_mirror_instructions(
        &self,
        trade: &DetectedTrade,
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        let amm_accts = self.extract_accounts(trade)?;
        let token_mint = self.detect_token_mint(&amm_accts)?;
        let wsol_mint = Pubkey::from_str(WSOL_MINT).unwrap();
        let token_program = Pubkey::from_str(TOKEN_PROGRAM).unwrap();
        let user = &config.pubkey;

        info!(
            "Building Raydium AMM {} mirror | amm: {} | mint: {}",
            if trade.is_buy { "BUY" } else { "SELL" },
            amm_accts.amm_id,
            token_mint,
        );

        let user_wsol_ata = get_associated_token_address(user, &wsol_mint);
        let user_token_ata = get_associated_token_address(user, &token_mint);

        let pre_instructions = vec![
            create_associated_token_account_idempotent(user, user, &wsol_mint, &token_program),
            create_associated_token_account_idempotent(user, user, &token_mint, &token_program),
        ];

        let program_id = Pubkey::from_str(RAYDIUM_AMM_V4).unwrap();

        let sol_amount = config.buy_lamports();

        // SwapBaseIn 指令数据: [instruction_index(1), amount_in(8), min_amount_out(8)]
        let (amount_in, min_out, source_ata, dest_ata) = if trade.is_buy {
            let min_out = 0u64; // TODO: 链上查询 pool reserves 计算最小输出
            (sol_amount, min_out, user_wsol_ata, user_token_ata)
        } else {
            let balance = self
                .rpc_client
                .get_token_account_balance(&user_token_ata)
                .map(|b| b.amount.parse::<u64>().unwrap_or(0))
                .unwrap_or(0);
            if balance == 0 {
                anyhow::bail!("No tokens to sell on Raydium AMM");
            }
            (balance, 0u64, user_token_ata, user_wsol_ata)
        };

        let mut data = Vec::with_capacity(17);
        data.push(SWAP_BASE_IN);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());

        let swap_accounts = vec![
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new(amm_accts.amm_id, false),
            AccountMeta::new_readonly(amm_accts.amm_authority, false),
            AccountMeta::new(amm_accts.amm_open_orders, false),
            AccountMeta::new(amm_accts.amm_target_orders, false),
            AccountMeta::new(amm_accts.pool_coin_token_account, false),
            AccountMeta::new(amm_accts.pool_pc_token_account, false),
            AccountMeta::new_readonly(amm_accts.serum_program_id, false),
            AccountMeta::new(amm_accts.serum_market, false),
            AccountMeta::new(amm_accts.serum_bids, false),
            AccountMeta::new(amm_accts.serum_asks, false),
            AccountMeta::new(amm_accts.serum_event_queue, false),
            AccountMeta::new(amm_accts.serum_coin_vault, false),
            AccountMeta::new(amm_accts.serum_pc_vault, false),
            AccountMeta::new_readonly(amm_accts.serum_vault_signer, false),
            AccountMeta::new(source_ata, false),
            AccountMeta::new(dest_ata, false),
            AccountMeta::new(*user, true),
        ];

        let swap_ix = Instruction {
            program_id,
            accounts: swap_accounts,
            data,
        };

        Ok(MirrorInstruction {
            swap_instructions: vec![swap_ix],
            pre_instructions,
            post_instructions: vec![],
            token_mint,
            sol_amount,
        })
    }
}
