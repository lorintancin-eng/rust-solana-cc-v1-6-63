use anyhow::Result;
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
use tracing::info;

use crate::config::AppConfig;
use crate::processor::{DetectedTrade, MirrorInstruction, TradeProcessor, TradeType};

const PUMPSWAP_PROGRAM_ID: &str = "PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

// PumpSwap buy/sell discriminators (Anchor sighash)
const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234]; // TODO: 验证实际 discriminator
const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173]; // TODO: 验证实际 discriminator

pub struct PumpSwapProcessor {
    rpc_client: Arc<RpcClient>,
}

impl PumpSwapProcessor {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }

    /// 从交易指令中提取 PumpSwap 相关账户
    fn extract_accounts(&self, trade: &DetectedTrade) -> Result<PumpSwapAccounts> {
        let accts = &trade.instruction_accounts;
        // PumpSwap account layout (需要根据实际 IDL 调整):
        // 0: pool
        // 1: user
        // 2: user_base_token_account
        // 3: user_quote_token_account
        // 4: pool_base_token_account
        // 5: pool_quote_token_account
        // 6: base_mint
        // 7: quote_mint
        // 8: token_program
        // 9: protocol_fee_recipient
        if accts.len() < 10 {
            anyhow::bail!("PumpSwap: expected >= 10 accounts, got {}", accts.len());
        }

        Ok(PumpSwapAccounts {
            pool: accts[0],
            base_mint: accts[6],
            quote_mint: accts[7],
            pool_base_ata: accts[4],
            pool_quote_ata: accts[5],
            protocol_fee_recipient: accts[9],
        })
    }
}

#[derive(Debug)]
struct PumpSwapAccounts {
    pool: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    pool_base_ata: Pubkey,
    pool_quote_ata: Pubkey,
    protocol_fee_recipient: Pubkey,
}

#[async_trait::async_trait]
impl TradeProcessor for PumpSwapProcessor {
    fn trade_type(&self) -> TradeType {
        TradeType::PumpSwap
    }

    async fn build_mirror_instructions(
        &self,
        trade: &DetectedTrade,
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        let accounts = self.extract_accounts(trade)?;

        info!(
            "Building PumpSwap {} mirror for pool {} | base_mint: {}",
            if trade.is_buy { "BUY" } else { "SELL" },
            accounts.pool,
            accounts.base_mint,
        );

        let user = &config.pubkey;
        let token_program = Pubkey::from_str(TOKEN_PROGRAM).unwrap();

        // 用户的 token ATAs
        let user_base_ata = get_associated_token_address(user, &accounts.base_mint);
        let user_quote_ata = get_associated_token_address(user, &accounts.quote_mint);

        // 预创建 ATAs
        let pre_instructions = vec![
            create_associated_token_account_idempotent(
                user,
                user,
                &accounts.base_mint,
                &token_program,
            ),
            create_associated_token_account_idempotent(
                user,
                user,
                &accounts.quote_mint,
                &token_program,
            ),
        ];

        let program_id = Pubkey::from_str(PUMPSWAP_PROGRAM_ID).unwrap();

        if trade.is_buy {
            let sol_amount = config.buy_lamports();
            let _max_sol = sol_amount + (sol_amount * config.slippage_bps / 10_000);

            // 构建 buy 指令数据
            // TODO: 根据 PumpSwap IDL 构建正确的指令数据
            // 包括: discriminator + amount_in + min_amount_out
            let mut data = Vec::with_capacity(24);
            data.extend_from_slice(&BUY_DISCRIMINATOR);
            data.extend_from_slice(&sol_amount.to_le_bytes());
            data.extend_from_slice(&0u64.to_le_bytes()); // min_out, 需要链上查询计算

            let swap_accounts = vec![
                AccountMeta::new(accounts.pool, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(user_base_ata, false),
                AccountMeta::new(user_quote_ata, false),
                AccountMeta::new(accounts.pool_base_ata, false),
                AccountMeta::new(accounts.pool_quote_ata, false),
                AccountMeta::new_readonly(accounts.base_mint, false),
                AccountMeta::new_readonly(accounts.quote_mint, false),
                AccountMeta::new_readonly(token_program, false),
                AccountMeta::new(accounts.protocol_fee_recipient, false),
                AccountMeta::new_readonly(system_program::id(), false),
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
                token_mint: accounts.base_mint,
                sol_amount,
            })
        } else {
            // SELL 逻辑
            let balance = self
                .rpc_client
                .get_token_account_balance(&user_base_ata)
                .map(|b| b.amount.parse::<u64>().unwrap_or(0))
                .unwrap_or(0);

            if balance == 0 {
                anyhow::bail!("No tokens to sell for PumpSwap pool {}", accounts.pool);
            }

            let mut data = Vec::with_capacity(24);
            data.extend_from_slice(&SELL_DISCRIMINATOR);
            data.extend_from_slice(&balance.to_le_bytes());
            data.extend_from_slice(&0u64.to_le_bytes()); // min_sol_out

            let swap_accounts = vec![
                AccountMeta::new(accounts.pool, false),
                AccountMeta::new(*user, true),
                AccountMeta::new(user_base_ata, false),
                AccountMeta::new(user_quote_ata, false),
                AccountMeta::new(accounts.pool_base_ata, false),
                AccountMeta::new(accounts.pool_quote_ata, false),
                AccountMeta::new_readonly(accounts.base_mint, false),
                AccountMeta::new_readonly(accounts.quote_mint, false),
                AccountMeta::new_readonly(token_program, false),
                AccountMeta::new(accounts.protocol_fee_recipient, false),
                AccountMeta::new_readonly(system_program::id(), false),
            ];

            let swap_ix = Instruction {
                program_id,
                accounts: swap_accounts,
                data,
            };

            Ok(MirrorInstruction {
                swap_instructions: vec![swap_ix],
                pre_instructions: vec![],
                post_instructions: vec![],
                token_mint: accounts.base_mint,
                sol_amount: 0, // 卖出时不知道确切收入
            })
        }
    }
}
