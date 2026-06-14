use anyhow::Result;
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

const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

// Anchor discriminator for swap_base_input: sha256("global:swap_base_input")[..8]
const SWAP_BASE_INPUT_DISC: [u8; 8] = [143, 190, 90, 218, 196, 30, 51, 222];

pub struct RaydiumCpmmProcessor {
    rpc_client: Arc<RpcClient>,
}

impl RaydiumCpmmProcessor {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }

    /// CPMM swap account layout:
    ///  0: payer (signer)
    ///  1: authority (PDA)
    ///  2: amm_config
    ///  3: pool_state
    ///  4: input_token_account (user)
    ///  5: output_token_account (user)
    ///  6: input_vault
    ///  7: output_vault
    ///  8: input_token_program
    ///  9: output_token_program
    /// 10: input_token_mint
    /// 11: output_token_mint
    /// 12: observation_state
    fn extract_accounts(&self, trade: &DetectedTrade) -> Result<CpmmAccounts> {
        let accts = &trade.instruction_accounts;
        if accts.len() < 13 {
            anyhow::bail!("Raydium CPMM: expected >= 13 accounts, got {}", accts.len());
        }

        Ok(CpmmAccounts {
            authority: accts[1],
            amm_config: accts[2],
            pool_state: accts[3],
            input_vault: accts[6],
            output_vault: accts[7],
            input_token_mint: accts[10],
            output_token_mint: accts[11],
            observation_state: accts[12],
        })
    }
}

#[derive(Debug)]
struct CpmmAccounts {
    authority: Pubkey,
    amm_config: Pubkey,
    pool_state: Pubkey,
    input_vault: Pubkey,
    output_vault: Pubkey,
    input_token_mint: Pubkey,
    output_token_mint: Pubkey,
    observation_state: Pubkey,
}

#[async_trait::async_trait]
impl TradeProcessor for RaydiumCpmmProcessor {
    fn trade_type(&self) -> TradeType {
        TradeType::RaydiumCpmm
    }

    async fn build_mirror_instructions(
        &self,
        trade: &DetectedTrade,
        config: &AppConfig,
    ) -> Result<MirrorInstruction> {
        let cpmm = self.extract_accounts(trade)?;
        let wsol_mint = Pubkey::from_str(WSOL_MINT).unwrap();
        let token_program = Pubkey::from_str(TOKEN_PROGRAM).unwrap();
        let user = &config.pubkey;

        // 确定 token mint（非 WSOL 的那个）
        let token_mint = if cpmm.input_token_mint == wsol_mint {
            cpmm.output_token_mint
        } else if cpmm.output_token_mint == wsol_mint {
            cpmm.input_token_mint
        } else {
            // 两个都不是 WSOL，取 output 作为 token
            cpmm.output_token_mint
        };

        info!(
            "Building Raydium CPMM {} mirror | pool: {} | mint: {}",
            if trade.is_buy { "BUY" } else { "SELL" },
            cpmm.pool_state,
            token_mint,
        );

        let user_wsol_ata = get_associated_token_address(user, &wsol_mint);
        let user_token_ata = get_associated_token_address(user, &token_mint);

        let pre_instructions = vec![
            create_associated_token_account_idempotent(user, user, &wsol_mint, &token_program),
            create_associated_token_account_idempotent(user, user, &token_mint, &token_program),
        ];

        let program_id = Pubkey::from_str(RAYDIUM_CPMM).unwrap();
        let sol_amount = config.buy_lamports();

        // 构建 swap 指令
        let (amount_in, min_out, input_ata, output_ata, input_mint, output_mint) = if trade.is_buy {
            (sol_amount, 0u64, user_wsol_ata, user_token_ata, wsol_mint, token_mint)
        } else {
            let balance = self
                .rpc_client
                .get_token_account_balance(&user_token_ata)
                .map(|b| b.amount.parse::<u64>().unwrap_or(0))
                .unwrap_or(0);
            if balance == 0 {
                anyhow::bail!("No tokens to sell on Raydium CPMM");
            }
            (balance, 0u64, user_token_ata, user_wsol_ata, token_mint, wsol_mint)
        };

        // 指令数据: discriminator(8) + amount_in(8) + minimum_amount_out(8)
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&SWAP_BASE_INPUT_DISC);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());

        let swap_accounts = vec![
            AccountMeta::new(*user, true),           // payer
            AccountMeta::new_readonly(cpmm.authority, false), // authority PDA
            AccountMeta::new_readonly(cpmm.amm_config, false),
            AccountMeta::new(cpmm.pool_state, false),
            AccountMeta::new(input_ata, false),       // user input
            AccountMeta::new(output_ata, false),      // user output
            AccountMeta::new(cpmm.input_vault, false),
            AccountMeta::new(cpmm.output_vault, false),
            AccountMeta::new_readonly(token_program, false), // input token program
            AccountMeta::new_readonly(token_program, false), // output token program
            AccountMeta::new_readonly(input_mint, false),
            AccountMeta::new_readonly(output_mint, false),
            AccountMeta::new(cpmm.observation_state, false),
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
