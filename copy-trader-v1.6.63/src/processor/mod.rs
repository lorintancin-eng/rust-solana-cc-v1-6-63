pub mod prefetch;
pub mod pumpfun;
pub mod pumpswap;
pub mod raydium_amm;
pub mod raydium_cpmm;

use anyhow::Result;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use std::fmt;
use std::time::Instant;

use crate::config::AppConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeType {
    Pumpfun,
    PumpSwap,
    RaydiumAmm,
    RaydiumCpmm,
}

impl fmt::Display for TradeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TradeType::Pumpfun => write!(f, "Pump.fun"),
            TradeType::PumpSwap => write!(f, "PumpSwap"),
            TradeType::RaydiumAmm => write!(f, "Raydium AMM"),
            TradeType::RaydiumCpmm => write!(f, "Raydium CPMM"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeOrigin {
    Direct,
    WrapperCpi,
}

impl TradeOrigin {
    pub fn uses_mirror_accounts(self) -> bool {
        matches!(self, Self::Direct)
    }

    pub fn is_wrapper_cpi(self) -> bool {
        matches!(self, Self::WrapperCpi)
    }
}

impl fmt::Display for TradeOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TradeOrigin::Direct => write!(f, "direct"),
            TradeOrigin::WrapperCpi => write!(f, "wrapper_cpi"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DetectedTrade {
    pub signature: String,
    pub source_wallet: Pubkey,
    pub trade_type: TradeType,
    pub trade_origin: TradeOrigin,
    pub is_buy: bool,
    pub program_id: Pubkey,
    pub instruction_data: Vec<u8>,
    pub instruction_accounts: Vec<Pubkey>,
    pub all_account_keys: Vec<Pubkey>,
    pub detected_at: Instant,
    pub sol_amount_lamports: u64,
    pub raw_transaction_bytes: Vec<u8>,
    pub is_pre_execution: bool,
    pub execution_failed: bool,
    pub token_mint: Option<Pubkey>,
    pub token_program: Option<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct MirrorInstruction {
    pub swap_instructions: Vec<Instruction>,
    pub pre_instructions: Vec<Instruction>,
    pub post_instructions: Vec<Instruction>,
    pub token_mint: Pubkey,
    pub sol_amount: u64,
}

#[async_trait::async_trait]
pub trait TradeProcessor: Send + Sync {
    fn trade_type(&self) -> TradeType;

    async fn build_mirror_instructions(
        &self,
        trade: &DetectedTrade,
        config: &AppConfig,
    ) -> Result<MirrorInstruction>;
}

pub struct ProcessorRegistry {
    processors: Vec<Box<dyn TradeProcessor>>,
}

impl ProcessorRegistry {
    pub fn new() -> Self {
        Self {
            processors: Vec::new(),
        }
    }

    pub fn register(&mut self, processor: Box<dyn TradeProcessor>) {
        tracing::info!("已注册处理器: {}", processor.trade_type());
        self.processors.push(processor);
    }

    pub fn get_processor(&self, trade_type: TradeType) -> Option<&dyn TradeProcessor> {
        self.processors
            .iter()
            .find(|p| p.trade_type() == trade_type)
            .map(|p| p.as_ref())
    }

    pub fn register_all_defaults(
        &mut self,
        rpc_client: std::sync::Arc<solana_client::rpc_client::RpcClient>,
    ) {
        self.register(Box::new(pumpfun::PumpfunProcessor::new(rpc_client.clone())));
        self.register(Box::new(pumpswap::PumpSwapProcessor::new(
            rpc_client.clone(),
        )));
        self.register(Box::new(raydium_amm::RaydiumAmmProcessor::new(
            rpc_client.clone(),
        )));
        self.register(Box::new(raydium_cpmm::RaydiumCpmmProcessor::new(
            rpc_client,
        )));
    }
}
