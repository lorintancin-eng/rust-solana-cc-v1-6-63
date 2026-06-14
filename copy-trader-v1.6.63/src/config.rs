use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

/// 全局配置，从 .env 加载
#[derive(Debug, Clone)]
pub struct AppConfig {
    // RPC & gRPC
    pub rpc_url: String,
    pub secondary_rpc_url: Option<String>,
    /// 交易监听流默认走 Shyft RabbitStream pre-exec。
    /// 兼容旧变量：优先读取 RABBITSTREAM_URL，其次 GRPC_URL。
    pub grpc_url: String,
    /// 交易监听流 token。
    /// 兼容旧变量：优先读取 RABBITSTREAM_TOKEN，其次 GRPC_TOKEN。
    pub grpc_token: Option<String>,
    /// 账户监控用的 gRPC URL（RabbitStream 不支持账户订阅，需要普通 gRPC）
    /// 不设置时回退到 SHYFT_GRPC_URL / GRPC_ACCOUNT_URL；再退到旧的 GRPC_URL
    pub grpc_account_url: String,
    pub grpc_account_token: Option<String>,

    // Wallet
    pub keypair: std::sync::Arc<Keypair>,
    pub pubkey: Pubkey,

    // Target wallets to copy
    pub target_wallets: Vec<Pubkey>,

    // Consensus
    pub consensus_min_wallets: usize,
    pub consensus_timeout_secs: u64,

    // Trading
    pub buy_sol_amount: f64,
    pub slippage_bps: u64,
    /// 卖出专用滑点（meme 币卖出波动大，需要更高滑点）
    pub sell_slippage_bps: u64,
    pub compute_units: u32,
    pub priority_fee_micro_lamport: u64,
    /// 目标钱包最小买入 SOL 数（过滤小额噪音），0 表示不过滤
    /// .env: MIN_TARGET_BUY_SOL=0.5
    pub min_target_buy_sol: f64,

    // Jito
    pub jito_enabled: bool,
    pub jito_block_engine_urls: Vec<String>,
    pub jito_buy_tip_lamports: u64,
    pub jito_sell_tip_lamports: u64,
    /// Jito 认证 UUID（x-jito-auth header），大幅提升 rate limit
    /// VPS 上用 `uuidgen` 生成，填入 .env JITO_AUTH_UUID
    pub jito_auth_uuid: Option<String>,

    // 0slot staked connection（质押加速，提升同区块率）
    /// 0slot endpoint URLs（带 api-key），逗号分隔
    /// 例: http://ny1.0slot.trade/?api-key=xxx,http://la1.0slot.trade/?api-key=xxx
    pub zero_slot_urls: Vec<String>,
    /// 0slot fee（lamports），默认 0.001 SOL
    pub zero_slot_tip_lamports: u64,

    // Confirmation
    pub confirm_timeout_secs: u64,

    // Auto-sell
    pub auto_sell_enabled: bool,
    pub take_profit_percent: f64,
    pub stop_loss_percent: f64,
    pub trailing_stop_percent: f64,
    pub max_hold_seconds: u64,
    pub price_check_interval_secs: u64,
    /// SOL/USD 默认价格（API 获取失败时使用）
    pub default_sol_usd_price: f64,

    // Telegram Bot
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let private_key_str = std::env::var("PRIVATE_KEY").context("PRIVATE_KEY not set")?;
        let keypair = parse_keypair(&private_key_str)?;
        let pubkey = keypair.pubkey();

        let target_wallets_str =
            std::env::var("TARGET_WALLETS").context("TARGET_WALLETS not set")?;
        let target_wallets: Vec<Pubkey> = target_wallets_str
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| Pubkey::from_str(s.trim()))
            .collect::<Result<Vec<_>, _>>()
            .context("Invalid TARGET_WALLETS")?;

        Ok(Self {
            rpc_url: env_or("RPC_URL", "https://api.mainnet-beta.solana.com"),
            secondary_rpc_url: std::env::var("SECONDARY_RPC_URL").ok(),
            grpc_url: first_env(&["RABBITSTREAM_URL", "GRPC_URL"])
                .unwrap_or_else(|| "https://grpc.triton.one".to_string()),
            grpc_token: first_env(&["RABBITSTREAM_TOKEN", "GRPC_TOKEN"]),
            // 账户监控 gRPC：显式使用普通 Yellowstone gRPC，避免误接 RabbitStream。
            grpc_account_url: first_env(&["GRPC_ACCOUNT_URL", "SHYFT_GRPC_URL"])
                .or_else(|| first_env(&["GRPC_URL"]).filter(|url| !is_rabbitstream_url(url)))
                .unwrap_or_else(|| "https://grpc.triton.one".to_string()),
            grpc_account_token: first_env(&["GRPC_ACCOUNT_TOKEN", "SHYFT_GRPC_TOKEN"])
                .or_else(|| first_env(&["GRPC_TOKEN", "RABBITSTREAM_TOKEN"])),
            keypair: std::sync::Arc::new(keypair),
            pubkey,
            target_wallets,
            consensus_min_wallets: env_parse("CONSENSUS_MIN_WALLETS", 2),
            consensus_timeout_secs: env_parse("CONSENSUS_TIMEOUT_SECS", 60),
            buy_sol_amount: env_parse("BUY_SOL_AMOUNT", 0.01),
            slippage_bps: env_parse("SLIPPAGE_BPS", 500),
            sell_slippage_bps: env_parse("SELL_SLIPPAGE_BPS", env_parse("SLIPPAGE_BPS", 1500)),
            compute_units: env_parse("COMPUTE_UNITS", 400_000),
            priority_fee_micro_lamport: env_parse("PRIORITY_FEE_MICRO_LAMPORT", 5000),
            min_target_buy_sol: env_parse("MIN_TARGET_BUY_SOL", 0.5),
            jito_enabled: env_parse("JITO_ENABLED", false),
            jito_block_engine_urls: std::env::var("JITO_BLOCK_ENGINE_URL")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|u| u.trim().to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| {
                    vec![
                        "https://mainnet.block-engine.jito.wtf".to_string(),
                        "https://amsterdam.mainnet.block-engine.jito.wtf".to_string(),
                        "https://frankfurt.mainnet.block-engine.jito.wtf".to_string(),
                        "https://ny.mainnet.block-engine.jito.wtf".to_string(),
                        "https://tokyo.mainnet.block-engine.jito.wtf".to_string(),
                    ]
                }),
            jito_buy_tip_lamports: env_parse(
                "JITO_BUY_TIP_LAMPORTS",
                env_parse("JITO_TIP_LAMPORTS", 10_000),
            ),
            jito_sell_tip_lamports: env_parse(
                "JITO_SELL_TIP_LAMPORTS",
                env_parse("JITO_TIP_LAMPORTS", 10_000),
            ),
            jito_auth_uuid: std::env::var("JITO_AUTH_UUID")
                .ok()
                .filter(|s| !s.is_empty()),
            zero_slot_urls: std::env::var("ZERO_SLOT_URLS")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|u| u.trim().to_string())
                        .filter(|u| !u.is_empty())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            zero_slot_tip_lamports: env_parse("ZERO_SLOT_TIP_LAMPORTS", 1_000_000),
            confirm_timeout_secs: env_parse("CONFIRM_TIMEOUT_SECS", 5),
            auto_sell_enabled: env_parse("AUTO_SELL_ENABLED", true),
            take_profit_percent: env_parse("TAKE_PROFIT_PERCENT", 15.0),
            stop_loss_percent: env_parse("STOP_LOSS_PERCENT", 10.0),
            trailing_stop_percent: env_parse("TRAILING_STOP_PERCENT", 5.0),
            max_hold_seconds: env_parse("MAX_HOLD_SECONDS", 120),
            price_check_interval_secs: env_parse("PRICE_CHECK_INTERVAL_SECS", 3),
            default_sol_usd_price: env_parse("DEFAULT_SOL_USD_PRICE", 83.0),
            telegram_bot_token: std::env::var("TELEGRAM_BOT_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
            telegram_chat_id: std::env::var("TELEGRAM_CHAT_ID")
                .ok()
                .filter(|s| !s.is_empty()),
        })
    }

    /// 买入的 lamports 数量
    pub fn buy_lamports(&self) -> u64 {
        (self.buy_sol_amount * 1_000_000_000.0) as u64
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        std::env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn is_rabbitstream_url(url: &str) -> bool {
    url.to_ascii_lowercase().contains("rabbitstream")
}

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ============================================
// DynConfig: 运行时可动态修改的参数（原子操作，无锁）
// ============================================

fn store_f64(atom: &AtomicU64, val: f64) {
    atom.store(val.to_bits(), Ordering::Relaxed);
}
fn load_f64(atom: &AtomicU64) -> f64 {
    f64::from_bits(atom.load(Ordering::Relaxed))
}

/// 卖出模式: 0=止盈止损模式(TP/SL/Trailing), 1=跟卖模式(Follow Smart Money Sell)
pub const SELL_MODE_TP_SL: u8 = 0;
pub const SELL_MODE_FOLLOW: u8 = 1;

/// 可在运行时通过 TG /set 修改的参数
pub struct DynConfig {
    buy_sol_amount: AtomicU64, // f64 bits
    slippage_bps: AtomicU64,
    sell_slippage_bps: AtomicU64,
    take_profit_percent: AtomicU64,   // f64 bits
    stop_loss_percent: AtomicU64,     // f64 bits
    trailing_stop_percent: AtomicU64, // f64 bits
    max_hold_seconds: AtomicU64,
    consensus_min_wallets: AtomicU64,
    jito_buy_tip_lamports: AtomicU64,
    jito_sell_tip_lamports: AtomicU64,
    zero_slot_tip_lamports: AtomicU64,
    /// 卖出模式: SELL_MODE_TP_SL(0) 或 SELL_MODE_FOLLOW(1)
    sell_mode: AtomicU8,
    /// 目标钱包最小买入 SOL 过滤
    min_target_buy_sol: AtomicU64, // f64 bits
    /// 跟踪钱包列表（需要重启 gRPC 订阅才生效）
    pub target_wallets: RwLock<Vec<Pubkey>>,
    /// 代币黑名单
    pub blocklist: dashmap::DashSet<Pubkey>,
}

impl DynConfig {
    pub fn from_config(config: &AppConfig) -> Arc<Self> {
        Arc::new(Self {
            buy_sol_amount: AtomicU64::new(config.buy_sol_amount.to_bits()),
            slippage_bps: AtomicU64::new(config.slippage_bps),
            sell_slippage_bps: AtomicU64::new(config.sell_slippage_bps),
            take_profit_percent: AtomicU64::new(config.take_profit_percent.to_bits()),
            stop_loss_percent: AtomicU64::new(config.stop_loss_percent.to_bits()),
            trailing_stop_percent: AtomicU64::new(config.trailing_stop_percent.to_bits()),
            max_hold_seconds: AtomicU64::new(config.max_hold_seconds),
            consensus_min_wallets: AtomicU64::new(config.consensus_min_wallets as u64),
            jito_buy_tip_lamports: AtomicU64::new(config.jito_buy_tip_lamports),
            jito_sell_tip_lamports: AtomicU64::new(config.jito_sell_tip_lamports),
            zero_slot_tip_lamports: AtomicU64::new(config.zero_slot_tip_lamports),
            sell_mode: AtomicU8::new(SELL_MODE_TP_SL),
            min_target_buy_sol: AtomicU64::new(config.min_target_buy_sol.to_bits()),
            target_wallets: RwLock::new(config.target_wallets.clone()),
            blocklist: dashmap::DashSet::new(),
        })
    }

    // Getters
    pub fn buy_sol_amount(&self) -> f64 {
        load_f64(&self.buy_sol_amount)
    }
    pub fn buy_lamports(&self) -> u64 {
        (self.buy_sol_amount() * 1e9) as u64
    }
    pub fn slippage_bps(&self) -> u64 {
        self.slippage_bps.load(Ordering::Relaxed)
    }
    pub fn sell_slippage_bps(&self) -> u64 {
        self.sell_slippage_bps.load(Ordering::Relaxed)
    }
    pub fn take_profit_percent(&self) -> f64 {
        load_f64(&self.take_profit_percent)
    }
    pub fn stop_loss_percent(&self) -> f64 {
        load_f64(&self.stop_loss_percent)
    }
    pub fn trailing_stop_percent(&self) -> f64 {
        load_f64(&self.trailing_stop_percent)
    }
    pub fn max_hold_seconds(&self) -> u64 {
        self.max_hold_seconds.load(Ordering::Relaxed)
    }
    pub fn consensus_min_wallets(&self) -> usize {
        self.consensus_min_wallets.load(Ordering::Relaxed) as usize
    }
    pub fn jito_buy_tip_lamports(&self) -> u64 {
        self.jito_buy_tip_lamports.load(Ordering::Relaxed)
    }
    pub fn jito_sell_tip_lamports(&self) -> u64 {
        self.jito_sell_tip_lamports.load(Ordering::Relaxed)
    }
    pub fn zero_slot_tip_lamports(&self) -> u64 {
        self.zero_slot_tip_lamports.load(Ordering::Relaxed)
    }
    pub fn sell_mode(&self) -> u8 {
        self.sell_mode.load(Ordering::Relaxed)
    }
    pub fn is_follow_sell_mode(&self) -> bool {
        self.sell_mode() == SELL_MODE_FOLLOW
    }
    pub fn min_target_buy_sol(&self) -> f64 {
        load_f64(&self.min_target_buy_sol)
    }
    pub fn min_target_buy_lamports(&self) -> u64 {
        (self.min_target_buy_sol() * 1e9) as u64
    }

    // Setters
    pub fn set_buy_sol_amount(&self, v: f64) {
        store_f64(&self.buy_sol_amount, v);
    }
    pub fn set_slippage_bps(&self, v: u64) {
        self.slippage_bps.store(v, Ordering::Relaxed);
    }
    pub fn set_sell_slippage_bps(&self, v: u64) {
        self.sell_slippage_bps.store(v, Ordering::Relaxed);
    }
    pub fn set_take_profit_percent(&self, v: f64) {
        store_f64(&self.take_profit_percent, v);
    }
    pub fn set_stop_loss_percent(&self, v: f64) {
        store_f64(&self.stop_loss_percent, v);
    }
    pub fn set_trailing_stop_percent(&self, v: f64) {
        store_f64(&self.trailing_stop_percent, v);
    }
    pub fn set_max_hold_seconds(&self, v: u64) {
        self.max_hold_seconds.store(v, Ordering::Relaxed);
    }
    pub fn set_consensus_min_wallets(&self, v: usize) {
        self.consensus_min_wallets
            .store(v as u64, Ordering::Relaxed);
    }
    pub fn set_jito_buy_tip_lamports(&self, v: u64) {
        self.jito_buy_tip_lamports.store(v, Ordering::Relaxed);
    }
    pub fn set_jito_sell_tip_lamports(&self, v: u64) {
        self.jito_sell_tip_lamports.store(v, Ordering::Relaxed);
    }
    pub fn set_zero_slot_tip_lamports(&self, v: u64) {
        self.zero_slot_tip_lamports.store(v, Ordering::Relaxed);
    }
    pub fn set_sell_mode(&self, v: u8) {
        self.sell_mode.store(v, Ordering::Relaxed);
    }
    pub fn set_min_target_buy_sol(&self, v: f64) {
        store_f64(&self.min_target_buy_sol, v);
    }

    /// 是否已拉黑该代币
    pub fn is_blocked(&self, mint: &Pubkey) -> bool {
        self.blocklist.contains(mint)
    }
}

/// 支持 base58 私钥 或 JSON 数组格式
fn parse_keypair(s: &str) -> Result<Keypair> {
    // 尝试 JSON 数组格式 [1,2,3,...]
    if s.starts_with('[') {
        let bytes: Vec<u8> =
            serde_json::from_str(s).context("Failed to parse PRIVATE_KEY as JSON array")?;
        return Keypair::try_from(bytes.as_slice())
            .map_err(|e| anyhow::anyhow!("Invalid keypair bytes: {}", e));
    }
    // 尝试 base58
    let bytes = bs58::decode(s)
        .into_vec()
        .context("Failed to decode PRIVATE_KEY as base58")?;
    Keypair::try_from(bytes.as_slice()).map_err(|e| anyhow::anyhow!("Invalid keypair bytes: {}", e))
}
