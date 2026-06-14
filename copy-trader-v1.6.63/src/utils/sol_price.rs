use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{info, warn, debug};

const COINGECKO_URL: &str = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd";
const USER_AGENT: &str = concat!("copy-trader/", env!("CARGO_PKG_VERSION"));

/// SOL/USD 实时汇率（CoinGecko 免费 API + 本地缓存）
#[derive(Clone)]
pub struct SolUsdPrice {
    /// 价格 × 100 存储（整数，避免浮点精度问题）
    price_cents: Arc<AtomicU64>,
}

impl SolUsdPrice {
    pub fn new() -> Self {
        Self {
            price_cents: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 启动时初始化：先尝试 CoinGecko，失败用默认值
    pub async fn init(&self, default_price: f64) {
        match Self::fetch_coingecko().await {
            Some(price) => {
                let cents = (price * 100.0) as u64;
                self.price_cents.store(cents, Ordering::Relaxed);
                info!("SOL/USD 实时价格: ${:.2} (CoinGecko)", price);
            }
            None => {
                let cents = (default_price * 100.0) as u64;
                self.price_cents.store(cents, Ordering::Relaxed);
                warn!("SOL/USD CoinGecko 获取失败，使用默认: ${:.2}", default_price);
            }
        }
    }

    /// 获取当前 SOL/USD 价格
    pub fn get(&self) -> f64 {
        self.price_cents.load(Ordering::Relaxed) as f64 / 100.0
    }

    /// SOL 数量转 USD
    pub fn sol_to_usd(&self, sol: f64) -> f64 {
        sol * self.get()
    }

    /// 后台每 60 秒刷新价格
    pub fn start_refresh_task(&self) -> tokio::task::JoinHandle<()> {
        let price_cents = self.price_cents.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                if let Some(price) = Self::fetch_coingecko().await {
                    let cents = (price * 100.0) as u64;
                    let old = price_cents.swap(cents, Ordering::Relaxed) as f64 / 100.0;
                    if (price - old).abs() > 1.0 {
                        debug!("SOL/USD 更新: ${:.2} → ${:.2}", old, price);
                    }
                }
            }
        })
    }

    /// 从 CoinGecko 免费 API 获取 SOL/USD
    async fn fetch_coingecko() -> Option<f64> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .ok()?;

        let resp = match client
            .get(COINGECKO_URL)
            .header("Accept", "application/json")
            .header("User-Agent", USER_AGENT)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("CoinGecko 请求失败: {}", e);
                return None;
            }
        };

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!("CoinGecko HTTP {}: {}", status, &body[..body.len().min(200)]);
            return None;
        }

        let body: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!("CoinGecko JSON 解析失败: {}", e);
                return None;
            }
        };

        match body["solana"]["usd"].as_f64() {
            Some(price) => Some(price),
            None => {
                warn!("CoinGecko 响应无 solana.usd 字段: {}", body);
                None
            }
        }
    }
}
