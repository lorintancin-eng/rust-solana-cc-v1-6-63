use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// 预缓存最新 blockhash，避免交易构建时的 RPC 延迟
/// 使用 std::sync::RwLock（非 async）— 读取路径零开销，无需 .await
#[derive(Clone)]
pub struct BlockhashCache {
    inner: Arc<RwLock<CachedBlockhash>>,
}

struct CachedBlockhash {
    hash: Hash,
    last_valid_block_height: u64,
    fetched_at: Instant,
}

impl BlockhashCache {
    pub fn new(initial_hash: Hash, last_valid_block_height: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CachedBlockhash {
                hash: initial_hash,
                last_valid_block_height,
                fetched_at: Instant::now(),
            })),
        }
    }

    /// 获取缓存的 blockhash（同步，零延迟，无需 .await）
    pub fn get_sync(&self) -> (Hash, u64) {
        let cached = self.inner.read().unwrap();
        (cached.hash, cached.last_valid_block_height)
    }

    /// 兼容旧接口（async wrapper）
    pub async fn get(&self) -> (Hash, u64) {
        self.get_sync()
    }

    /// 获取缓存年龄（毫秒）
    pub fn age_ms_sync(&self) -> u128 {
        let cached = self.inner.read().unwrap();
        cached.fetched_at.elapsed().as_millis()
    }

    /// 启动后台刷新任务
    /// 每 120ms 刷新一次，尽量减少 stale blockhash 导致的跨 slot 延迟
    pub fn start_refresh_task(
        &self,
        rpc_client: Arc<RpcClient>,
        refresh_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let inner = self.inner.clone();

        tokio::spawn(async move {
            let mut consecutive_errors = 0u32;

            loop {
                let rpc = rpc_client.clone();
                let result = tokio::task::spawn_blocking(move || {
                    rpc.get_latest_blockhash_with_commitment(
                        solana_sdk::commitment_config::CommitmentConfig::processed(),
                    )
                })
                .await;

                match result {
                    Ok(Ok((hash, last_valid_block_height))) => {
                        let mut cached = inner.write().unwrap();
                        let old_hash = cached.hash;
                        cached.hash = hash;
                        cached.last_valid_block_height = last_valid_block_height;
                        cached.fetched_at = Instant::now();

                        if hash != old_hash {
                            debug!(
                                "Blockhash updated: {} (height: {})",
                                hash, last_valid_block_height
                            );
                        }
                        consecutive_errors = 0;
                    }
                    Ok(Err(e)) => {
                        consecutive_errors += 1;
                        if consecutive_errors <= 3 {
                            warn!("Blockhash fetch error ({}): {}", consecutive_errors, e);
                        } else {
                            error!(
                                "Blockhash fetch failing repeatedly ({}): {}",
                                consecutive_errors, e
                            );
                        }
                    }
                    Err(e) => {
                        error!("Blockhash task panicked: {}", e);
                    }
                }

                tokio::time::sleep(refresh_interval).await;
            }
        })
    }
}

/// 初始化 blockhash 缓存
pub async fn init_blockhash_cache(rpc_client: &RpcClient) -> Result<BlockhashCache> {
    let (hash, last_valid_block_height) = rpc_client.get_latest_blockhash_with_commitment(
        solana_sdk::commitment_config::CommitmentConfig::processed(),
    )?;

    let cache = BlockhashCache::new(hash, last_valid_block_height);
    tracing::info!("Blockhash cache initialized: {}", hash);
    Ok(cache)
}
