use anyhow::{Context, Result};
use solana_sdk::{signature::Signature, transaction::VersionedTransaction};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// 交易发送器 — 多通道梯度提交（同区块优化版）
///
/// 全面使用 HTTP JSON-RPC 发送 VersionedTransaction（base64 编码）
/// 优化要点:
/// 1. 预序列化交易，所有通道复用同一份 bytes（避免重复序列化）
/// 2. 所有通道 T+0 并发发送
/// 3. fire-and-forget 模式：立即返回，不等待通道结果
/// 4. Jito endpoint 轮换：原子计数器分散限频压力
/// 5. 0slot staked connection：质押加速，提升同区块率
pub struct TxSender {
    /// 主 RPC URL (Shyft)
    primary_rpc_url: String,
    /// 备用 RPC URL (Helius)
    secondary_rpc_url: Option<String>,
    /// Jito block engine URLs (多端点轮换)
    jito_block_engine_urls: Vec<String>,
    /// Jito TX URLs（仅用于 sendTransaction，排除 bundles-only relay）
    jito_tx_urls: Vec<String>,
    jito_enabled: bool,
    /// Jito 认证 UUID（x-jito-auth header）
    jito_auth_uuid: Option<String>,
    /// 0slot staked connection URLs（质押加速，最高优先级通道）
    zero_slot_urls: Vec<String>,
    http_client: reqwest::Client,
    /// Jito endpoint 轮换计数器（原子操作，~1ns）
    jito_url_counter: AtomicUsize,
}

impl TxSender {
    pub fn new(
        primary_rpc_url: String,
        secondary_rpc_url: Option<String>,
        jito_block_engine_urls: Vec<String>,
        jito_enabled: bool,
        jito_auth_uuid: Option<String>,
        zero_slot_urls: Vec<String>,
    ) -> Self {
        // 追加 bundles.jito.wtf 作为额外 relay 端点（独立限频池）
        let mut urls = jito_block_engine_urls;
        let relay = "https://bundles.jito.wtf".to_string();
        if !urls.contains(&relay) {
            urls.push(relay);
        }
        let tx_urls = urls
            .iter()
            .filter(|url| !Self::is_bundles_only_endpoint(url))
            .cloned()
            .collect::<Vec<_>>();

        if jito_auth_uuid.is_some() {
            info!("Jito 认证已配置 (x-jito-auth UUID)");
        } else {
            warn!("Jito 未配置认证 UUID，rate limit 将非常低。设置 JITO_AUTH_UUID 环境变量");
        }

        if !zero_slot_urls.is_empty() {
            info!("0slot 质押加速已配置: {} 个端点", zero_slot_urls.len());
        }

        Self {
            primary_rpc_url,
            secondary_rpc_url,
            jito_block_engine_urls: urls,
            jito_tx_urls: tx_urls,
            jito_enabled,
            jito_auth_uuid,
            zero_slot_urls,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .pool_max_idle_per_host(4)
                .build()
                .unwrap(),
            jito_url_counter: AtomicUsize::new(0),
        }
    }

    /// 获取下一个 Jito endpoint URL（轮换，~1ns）
    fn next_jito_url(&self) -> &str {
        let idx = self.jito_url_counter.fetch_add(1, Ordering::Relaxed)
            % self.jito_block_engine_urls.len();
        &self.jito_block_engine_urls[idx]
    }

    /// 获取下一对 Jito endpoint URLs（两个不同 endpoint，用于 Bundle + TX 并发）
    fn next_jito_url_pair(&self) -> (&str, &str) {
        let len = self.jito_block_engine_urls.len();
        let idx = self.jito_url_counter.fetch_add(2, Ordering::Relaxed);
        let url1 = &self.jito_block_engine_urls[idx % len];
        let url2 = &self.jito_block_engine_urls[(idx + 1) % len];
        (url1, url2)
    }

    fn next_jito_tx_url(&self) -> Option<&str> {
        if self.jito_tx_urls.is_empty() {
            return None;
        }
        let idx = self.jito_url_counter.fetch_add(1, Ordering::Relaxed) % self.jito_tx_urls.len();
        Some(&self.jito_tx_urls[idx])
    }

    fn is_bundles_only_endpoint(url: &str) -> bool {
        url.to_ascii_lowercase().contains("bundles.jito.wtf")
    }

    /// 通过 HTTP JSON-RPC sendTransaction 发送原始交易（base64 编码）
    async fn send_rpc_raw(
        http_client: &reqwest::Client,
        rpc_url: &str,
        tx_base64: &str,
        skip_preflight: bool,
    ) -> Result<Signature> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": skip_preflight,
                    "maxRetries": 0
                }
            ]
        });

        let resp: serde_json::Value = http_client
            .post(rpc_url)
            .json(&request)
            .send()
            .await
            .context("RPC sendTransaction HTTP 请求失败")?
            .json()
            .await
            .context("RPC sendTransaction 响应解析失败")?;

        if let Some(error) = resp.get("error") {
            anyhow::bail!("RPC sendTransaction error: {}", error);
        }

        let sig_str = resp["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("RPC sendTransaction 无签名返回"))?;

        sig_str
            .parse::<Signature>()
            .map_err(|e| anyhow::anyhow!("签名解析失败: {}", e))
    }

    /// 🚀 同区块优化: fire-and-forget 发送
    /// 预序列化一次，所有通道 T+0 并发，立即返回不等待
    /// zero_slot_tx: 0slot 专用交易（含官方 fee），None 时 0slot 通道复用主交易
    pub fn fire_and_forget(
        &self,
        transaction: &VersionedTransaction,
        zero_slot_tx: Option<&VersionedTransaction>,
    ) -> Result<Signature> {
        self.fire_and_forget_with_mode(transaction, zero_slot_tx, true)
    }

    pub fn fire_and_forget_without_0slot(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<Signature> {
        self.fire_and_forget_with_mode(transaction, None, false)
    }

    fn fire_and_forget_with_mode(
        &self,
        transaction: &VersionedTransaction,
        zero_slot_tx: Option<&VersionedTransaction>,
        allow_zero_slot: bool,
    ) -> Result<Signature> {
        let start = Instant::now();
        let zero_slot_enabled = allow_zero_slot && !self.zero_slot_urls.is_empty();
        let zero_slot_only_mode = zero_slot_enabled;
        let wire_tx = zero_slot_tx.unwrap_or(transaction);

        // 预序列化交易（只做一次，VersionedTransaction 用 bincode 序列化）
        let tx_bytes = bincode::serialize(wire_tx)?;
        let tx_base64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);

        // 提取签名（本地操作）
        let signature = wire_tx.signatures.first().copied().unwrap_or_default();

        let mut channel_count = 0u32;

        // 通道 0slot: 质押加速（最高优先级，要求独立 fee 交易）
        if zero_slot_enabled {
            for zero_url in &self.zero_slot_urls {
                let http = self.http_client.clone();
                let url = zero_url.clone();
                let b64 = tx_base64.clone();
                tokio::spawn(async move {
                    match Self::send_rpc_raw(&http, &url, &b64, true).await {
                        Ok(sig) => info!("通道结果: 0slot ✅ | {}", sig),
                        Err(e) => warn!("通道结果: 0slot ❌ | {}", e),
                    }
                });
                channel_count += 1;
            }
        }

        if zero_slot_only_mode {
            info!("0slot only mode: skipping standard RPC channels");
        } else {
            // 通道 RPC1: 主 RPC (Shyft)
            {
                let http = self.http_client.clone();
                let url = self.primary_rpc_url.clone();
                let b64 = tx_base64.clone();
                tokio::spawn(async move {
                    match Self::send_rpc_raw(&http, &url, &b64, true).await {
                        Ok(sig) => info!("通道结果: Shyft ✅ | {}", sig),
                        Err(e) => warn!("通道结果: Shyft ❌ | {}", e),
                    }
                });
                channel_count += 1;
            }

            // 通道 2: 备用 RPC (Helius)
            if let Some(url2) = &self.secondary_rpc_url {
                let http = self.http_client.clone();
                let url2 = url2.clone();
                let b64 = tx_base64.clone();
                tokio::spawn(async move {
                    match Self::send_rpc_raw(&http, &url2, &b64, true).await {
                        Ok(sig) => info!("通道结果: Helius ✅ | {}", sig),
                        Err(e) => warn!("通道结果: Helius ❌ | {}", e),
                    }
                });
                channel_count += 1;
            }
        }

        // 通道 3: Jito TX — Jito Bundle 通道已禁用
        if zero_slot_enabled {
            info!("0slot only mode: skipping Jito channels to avoid duplicate execution");
        } else if self.jito_enabled && !self.jito_block_engine_urls.is_empty() {
            if let Some(tx_url) = self.next_jito_tx_url() {
                let http = self.http_client.clone();
                let jito_url2 = tx_url.to_string();
                let auth = self.jito_auth_uuid.clone();
                let b64 = tx_base64;
                tokio::spawn(async move {
                    match Self::send_jito_tx_raw(&http, &jito_url2, &b64, &auth).await {
                        Ok(()) => info!("通道结果: Jito TX ✅"),
                        Err(e) => warn!("通道结果: Jito TX ❌ | {}", e),
                    }
                });
                channel_count += 1;
            }
        }

        let elapsed = start.elapsed();
        info!(
            "Fire-and-forget: {} 通道已触发 | 耗时: {:?} | sig: {}",
            channel_count,
            elapsed,
            &signature.to_string()[..16],
        );

        Ok(signature)
    }

    /// 🚀 Jito Backrun Bundle: [目标tx, 我们的tx] 同区块执行
    /// 目标交易和我们的交易打包在同一个 bundle，Jito 保证连续执行
    pub fn fire_and_forget_backrun(
        &self,
        target_tx_bytes: &[u8],
        our_transaction: &VersionedTransaction,
        zero_slot_tx: Option<&VersionedTransaction>,
    ) -> Result<Signature> {
        let _ = target_tx_bytes;
        info!("Jito Bundle 通道已禁用，Backrun 回退普通发送");
        self.fire_and_forget(our_transaction, zero_slot_tx)
    }

    /// 原有的等待模式（卖出时使用，需要知道是否成功）
    pub async fn send_all_channels(
        &self,
        transaction: &VersionedTransaction,
        zero_slot_tx: Option<&VersionedTransaction>,
    ) -> Result<SendResult> {
        self.send_all_channels_with_opts(transaction, zero_slot_tx, true)
            .await
    }

    pub async fn send_all_channels_with_opts(
        &self,
        transaction: &VersionedTransaction,
        zero_slot_tx: Option<&VersionedTransaction>,
        skip_preflight: bool,
    ) -> Result<SendResult> {
        let start = Instant::now();
        let mut handles: Vec<tokio::task::JoinHandle<(&str, Result<Signature>)>> = Vec::new();
        let zero_slot_only_mode = !self.zero_slot_urls.is_empty();
        let wire_tx = zero_slot_tx.unwrap_or(transaction);

        // 预序列化一次，所有通道复用
        let tx_bytes = bincode::serialize(wire_tx)?;
        let tx_base64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);

        // T+0: 所有通道并发（全部使用 HTTP JSON-RPC）

        // 0slot 质押加速通道（最高优先级，需要带官方 fee 的独立交易）
        if !self.zero_slot_urls.is_empty() {
            for zero_url in &self.zero_slot_urls {
                let http = self.http_client.clone();
                let url = zero_url.clone();
                let b64 = tx_base64.clone();
                let sp = skip_preflight;
                handles.push(tokio::spawn(async move {
                    let result = Self::send_rpc_raw(&http, &url, &b64, sp).await;
                    ("0slot", result)
                }));
            }
        }

        if zero_slot_only_mode {
            info!("0slot only mode: skipping standard RPC channels");
        } else {
            {
                let http = self.http_client.clone();
                let url = self.primary_rpc_url.clone();
                let b64 = tx_base64.clone();
                handles.push(tokio::spawn(async move {
                    let result = Self::send_rpc_raw(&http, &url, &b64, skip_preflight).await;
                    ("Shyft RPC", result)
                }));
            }

            if let Some(url2) = &self.secondary_rpc_url {
                let http = self.http_client.clone();
                let url2 = url2.clone();
                let b64 = tx_base64.clone();
                handles.push(tokio::spawn(async move {
                    let result = Self::send_rpc_raw(&http, &url2, &b64, skip_preflight).await;
                    ("Helius RPC", result)
                }));
            }
        }

        if zero_slot_only_mode {
            info!("0slot only mode: skipping Jito channels to avoid duplicate execution");
        } else if self.jito_enabled && !self.jito_block_engine_urls.is_empty() {
            let auth = self.jito_auth_uuid.clone();

            if let Some(tx_url) = self.next_jito_tx_url() {
                let jito_http = self.http_client.clone();
                let jito_url2 = tx_url.to_string();
                let b64 = tx_base64;
                handles.push(tokio::spawn(async move {
                    match Self::send_jito_tx_raw(&jito_http, &jito_url2, &b64, &auth).await {
                        Ok(()) => ("Jito TX", Ok(Signature::default())),
                        Err(e) => ("Jito TX", Err(e)),
                    }
                }));
            }
        }

        let channel_count = handles.len();

        // 收集结果
        let mut first_signature: Option<Signature> = None;
        let mut success_channels = Vec::new();
        let mut fail_channels = Vec::new();

        for handle in handles {
            match handle.await {
                Ok((name, Ok(sig))) => {
                    if first_signature.is_none() && sig != Signature::default() {
                        first_signature = Some(sig);
                    }
                    success_channels.push(name);
                }
                Ok((name, Err(e))) => {
                    if name.starts_with("Jito") {
                        debug!("{} 发送失败: {}", name, e);
                    } else {
                        warn!("{} 发送失败: {}", name, e);
                    }
                    fail_channels.push(name);
                }
                Err(e) => {
                    error!("通道任务错误: {}", e);
                }
            }
        }

        let elapsed = start.elapsed();
        let success = !success_channels.is_empty();

        if success {
            info!(
                "发送完成: 成功=[{}] 失败=[{}] | 耗时={:?}",
                success_channels.join(", "),
                if fail_channels.is_empty() {
                    "无".to_string()
                } else {
                    fail_channels.join(", ")
                },
                elapsed,
            );
        } else {
            error!(
                "所有通道均失败: [{}] | 耗时={:?}",
                fail_channels.join(", "),
                elapsed,
            );
        }

        Ok(SendResult {
            signature: first_signature,
            success,
            elapsed,
            channels_sent: channel_count,
            channels_succeeded: success_channels.len(),
        })
    }

    // ============================================
    // Jito 发送（预序列化版本，零额外序列化开销）
    // ============================================

    /// 构建带认证的 Jito HTTP 请求
    fn jito_request(
        http_client: &reqwest::Client,
        url: &str,
        body: &serde_json::Value,
        auth_uuid: &Option<String>,
    ) -> reqwest::RequestBuilder {
        let mut req = http_client.post(url).json(body);
        if let Some(uuid) = auth_uuid {
            req = req.header("x-jito-auth", uuid);
        }
        req
    }

    /// Jito Backrun Bundle: [target_tx, our_tx] — 同区块连续执行
    /// 返回 bundle_id（用于查询状态）
    async fn send_jito_backrun_bundle(
        http_client: &reqwest::Client,
        block_engine_url: &str,
        target_tx_b58: &str,
        our_tx_b58: &str,
        auth_uuid: &Option<String>,
    ) -> Result<String> {
        let bundle_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [[target_tx_b58, our_tx_b58]]
        });

        let url = format!("{}/api/v1/bundles", block_engine_url);
        let resp = Self::jito_request(http_client, &url, &bundle_request, auth_uuid)
            .send()
            .await
            .context("Jito Backrun Bundle HTTP 请求失败")?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();

        if status.as_u16() == 429 {
            anyhow::bail!("Jito rate limited");
        }
        if !status.is_success() {
            let detail = body
                .get("error")
                .or_else(|| body.get("message"))
                .map(|v| v.to_string())
                .unwrap_or_else(|| body.to_string());
            anyhow::bail!("Jito Backrun error {}: {}", status, detail);
        }

        if let Some(error) = body.get("error") {
            let msg = error
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Jito Backrun error: {}", msg);
        }

        // 提取 bundle_id
        let bundle_id = body["result"].as_str().unwrap_or("unknown").to_string();

        Ok(bundle_id)
    }

    async fn send_jito_bundle_raw(
        http_client: &reqwest::Client,
        block_engine_url: &str,
        tx_b58: &str,
        auth_uuid: &Option<String>,
    ) -> Result<()> {
        let bundle_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [[tx_b58]]
        });

        let url = format!("{}/api/v1/bundles", block_engine_url);
        let resp = Self::jito_request(http_client, &url, &bundle_request, auth_uuid)
            .send()
            .await
            .context("Jito Bundle HTTP 请求失败")?;

        let status = resp.status();
        if status.as_u16() == 429 {
            anyhow::bail!("Jito rate limited");
        }
        if !status.is_success() {
            anyhow::bail!("Jito Bundle error: {}", status);
        }

        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        if let Some(error) = body.get("error") {
            let msg = error
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Jito Bundle error: {}", msg);
        }

        Ok(())
    }

    async fn send_jito_tx_raw(
        http_client: &reqwest::Client,
        block_engine_url: &str,
        tx_base64: &str,
        auth_uuid: &Option<String>,
    ) -> Result<()> {
        let tx_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        });

        let url = format!("{}/api/v1/transactions", block_engine_url);
        let resp = Self::jito_request(http_client, &url, &tx_request, auth_uuid)
            .send()
            .await
            .context("Jito TX HTTP 请求失败")?;

        let status = resp.status();
        if status.as_u16() == 429 {
            anyhow::bail!("Jito TX rate limited");
        }
        if !status.is_success() {
            anyhow::bail!("Jito TX error: {}", status);
        }

        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        if let Some(error) = body.get("error") {
            let msg = error
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Jito TX error: {}", msg);
        }

        Ok(())
    }

    /// 获取随机 Jito tip 账户
    pub fn random_jito_tip_account(&self) -> solana_sdk::pubkey::Pubkey {
        use std::str::FromStr;
        let tip_accounts = [
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4bVqkfRtQ7NmXwkiNPLNiGp",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSDBTg6EZ7NMckRBHYc",
            "DfXygSm4jCyNCzbzYYVKVXdKP8BYSqLVQNpLKfcku9T2",
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        ];
        let idx = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as usize)
            % tip_accounts.len();
        solana_sdk::pubkey::Pubkey::from_str(tip_accounts[idx]).unwrap()
    }

    /// 0slot 官方 fee 账户列表，来源: https://0slot.trade/docs.php
    pub fn random_0slot_tip_account(&self) -> solana_sdk::pubkey::Pubkey {
        use std::str::FromStr;
        let tip_accounts = [
            "6fQaVhYZA4w3MBSXjJ81Vf6W1EDYeUPXpgVQ6UQyU1Av",
            "4HiwLEP2Bzqj3hM2ENxJuzhcPCdsafwiet3oGkMkuQY4",
            "7toBU3inhmrARGngC7z6SjyP85HgGMmCTEwGNRAcYnEK",
            "8mR3wB1nh4D6J9RUCugxUpc6ya8w38LPxZ3ZjcBhgzws",
            "6SiVU5WEwqfFapRuYCndomztEwDjvS5xgtEof3PLEGm9",
            "TpdxgNJBWZRL8UXF5mrEsyWxDWx9HQexA9P1eTWQ42p",
            "D8f3WkQu6dCF33cZxuAsrKHrGsqGP2yvAHf8mX6RXnwf",
            "GQPFicsy3P3NXxB5piJohoxACqTvWE9fKpLgdsMduoHE",
            "Ey2JEr8hDkgN8qKJGrLf2yFjRhW7rab99HVxwi5rcvJE",
            "4iUgjMT8q2hNZnLuhpqZ1QtiV8deFPy2ajvvjEpKKgsS",
            "3Rz8uD83QsU8wKvZbgWAPvCNDU6Fy8TSZTMcPm3RB6zt",
        ];
        let idx = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as usize)
            % tip_accounts.len();
        solana_sdk::pubkey::Pubkey::from_str(tip_accounts[idx]).unwrap()
    }
}

#[derive(Debug)]
pub struct SendResult {
    pub signature: Option<Signature>,
    pub success: bool,
    pub elapsed: Duration,
    pub channels_sent: usize,
    pub channels_succeeded: usize,
}
