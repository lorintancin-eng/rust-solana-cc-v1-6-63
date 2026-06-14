use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::time::Duration;
use tracing::{debug, info};

/// Jupiter API 卖出（自动路由，兼容 PumpFun/PumpSwap/Raydium）
pub struct JupiterSeller {
    http_client: reqwest::Client,
}

// SOL mint 地址
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const JUPITER_QUOTE_URL: &str = "https://public.jupiterapi.com/quote";
const JUPITER_SWAP_URL: &str = "https://public.jupiterapi.com/swap";

impl JupiterSeller {
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .unwrap(),
        }
    }

    /// 通过 Jupiter API 卖出代币
    /// 返回签名后的 VersionedTransaction 字节（可直接发送）
    pub async fn build_sell_transaction(
        &self,
        mint: &Pubkey,
        token_amount: u64,
        slippage_bps: u64,
        keypair: &Keypair,
    ) -> Result<Vec<u8>> {
        let start = std::time::Instant::now();
        let mint_str = mint.to_string();
        let user_pubkey = keypair.pubkey().to_string();

        // [Step 1] 获取报价
        let t_quote = std::time::Instant::now();
        let quote_response = self.http_client
            .get(JUPITER_QUOTE_URL)
            .query(&[
                ("inputMint", mint_str.as_str()),
                ("outputMint", SOL_MINT),
                ("amount", &token_amount.to_string()),
                ("slippageBps", &slippage_bps.to_string()),
            ])
            .send()
            .await
            .context(format!("Jupiter quote 连接失败: {}", JUPITER_QUOTE_URL))?;

        let quote_status = quote_response.status();
        let quote_resp: serde_json::Value = quote_response
            .json()
            .await
            .context("Jupiter quote 响应解析失败")?;

        // 检查 HTTP 错误或 API 错误
        if !quote_status.is_success() {
            anyhow::bail!("Jupiter quote HTTP {}: {}", quote_status, quote_resp);
        }
        if let Some(error) = quote_resp.get("error") {
            anyhow::bail!("Jupiter quote 错误: {}", error);
        }
        if let Some(error) = quote_resp.get("errorCode") {
            anyhow::bail!("Jupiter quote 错误: {} - {}", error,
                quote_resp.get("error").unwrap_or(&serde_json::Value::Null));
        }

        let out_amount = quote_resp["outAmount"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        if out_amount == 0 {
            anyhow::bail!(
                "Jupiter 无路由: {} tokens → 0 SOL | mint: {} | 可能是PumpFun内盘未上Jupiter | resp: {}",
                token_amount, mint_str,
                serde_json::to_string(&quote_resp).unwrap_or_default(),
            );
        }

        info!(
            "Jupiter 报价: {} tokens → {:.6} SOL | {}ms",
            token_amount,
            out_amount as f64 / 1e9,
            t_quote.elapsed().as_millis(),
        );

        // [Step 2] 获取 swap 交易
        let t_swap = std::time::Instant::now();
        let swap_request = serde_json::json!({
            "quoteResponse": quote_resp,
            "userPublicKey": user_pubkey,
            "dynamicComputeUnitLimit": true,
            "prioritizationFeeLamports": "auto",
        });

        let swap_response = self.http_client
            .post(JUPITER_SWAP_URL)
            .json(&swap_request)
            .send()
            .await
            .context("Jupiter swap 请求失败")?;

        let swap_status = swap_response.status();
        let swap_resp: serde_json::Value = swap_response
            .json()
            .await
            .context("Jupiter swap 响应解析失败")?;

        if !swap_status.is_success() {
            anyhow::bail!("Jupiter swap HTTP {}: {}", swap_status, swap_resp);
        }
        if let Some(error) = swap_resp.get("error") {
            anyhow::bail!("Jupiter swap 错误: {}", error);
        }

        let swap_tx_base64 = swap_resp["swapTransaction"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Jupiter swap 无 swapTransaction 字段"))?;

        debug!("Jupiter swap 交易获取: {}ms", t_swap.elapsed().as_millis());

        // [Step 3] 反序列化、签名、重新序列化
        let tx_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            swap_tx_base64,
        ).context("Jupiter swap base64 解码失败")?;

        let mut versioned_tx: solana_sdk::transaction::VersionedTransaction =
            bincode::deserialize(&tx_bytes)
                .context("Jupiter swap 交易反序列化失败")?;

        // 签名：找到我们 pubkey 对应的签名位置，替换
        let user_pubkey = keypair.pubkey();
        let account_keys = versioned_tx.message.static_account_keys();
        if let Some(idx) = account_keys.iter().position(|k| k == &user_pubkey) {
            let blockhash = *versioned_tx.message.recent_blockhash();
            let signature = keypair.sign_message(
                &versioned_tx.message.serialize(),
            );
            if idx < versioned_tx.signatures.len() {
                versioned_tx.signatures[idx] = signature;
            }
        }

        let signed_bytes = bincode::serialize(&versioned_tx)
            .context("签名后交易序列化失败")?;

        info!(
            "Jupiter 卖出交易构建完成: {}ms (quote: {}ms + swap: {}ms) | {:.6} SOL",
            start.elapsed().as_millis(),
            t_quote.elapsed().as_millis(),
            t_swap.elapsed().as_millis(),
            out_amount as f64 / 1e9,
        );

        Ok(signed_bytes)
    }
}
