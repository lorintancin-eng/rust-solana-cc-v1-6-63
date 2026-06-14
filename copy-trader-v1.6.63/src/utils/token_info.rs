use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::debug;

use crate::processor::pumpfun::BondingCurveState;

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

/// 代币信息
#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub name: String,
    pub symbol: String,
    pub market_cap_sol: f64,
}

/// 从链上获取代币名称（Metaplex metadata）
pub async fn fetch_token_info(
    rpc_client: &Arc<RpcClient>,
    mint: &Pubkey,
) -> TokenInfo {
    // 尝试获取 Metaplex metadata
    let (name, symbol) = fetch_metadata_name(rpc_client, mint).await;

    // 从 bonding curve 计算市值
    let market_cap_sol = fetch_market_cap(rpc_client, mint).await;

    TokenInfo {
        name,
        symbol,
        market_cap_sol,
    }
}

/// 从 Metaplex metadata 获取名称
async fn fetch_metadata_name(rpc: &Arc<RpcClient>, mint: &Pubkey) -> (String, String) {
    // Metaplex metadata PDA: ["metadata", metaplex_program, mint]
    let metaplex_program =
        Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s").unwrap();
    let (metadata_pda, _) = Pubkey::find_program_address(
        &[b"metadata", metaplex_program.as_ref(), mint.as_ref()],
        &metaplex_program,
    );

    let rpc_clone = rpc.clone();
    let result = tokio::task::spawn_blocking(move || rpc_clone.get_account(&metadata_pda)).await;

    match result {
        Ok(Ok(account)) => {
            // Metaplex metadata layout:
            // 1 byte: key
            // 32 bytes: update_authority
            // 32 bytes: mint
            // 4 bytes: name length (little-endian u32)
            // N bytes: name (padded to 32 bytes)
            // 4 bytes: symbol length
            // N bytes: symbol (padded to 10 bytes)
            let data = &account.data;
            if data.len() > 69 {
                let name_start = 65; // 1 + 32 + 32
                let name = extract_string(data, name_start, 32);
                let symbol_start = name_start + 4 + 32; // 4 bytes len prefix + 32 bytes name
                let symbol = extract_string(data, symbol_start, 10);
                debug!("Token info: {} ({})", name, symbol);
                (name, symbol)
            } else {
                (short_mint(mint), String::new())
            }
        }
        _ => (short_mint(mint), String::new()),
    }
}

/// 从 bonding curve 计算市值（SOL）
async fn fetch_market_cap(rpc: &Arc<RpcClient>, mint: &Pubkey) -> f64 {
    let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
    let (bonding_curve, _) =
        Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id);

    let rpc_clone = rpc.clone();
    let bc = bonding_curve;
    let result =
        tokio::task::spawn_blocking(move || rpc_clone.get_account(&bc)).await;

    match result {
        Ok(Ok(account)) => {
            if let Ok(state) = BondingCurveState::from_account_data(&account.data) {
                // 市值 ≈ real_sol_reserves (已投入的 SOL)
                state.real_sol_reserves as f64 / 1e9
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

/// 从 Metaplex 数据中提取字符串
fn extract_string(data: &[u8], offset: usize, max_len: usize) -> String {
    if offset + 4 > data.len() {
        return String::new();
    }
    let len = u32::from_le_bytes(
        data[offset..offset + 4].try_into().unwrap_or([0; 4]),
    ) as usize;
    let actual_len = len.min(max_len).min(data.len() - offset - 4);
    let start = offset + 4;
    let end = start + actual_len;
    if end > data.len() {
        return String::new();
    }
    String::from_utf8_lossy(&data[start..end])
        .trim_end_matches('\0')
        .trim()
        .to_string()
}

fn short_mint(mint: &Pubkey) -> String {
    let s = mint.to_string();
    if s.len() > 8 {
        format!("{}..{}", &s[..4], &s[s.len() - 4..])
    } else {
        s
    }
}
