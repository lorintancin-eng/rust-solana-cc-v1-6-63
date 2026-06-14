use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::path::Path;
use tracing::{debug, error, info, warn};

use super::position::{Position, PositionKey, PositionState};

const POSITIONS_FILE: &str = "positions.json";

/// 可序列化的仓位数据（用于 JSON 持久化）
#[derive(Serialize, Deserialize, Debug)]
struct SavedPosition {
    group_id: String,
    group_name: String,
    token_mint: String,
    state: String,
    entry_sol_amount: u64,
    token_amount: u64,
    entry_price_sol: f64,
    highest_price: f64,
    current_price: f64,
    source_wallet: String,
    buy_signature: String,
    sell_attempts: u32,
    #[serde(default)]
    zero_balance_sell_skips: u32,
    /// 代币名称
    #[serde(default)]
    token_name: String,
    /// 入场市值 (SOL)
    #[serde(default)]
    entry_mcap_sol: f64,
}

/// 保存所有活跃仓位到 positions.json
pub fn save_positions(positions: &DashMap<PositionKey, Position>) {
    let saved: Vec<SavedPosition> = positions
        .iter()
        .filter(|p| {
            matches!(
                p.state,
                PositionState::Submitted
                    | PositionState::Confirming
                    | PositionState::Active
                    | PositionState::Selling
            )
        })
        .map(|p| SavedPosition {
            group_id: p.group.id.clone(),
            group_name: p.group.name.clone(),
            token_mint: p.token_mint.to_string(),
            state: p.state.to_string(),
            entry_sol_amount: p.entry_sol_amount,
            token_amount: p.token_amount,
            entry_price_sol: p.entry_price_sol,
            highest_price: p.highest_price,
            current_price: p.current_price,
            source_wallet: p.source_wallet.to_string(),
            buy_signature: p.buy_signature.clone(),
            sell_attempts: p.sell_attempts,
            zero_balance_sell_skips: p.zero_balance_sell_skips,
            token_name: p.token_name.clone(),
            entry_mcap_sol: p.entry_mcap_sol,
        })
        .collect();

    match serde_json::to_string_pretty(&saved) {
        Ok(json) => {
            if let Err(e) = std::fs::write(POSITIONS_FILE, &json) {
                error!("Failed to write {}: {}", POSITIONS_FILE, e);
            } else {
                debug!("Saved {} positions to {}", saved.len(), POSITIONS_FILE);
            }
        }
        Err(e) => error!("Failed to serialize positions: {}", e),
    }
}

/// 从 positions.json 加载仓位（启动时恢复）
pub fn load_positions() -> Vec<(Pubkey, u64, f64, Pubkey, String, u64)> {
    let path = Path::new(POSITIONS_FILE);
    if !path.exists() {
        return Vec::new();
    }

    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) => {
            warn!("Failed to read {}: {}", POSITIONS_FILE, e);
            return Vec::new();
        }
    };

    let saved: Vec<SavedPosition> = match serde_json::from_str(&data) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to parse {}: {}", POSITIONS_FILE, e);
            return Vec::new();
        }
    };

    let mut result = Vec::new();
    for sp in &saved {
        if let (Ok(mint), Ok(wallet)) = (
            sp.token_mint.parse::<Pubkey>(),
            sp.source_wallet.parse::<Pubkey>(),
        ) {
            result.push((
                mint,
                sp.entry_sol_amount,
                sp.entry_price_sol,
                wallet,
                sp.buy_signature.clone(),
                sp.token_amount,
            ));
        }
    }

    if !result.is_empty() {
        info!(
            "Loaded {} positions from {} for recovery",
            result.len(),
            POSITIONS_FILE,
        );
    }

    result
}