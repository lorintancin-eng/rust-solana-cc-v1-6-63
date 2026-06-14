use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use tracing::debug;

/// 查询用户持有某 token 的余额
pub fn get_token_balance(rpc: &RpcClient, owner: &Pubkey, mint: &Pubkey) -> Result<u64> {
    let ata = get_associated_token_address(owner, mint);
    match rpc.get_token_account_balance(&ata) {
        Ok(balance) => {
            let amount = balance.amount.parse::<u64>().unwrap_or(0);
            debug!("Token balance for {} (mint {}): {}", owner, mint, amount);
            Ok(amount)
        }
        Err(_) => {
            // ATA 不存在 = 余额为 0
            Ok(0)
        }
    }
}

/// 查询用户 SOL 余额（lamports）
pub fn get_sol_balance(rpc: &RpcClient, pubkey: &Pubkey) -> Result<u64> {
    let balance = rpc.get_balance(pubkey)?;
    Ok(balance)
}

/// 从 SPL Token account data 中提取 mint 地址
/// Token account layout: mint(32) + owner(32) + amount(8) + ...
pub fn extract_mint_from_token_account(data: &[u8]) -> Result<Pubkey> {
    if data.len() < 32 {
        anyhow::bail!("Token account data too short");
    }
    Ok(Pubkey::new_from_array(data[..32].try_into()?))
}

/// 从 SPL Token account data 中提取 amount
pub fn extract_amount_from_token_account(data: &[u8]) -> Result<u64> {
    if data.len() < 72 {
        anyhow::bail!("Token account data too short for amount");
    }
    Ok(u64::from_le_bytes(data[64..72].try_into()?))
}

/// 计算 PnL 百分比
pub fn calc_pnl_percent(entry_sol: f64, current_value_sol: f64) -> f64 {
    if entry_sol <= 0.0 {
        return 0.0;
    }
    ((current_value_sol - entry_sol) / entry_sol) * 100.0
}

/// lamports 转 SOL 显示
pub fn lamports_to_sol(lamports: u64) -> f64 {
    lamports as f64 / 1_000_000_000.0
}

/// SOL 转 lamports
pub fn sol_to_lamports(sol: f64) -> u64 {
    (sol * 1_000_000_000.0) as u64
}
