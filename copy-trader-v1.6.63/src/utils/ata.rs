use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use std::str::FromStr;

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// 获取 WSOL ATA
pub fn get_wsol_ata(owner: &Pubkey) -> Pubkey {
    let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
    get_associated_token_address(owner, &wsol)
}

/// 批量导出给定 owner 的所有 token ATA
pub fn derive_atas(owner: &Pubkey, mints: &[Pubkey]) -> Vec<(Pubkey, Pubkey)> {
    mints
        .iter()
        .map(|mint| (*mint, get_associated_token_address(owner, mint)))
        .collect()
}

/// 判断是否是系统地址（用于过滤早期买家列表）
pub fn is_system_address(addr: &Pubkey) -> bool {
    let known_system = [
        "11111111111111111111111111111111",
        TOKEN_PROGRAM,
        WSOL_MINT,
        "SysvarRent111111111111111111111111111111111",
        "SysvarC1ock11111111111111111111111111111111",
        "ComputeBudget111111111111111111111111111111",
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    ];

    known_system
        .iter()
        .any(|s| Pubkey::from_str(s).map(|p| p == *addr).unwrap_or(false))
}
