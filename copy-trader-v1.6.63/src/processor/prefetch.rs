use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug;

use crate::config::AppConfig;
use crate::grpc::BondingCurveCache;

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// 预取的代币数据
#[derive(Debug, Clone)]
pub struct PrefetchedToken {
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub associated_bonding_curve: Pubkey,
    pub user_ata: Pubkey,
    pub token_program: Pubkey,
    /// 目标钱包交易中的完整账户列表（用于镜像构建指令）
    pub mirror_accounts: Vec<Pubkey>,
    /// 目标钱包地址（用于检测并替换用户特定 PDA）
    pub source_wallet: Pubkey,
    pub source_signature: String,
    pub quality_score: u32,
    pub created_at: Instant,
}

pub struct PrefetchCache {
    cache: Arc<DashMap<Pubkey, PrefetchedToken>>,
    bc_cache: BondingCurveCache,
}

impl PrefetchCache {
    pub fn new(bc_cache: BondingCurveCache) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            bc_cache,
        }
    }

    /// 预取代币数据
    /// mirror_accounts: 目标钱包交易的完整 instruction_accounts（用于镜像）
    pub fn prefetch_token(
        &self,
        mint: &Pubkey,
        token_program: &Pubkey,
        mirror_accounts: &[Pubkey],
        source_wallet: &Pubkey,
        source_signature: &str,
        quality_score: u32,
        force_replace: bool,
        config: &AppConfig,
    ) -> PrefetchedToken {
        if let Some(existing) = self.cache.get(mint) {
            if !force_replace && existing.quality_score > quality_score {
                return existing.clone();
            }
        }

        let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();

        let (bonding_curve, _) =
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program_id);
        let associated_bonding_curve =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &bonding_curve,
                mint,
                token_program,
            );
        let user_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &config.pubkey,
            mint,
            token_program,
        );

        let prefetched = PrefetchedToken {
            mint: *mint,
            bonding_curve,
            associated_bonding_curve,
            user_ata,
            token_program: *token_program,
            mirror_accounts: mirror_accounts.to_vec(),
            source_wallet: *source_wallet,
            source_signature: source_signature.to_string(),
            quality_score,
            created_at: Instant::now(),
        };

        debug!(
            "Prefetched: mint={}.. bc={}.. tp={} accounts={} score={}",
            &mint.to_string()[..12],
            &bonding_curve.to_string()[..12],
            &token_program.to_string()[..12],
            mirror_accounts.len(),
            quality_score,
        );

        self.cache.insert(*mint, prefetched.clone());
        prefetched
    }

    pub fn get(&self, mint: &Pubkey) -> Option<PrefetchedToken> {
        self.cache.get(mint).map(|v| v.clone())
    }

    pub async fn get_or_wait(&self, mint: &Pubkey, max_wait: Duration) -> Option<PrefetchedToken> {
        if let Some(prefetched) = self.get(mint) {
            return Some(prefetched);
        }

        let started = Instant::now();
        while started.elapsed() < max_wait {
            tokio::task::yield_now().await;
            if let Some(prefetched) = self.get(mint) {
                return Some(prefetched);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        None
    }

    pub fn remove(&self, mint: &Pubkey) {
        self.cache.remove(mint);
    }

    pub fn cleanup(&self, max_age_secs: u64) {
        let before = self.cache.len();
        self.cache
            .retain(|_, v| v.created_at.elapsed().as_secs() < max_age_secs);
        let removed = before - self.cache.len();
        if removed > 0 {
            debug!("Prefetch cleanup: removed {} expired entries", removed);
        }
    }
}
