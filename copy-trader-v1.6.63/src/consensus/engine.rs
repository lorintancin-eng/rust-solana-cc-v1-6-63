use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::processor::TradeOrigin;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConsensusKey {
    group_id: String,
    token_mint: Pubkey,
}

#[derive(Debug, Clone)]
pub struct BuySignal {
    pub group_id: String,
    pub group_name: String,
    pub token_mint: Pubkey,
    pub wallet: Pubkey,
    pub token_program: Pubkey,
    pub detected_at: Instant,
    pub signature: String,
    pub consensus_min_wallets: usize,
    pub consensus_timeout_secs: u64,
    pub instruction_data: Vec<u8>,
    pub instruction_accounts: Vec<Pubkey>,
    pub sol_amount_lamports: u64,
    pub is_pre_execution: bool,
    pub trade_origin: TradeOrigin,
}

#[derive(Debug, Clone)]
pub struct ConsensusTrigger {
    pub group_id: String,
    pub group_name: String,
    pub token_mint: Pubkey,
    pub wallets: Vec<Pubkey>,
    pub first_signature: String,
    pub canonical_signature: String,
    pub canonical_wallet: Pubkey,
    pub canonical_token_program: Pubkey,
    pub canonical_instruction_data: Vec<u8>,
    pub canonical_instruction_accounts: Vec<Pubkey>,
    pub canonical_trade_origin: TradeOrigin,
    pub triggered_at: Instant,
}

#[derive(Debug, Clone)]
struct TokenSignals {
    signals: Vec<BuySignal>,
    triggered: bool,
    min_wallets: usize,
    timeout: Duration,
}

pub struct ConsensusEngine {
    signals: Arc<DashMap<ConsensusKey, TokenSignals>>,
}

impl ConsensusEngine {
    pub fn new() -> Self {
        info!("Consensus engine initialized in group mode");
        Self {
            signals: Arc::new(DashMap::new()),
        }
    }

    pub fn submit_signal(
        &self,
        signal: BuySignal,
        trigger_tx: &mpsc::UnboundedSender<ConsensusTrigger>,
    ) {
        let key = ConsensusKey {
            group_id: signal.group_id.clone(),
            token_mint: signal.token_mint,
        };
        let now = Instant::now();

        let mut entry = self
            .signals
            .entry(key.clone())
            .or_insert_with(|| TokenSignals {
                signals: Vec::new(),
                triggered: false,
                min_wallets: signal.consensus_min_wallets,
                timeout: Duration::from_secs(signal.consensus_timeout_secs.max(1)),
            });

        let entry_was_triggered = entry.triggered;

        let timeout = entry.timeout;
        entry
            .signals
            .retain(|candidate| now.duration_since(candidate.detected_at) < timeout);

        if let Some(existing) = entry
            .signals
            .iter_mut()
            .find(|candidate| candidate.wallet == signal.wallet)
        {
            if should_replace_signal(existing, &signal) {
                *existing = signal.clone();
                if entry_was_triggered {
                    debug!(
                        "Update triggered consensus signal: [{}] {}.. -> {}",
                        signal.group_name,
                        &signal.wallet.to_string()[..8],
                        &signal.token_mint.to_string()[..12],
                    );
                } else {
                    debug!(
                        "Upgrade consensus wallet signal: [{}] {}.. -> {}",
                        signal.group_name,
                        &signal.wallet.to_string()[..8],
                        &signal.token_mint.to_string()[..12],
                    );
                }
            } else {
                debug!(
                    "Skip duplicate consensus wallet: [{}] {}.. -> {}",
                    signal.group_name,
                    &signal.wallet.to_string()[..8],
                    &signal.token_mint.to_string()[..12],
                );
                return;
            }
        } else {
            if entry_was_triggered {
                debug!(
                    "Skip new wallet signal for triggered consensus: [{}] {}",
                    signal.group_name,
                    &signal.token_mint.to_string()[..12],
                );
                return;
            }
            entry.signals.push(signal.clone());
        }
        let tally = tally_votes(&entry.signals, entry.min_wallets);

        if entry_was_triggered {
            return;
        }

        info!(
            "Consensus signal: [{}] {} | candidates={}/{} | raw={}",
            signal.group_name,
            &signal.token_mint.to_string()[..12],
            tally.candidate_wallets.len(),
            entry.min_wallets,
            entry.signals.len(),
        );

        if tally.should_trigger() {
            let Some(canonical_signal) = tally
                .effective_signals
                .iter()
                .max_by_key(|candidate| candidate.canonical_score())
                .map(|candidate| (*candidate).clone())
            else {
                return;
            };
            let first_signature = tally
                .effective_signals
                .iter()
                .min_by_key(|candidate| candidate.detected_at)
                .map(|candidate| candidate.signature.clone())
                .unwrap_or_default();
            let effective_wallets = tally.effective_wallets.clone();
            let canonical_signature = canonical_signal.signature.clone();
            let canonical_wallet = canonical_signal.wallet;
            let canonical_token_program = canonical_signal.token_program;
            let canonical_instruction_data = canonical_signal.instruction_data.clone();
            let canonical_instruction_accounts = canonical_signal.instruction_accounts.clone();
            let canonical_trade_origin = canonical_signal.trade_origin;

            drop(tally);

            entry.triggered = true;
            let trigger = ConsensusTrigger {
                group_id: signal.group_id,
                group_name: signal.group_name,
                token_mint: signal.token_mint,
                wallets: effective_wallets,
                first_signature,
                canonical_signature,
                canonical_wallet,
                canonical_token_program,
                canonical_instruction_data,
                canonical_instruction_accounts,
                canonical_trade_origin,
                triggered_at: Instant::now(),
            };

            info!(
                "Consensus reached: [{}] {}",
                trigger.group_name,
                &trigger.token_mint.to_string()[..12],
            );

            if trigger_tx.send(trigger).is_err() {
                warn!("Consensus trigger channel closed");
            }
        }
    }

    pub fn revoke_signal(&self, group_id: &str, mint: &Pubkey, wallet: &Pubkey) -> bool {
        let key = ConsensusKey {
            group_id: group_id.to_string(),
            token_mint: *mint,
        };

        if let Some(mut entry) = self.signals.get_mut(&key) {
            if entry.triggered {
                return false;
            }

            let before = entry.signals.len();
            entry
                .signals
                .retain(|candidate| candidate.wallet != *wallet);
            return before != entry.signals.len();
        }

        false
    }

    pub fn reject_signal(
        &self,
        group_id: &str,
        mint: &Pubkey,
        wallet: &Pubkey,
        signature: &str,
    ) -> bool {
        let key = ConsensusKey {
            group_id: group_id.to_string(),
            token_mint: *mint,
        };

        if let Some(mut entry) = self.signals.get_mut(&key) {
            let before = entry.signals.len();
            entry.signals.retain(|candidate| {
                !(candidate.wallet == *wallet && candidate.signature == signature)
            });
            let after = entry.signals.len();
            let was_triggered = entry.triggered;
            let tally = tally_votes(&entry.signals, entry.min_wallets);
            let should_still_trigger = tally.should_trigger();
            drop(tally);
            entry.triggered = should_still_trigger;
            if was_triggered && !entry.triggered {
                info!(
                    "Consensus retracted: [{}] {} | wallet={}..{} | sig: {}..{}",
                    group_id,
                    &mint.to_string()[..12],
                    &wallet.to_string()[..4],
                    &wallet.to_string()[wallet.to_string().len() - 4..],
                    &signature[..8.min(signature.len())],
                    &signature[signature.len().saturating_sub(4)..],
                );
            }
            return before != after;
        }

        false
    }

    pub fn start_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let signals = self.signals.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let now = Instant::now();
                signals.retain(|_, entry| {
                    entry
                        .signals
                        .retain(|signal| now.duration_since(signal.detected_at) < entry.timeout);
                    if entry.signals.is_empty() {
                        return false;
                    }
                    if entry.triggered {
                        let expired = entry.signals.iter().all(|signal| {
                            now.duration_since(signal.detected_at) > entry.timeout * 2
                        });
                        return !expired;
                    }
                    true
                });
            }
        })
    }

    pub fn pending_count(&self) -> usize {
        self.signals.iter().filter(|entry| !entry.triggered).count()
    }
}

impl BuySignal {
    pub fn has_target_instruction(&self) -> bool {
        self.instruction_data.len() >= 24
    }

    pub fn counts_for_candidate_consensus(&self) -> bool {
        self.has_target_instruction()
            || self.sol_amount_lamports > 0
            || self.trade_origin.is_wrapper_cpi()
    }

    fn canonical_score(&self) -> u32 {
        let mut score = 0u32;
        if self.has_target_instruction() {
            score += 8;
        }
        if !self.instruction_accounts.is_empty() {
            score += 4;
        }
        if self.sol_amount_lamports > 0 {
            score += 2;
        }
        if !self.is_pre_execution {
            score += 1;
        }
        score
    }
}

fn should_replace_signal(existing: &BuySignal, incoming: &BuySignal) -> bool {
    if incoming.signature == existing.signature {
        let incoming_score = incoming.canonical_score();
        let existing_score = existing.canonical_score();
        return incoming_score > existing_score
            || (incoming_score == existing_score
                && !incoming.is_pre_execution
                && existing.is_pre_execution);
    }

    let existing_score = existing.canonical_score();
    let incoming_score = incoming.canonical_score();
    incoming_score > existing_score
        || (incoming_score == existing_score
            && !incoming.is_pre_execution
            && existing.is_pre_execution)
}

struct VoteTally<'a> {
    effective_signals: Vec<&'a BuySignal>,
    candidate_wallets: Vec<Pubkey>,
    effective_wallets: Vec<Pubkey>,
    min_wallets: usize,
}

impl<'a> VoteTally<'a> {
    fn should_trigger(&self) -> bool {
        self.candidate_wallets.len() >= self.min_wallets
    }
}

fn tally_votes<'a>(signals: &'a [BuySignal], min_wallets: usize) -> VoteTally<'a> {
    let effective_signals: Vec<&BuySignal> = signals
        .iter()
        .filter(|candidate| candidate.counts_for_candidate_consensus())
        .collect();
    let effective_wallets: Vec<Pubkey> = effective_signals
        .iter()
        .map(|candidate| candidate.wallet)
        .collect();
    let candidate_wallets: Vec<Pubkey> = effective_signals
        .iter()
        .map(|candidate| candidate.wallet)
        .collect();

    VoteTally {
        effective_signals,
        candidate_wallets,
        effective_wallets,
        min_wallets,
    }
}
