use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::autosell::{
    AutoSellManager, Position, PositionKey, SellAccountSnapshot, SellReason, SellSignal,
};
use crate::config::AppConfig;
use crate::grpc::{AccountSubscriber, AtaBalanceCache, BondingCurveCache};
use crate::processor::prefetch::PrefetchCache;
use crate::processor::pumpfun::PumpfunProcessor;
use crate::telegram::{TgEvent, TgNotifier};
use crate::tx::blockhash::BlockhashCache;
use crate::tx::builder::TxBuilder;
use crate::tx::jupiter::JupiterSeller;
use crate::tx::sender::TxSender;

const MAX_SELL_RETRIES: u32 = 3;
const MAX_ZERO_BALANCE_SKIPS: u32 = 5;
const MAX_AUTO_SELL_SIGNAL_ATTEMPTS: u32 = 5;
const FAST_FIRST_CONFIRM_MS: u64 = 1_500;
const RETRY_CONFIRM_MS: u64 = 2_500;
const DEFAULT_CONFIRM_MS: u64 = 3_000;

#[derive(Debug, Clone, Copy, Default)]
struct SellPathTimings {
    signal_queue: Duration,
    snapshot_load: Duration,
    bc_lookup: Duration,
    quote_build: Duration,
    build: Duration,
    send_call: Duration,
    total: Duration,
}

#[derive(Debug, Clone)]
struct SellConfirmTrace {
    confirmed: bool,
    source: &'static str,
    signature_seen: Option<Duration>,
    rpc_ata_target: Option<Duration>,
    cache_ata_target: Option<Duration>,
    total: Duration,
}

fn format_latency(duration: Duration) -> String {
    if duration.as_millis() > 0 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{}us", duration.as_micros())
    }
}

fn render_optional_latency(duration: Option<Duration>) -> String {
    duration
        .map(format_latency)
        .unwrap_or_else(|| "-".to_string())
}

pub struct SellExecutor {
    config: AppConfig,
    rpc_client: Arc<RpcClient>,
    pumpfun: Arc<PumpfunProcessor>,
    tx_sender: Arc<TxSender>,
    blockhash_cache: BlockhashCache,
    auto_sell: Arc<AutoSellManager>,
    bc_cache: BondingCurveCache,
    ata_cache: AtaBalanceCache,
    prefetch_cache: Arc<PrefetchCache>,
    account_subscriber: Arc<AccountSubscriber>,
    jupiter: JupiterSeller,
    tg: TgNotifier,
}

impl SellExecutor {
    pub fn new(
        config: AppConfig,
        rpc_client: Arc<RpcClient>,
        pumpfun: Arc<PumpfunProcessor>,
        tx_sender: Arc<TxSender>,
        blockhash_cache: BlockhashCache,
        auto_sell: Arc<AutoSellManager>,
        bc_cache: BondingCurveCache,
        ata_cache: AtaBalanceCache,
        prefetch_cache: Arc<PrefetchCache>,
        account_subscriber: Arc<AccountSubscriber>,
        tg: TgNotifier,
    ) -> Self {
        Self {
            config,
            rpc_client,
            pumpfun,
            tx_sender,
            blockhash_cache,
            auto_sell,
            bc_cache,
            ata_cache,
            prefetch_cache,
            account_subscriber,
            jupiter: JupiterSeller::new(),
            tg,
        }
    }

    fn confirm_timeout_ms(reason: SellReason, attempt: u32) -> u64 {
        match reason {
            SellReason::FollowSell if attempt == 1 => FAST_FIRST_CONFIRM_MS,
            SellReason::FollowSell => RETRY_CONFIRM_MS,
            _ => DEFAULT_CONFIRM_MS,
        }
    }

    async fn resolve_sell_snapshot(&self, position: &Position) -> Result<SellAccountSnapshot> {
        if let Some(snapshot) = position.sell_snapshot.clone() {
            return Ok(snapshot);
        }

        self.prefetch_cache
            .get(&position.token_mint)
            .map(|prefetched| SellAccountSnapshot {
                bonding_curve: prefetched.bonding_curve,
                associated_bonding_curve: prefetched.associated_bonding_curve,
                user_ata: prefetched.user_ata,
                token_program: prefetched.token_program,
                mirror_accounts: prefetched.mirror_accounts,
                source_wallet: prefetched.source_wallet,
            })
            .ok_or_else(|| anyhow::anyhow!("no sell snapshot"))
    }

    fn get_token_balance_rpc(&self, user_ata: &Pubkey) -> u64 {
        self.rpc_client
            .get_token_account_balance(user_ata)
            .map(|value| value.amount.parse::<u64>().unwrap_or(0))
            .unwrap_or(0)
    }

    fn cleanup_mint_if_unused(&self, mint: &Pubkey) {
        if self.auto_sell.open_position_count_for_mint(mint) == 0 {
            self.ata_cache.remove(mint);
            self.account_subscriber.untrack_mint(mint);
            self.prefetch_cache.remove(mint);
        }
    }

    async fn try_pumpfun_sell(
        &self,
        position: &Position,
        token_amount: u64,
        signal_received_at: Instant,
    ) -> Result<(String, SellPathTimings)> {
        let sell_start = Instant::now();
        let mut timings = SellPathTimings {
            signal_queue: sell_start.saturating_duration_since(signal_received_at),
            snapshot_load: Duration::default(),
            bc_lookup: Duration::default(),
            quote_build: Duration::default(),
            build: Duration::default(),
            send_call: Duration::default(),
            total: Duration::default(),
        };

        let group_config = position.group.to_app_config(&self.config);

        let snapshot_load_start = Instant::now();
        let snapshot = self.resolve_sell_snapshot(position).await?;
        timings.snapshot_load = snapshot_load_start.elapsed();

        let quote_build_start = Instant::now();
        let mirror = if snapshot.mirror_accounts.is_empty() {
            self.pumpfun
                .sell_standard(&position.token_mint, token_amount, &group_config, None)
                .await?
        } else {
            let bc_lookup_start = Instant::now();
            let bc_state = if let Some(state) = self.bc_cache.get(&position.token_mint) {
                state
            } else {
                self.pumpfun
                    .prefetch_bonding_curve(&snapshot.bonding_curve)
                    .await?
            };
            timings.bc_lookup = bc_lookup_start.elapsed();

            let expected_sol = bc_state.token_to_sol_quote(token_amount);
            let min_sol_output = expected_sol
                .saturating_sub(expected_sol * position.group.sell_slippage_bps / 10_000);
            let creator = bc_state
                .creator
                .ok_or_else(|| anyhow::anyhow!("BC state missing creator"))?;
            let sell_ix = self.pumpfun.build_sell_instruction_from_mirror(
                &self.config.pubkey,
                &snapshot.user_ata,
                &snapshot.mirror_accounts,
                token_amount,
                min_sol_output,
                &snapshot.token_program,
                &creator,
                bc_state.is_cashback,
            );

            crate::processor::MirrorInstruction {
                swap_instructions: vec![sell_ix],
                pre_instructions: vec![],
                post_instructions: vec![],
                token_mint: position.token_mint,
                sol_amount: expected_sol,
            }
        };
        timings.quote_build = quote_build_start.elapsed();

        let (blockhash, _) = self.blockhash_cache.get_sync();
        let tx_build_start = Instant::now();
        let transaction = if group_config.jito_enabled {
            let tip = self.tx_sender.random_jito_tip_account();
            TxBuilder::build_jito_bundle_transaction(
                &mirror,
                &group_config,
                &group_config.keypair,
                blockhash,
                &tip,
                position.group.tip_sell_lamports,
                &[],
            )?
        } else {
            TxBuilder::build_transaction(
                &mirror,
                &group_config,
                &group_config.keypair,
                blockhash,
                &[],
            )?
        };
        timings.build = tx_build_start.elapsed();

        let send_call_start = Instant::now();
        let sig = self.tx_sender.fire_and_forget_without_0slot(&transaction)?;
        timings.send_call = send_call_start.elapsed();
        timings.total = sell_start.elapsed();
        Ok((sig.to_string(), timings))
    }

    async fn try_jupiter_sell(&self, position: &Position, token_amount: u64) -> Result<String> {
        let signed_tx_bytes = self
            .jupiter
            .build_sell_transaction(
                &position.token_mint,
                token_amount,
                position.group.sell_slippage_bps,
                &self.config.keypair,
            )
            .await?;
        let tx = bincode::deserialize(&signed_tx_bytes)?;
        let sig = self.tx_sender.fire_and_forget_without_0slot(&tx)?;
        Ok(sig.to_string())
    }

    async fn try_jupiter_sell_with_confirm(
        &self,
        position: &Position,
        token_amount: u64,
        confirm_timeout_ms: u64,
        expected_after_balance: u64,
    ) -> Result<(String, SellConfirmTrace)> {
        let sig = self.try_jupiter_sell(position, token_amount).await?;
        let confirm_trace = self
            .wait_sell_confirm(position, &sig, confirm_timeout_ms, expected_after_balance)
            .await;
        Ok((sig, confirm_trace))
    }

    async fn should_route_sell_to_jupiter(
        &self,
        group_name: &str,
        token_mint: &Pubkey,
        snapshot: &SellAccountSnapshot,
    ) -> bool {
        match self
            .pumpfun
            .is_bonding_curve_migrated(&snapshot.bonding_curve)
            .await
        {
            Ok(true) => {
                info!(
                    "Sell routing: [{}] {} migrated to external pool, using Jupiter",
                    group_name,
                    &token_mint.to_string()[..12],
                );
                true
            }
            Ok(false) => false,
            Err(err) => {
                warn!(
                    "Sell routing check failed [{}] {}: {}",
                    group_name,
                    &token_mint.to_string()[..12],
                    err,
                );
                false
            }
        }
    }

    async fn wait_sell_confirm(
        &self,
        position: &Position,
        sig_str: &str,
        max_wait_ms: u64,
        expected_after_balance: u64,
    ) -> SellConfirmTrace {
        use solana_sdk::signature::Signature;

        let Ok(sig) = sig_str.parse::<Signature>() else {
            return SellConfirmTrace {
                confirmed: false,
                source: "invalid_signature",
                signature_seen: None,
                rpc_ata_target: None,
                cache_ata_target: None,
                total: Duration::default(),
            };
        };

        let start = Instant::now();
        let max_wait = Duration::from_millis(max_wait_ms);
        let user_ata = position
            .sell_snapshot
            .as_ref()
            .map(|snapshot| snapshot.user_ata)
            .unwrap_or_else(|| {
                get_associated_token_address(&self.config.pubkey, &position.token_mint)
            });
        let mut signature_seen = None;
        let mut rpc_ata_target = None;
        let mut cache_ata_target = None;

        while start.elapsed() < max_wait {
            if let Some(balance) = self.ata_cache.get(&position.token_mint) {
                if balance <= expected_after_balance {
                    cache_ata_target.get_or_insert_with(|| start.elapsed());
                    return SellConfirmTrace {
                        confirmed: true,
                        source: "cache_ata_target",
                        signature_seen,
                        rpc_ata_target,
                        cache_ata_target,
                        total: start.elapsed(),
                    };
                }
            }

            tokio::time::sleep(Duration::from_millis(80)).await;

            if signature_seen.is_none() {
                let rpc = self.rpc_client.clone();
                let status =
                    tokio::task::spawn_blocking(move || rpc.get_signature_statuses(&[sig]))
                        .await
                        .ok()
                        .and_then(|result| result.ok());
                if let Some(statuses) = status {
                    if let Some(Some(entry)) = statuses.value.first() {
                        if entry.err.is_some() {
                            return SellConfirmTrace {
                                confirmed: false,
                                source: "signature_error",
                                signature_seen,
                                rpc_ata_target,
                                cache_ata_target,
                                total: start.elapsed(),
                            };
                        }
                        signature_seen = Some(start.elapsed());
                    }
                }
            }

            let rpc = self.rpc_client.clone();
            let ata = user_ata;
            let rpc_balance = tokio::task::spawn_blocking(move || {
                rpc.get_token_account_balance(&ata)
                    .map(|value| value.amount.parse::<u64>().unwrap_or(0))
                    .unwrap_or(0)
            })
            .await
            .unwrap_or(u64::MAX);

            if rpc_balance <= expected_after_balance {
                rpc_ata_target.get_or_insert_with(|| start.elapsed());
                return SellConfirmTrace {
                    confirmed: true,
                    source: "rpc_ata_target",
                    signature_seen,
                    rpc_ata_target,
                    cache_ata_target,
                    total: start.elapsed(),
                };
            }

            if signature_seen.is_some() && expected_after_balance == rpc_balance {
                return SellConfirmTrace {
                    confirmed: true,
                    source: "signature_seen",
                    signature_seen,
                    rpc_ata_target,
                    cache_ata_target,
                    total: start.elapsed(),
                };
            }
        }

        SellConfirmTrace {
            confirmed: false,
            source: "timeout",
            signature_seen,
            rpc_ata_target,
            cache_ata_target,
            total: start.elapsed(),
        }
    }

    pub async fn handle_sell_signal(&self, signal: SellSignal) {
        let Some(position_before_sell) = self.auto_sell.get_position(&signal.position_key) else {
            return;
        };
        let previous_state = position_before_sell.state;
        let aggressive_follow = position_before_sell.group.follow_sell_mode()
            && signal.reason == SellReason::FollowSell;

        let snapshot = match self.resolve_sell_snapshot(&position_before_sell).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!("Sell snapshot missing [{}]: {}", signal.group_name, err);
                return;
            }
        };

        let current_ata_balance = self
            .ata_cache
            .get(&position_before_sell.token_mint)
            .unwrap_or_else(|| self.get_token_balance_rpc(&snapshot.user_ata));
        let token_amount = if position_before_sell.token_amount > 0 {
            position_before_sell.token_amount.min(current_ata_balance)
        } else {
            current_ata_balance
        };

        if token_amount == 0 {
            let zero_balance_skips = self
                .auto_sell
                .record_zero_balance_sell_skip(&signal.position_key)
                .unwrap_or(1);
            warn!(
                "Skip sell [{}] {}: zero balance ({}/{})",
                signal.group_name,
                &position_before_sell.token_mint.to_string()[..12],
                zero_balance_skips,
                MAX_ZERO_BALANCE_SKIPS,
            );
            if zero_balance_skips >= MAX_ZERO_BALANCE_SKIPS {
                self.auto_sell.confirm_failed(
                    &signal.position_key,
                    "sell skipped repeatedly: zero balance",
                );
            }
            return;
        }

        if !self.auto_sell.mark_selling(&signal.position_key) {
            return;
        }

        let expected_after_balance = current_ata_balance.saturating_sub(token_amount);
        let mut success = false;
        let mut last_sig = String::new();
        let mut saw_signature_error = false;
        let mut failure_reason = "sell retries exhausted".to_string();
        let route_via_jupiter = self
            .should_route_sell_to_jupiter(
                &signal.group_name,
                &position_before_sell.token_mint,
                &snapshot,
            )
            .await;

        for attempt in 1..=MAX_SELL_RETRIES {
            let confirm_timeout_ms = Self::confirm_timeout_ms(signal.reason, attempt);
            let send_started_at = Instant::now();

            if route_via_jupiter {
                match self
                    .try_jupiter_sell_with_confirm(
                        &position_before_sell,
                        token_amount,
                        confirm_timeout_ms,
                        expected_after_balance,
                    )
                    .await
                {
                    Ok((sig, confirm_trace)) => {
                        if confirm_trace.confirmed {
                            info!(
                                "Jupiter sell confirmed: [{}] {} | source={} | sig_seen={} | rpc_ata_target={} | cache_ata_target={} | total={}",
                                signal.group_name,
                                &position_before_sell.token_mint.to_string()[..12],
                                confirm_trace.source,
                                render_optional_latency(confirm_trace.signature_seen),
                                render_optional_latency(confirm_trace.rpc_ata_target),
                                render_optional_latency(confirm_trace.cache_ata_target),
                                format_latency(confirm_trace.total),
                            );
                            success = true;
                            last_sig = sig;
                            break;
                        }

                        if confirm_trace.source == "signature_error" {
                            saw_signature_error = true;
                            failure_reason = "jupiter sell failed on-chain".to_string();
                            warn!(
                                "Jupiter sell aborted after on-chain failure: [{}] {} | sig: {}",
                                signal.group_name,
                                &position_before_sell.token_mint.to_string()[..12],
                                sig,
                            );
                            break;
                        }
                    }
                    Err(err) => {
                        failure_reason = format!("jupiter sell failed: {}", err);
                        warn!("Jupiter sell failed [{}]: {}", signal.group_name, err);
                    }
                }

                if saw_signature_error {
                    break;
                }
                continue;
            }

            match self
                .try_pumpfun_sell(&position_before_sell, token_amount, send_started_at)
                .await
            {
                Ok((sig, timings)) => {
                    info!(
                        "Sell submitted: [{}] {} | signal_queue={} | snapshot={} | bc_lookup={} | quote_build={} | tx_build={} | send_call={} | total={}",
                        signal.group_name,
                        &position_before_sell.token_mint.to_string()[..12],
                        format_latency(timings.signal_queue),
                        format_latency(timings.snapshot_load),
                        format_latency(timings.bc_lookup),
                        format_latency(timings.quote_build),
                        format_latency(timings.build),
                        format_latency(timings.send_call),
                        format_latency(timings.total),
                    );

                    let confirm_trace = self
                        .wait_sell_confirm(
                            &position_before_sell,
                            &sig,
                            confirm_timeout_ms,
                            expected_after_balance,
                        )
                        .await;

                    if confirm_trace.confirmed {
                        info!(
                            "Sell confirmed: [{}] {} | source={} | sig_seen={} | rpc_ata_target={} | cache_ata_target={} | total={}",
                            signal.group_name,
                            &position_before_sell.token_mint.to_string()[..12],
                            confirm_trace.source,
                            render_optional_latency(confirm_trace.signature_seen),
                            render_optional_latency(confirm_trace.rpc_ata_target),
                            render_optional_latency(confirm_trace.cache_ata_target),
                            format_latency(confirm_trace.total),
                        );
                        success = true;
                        last_sig = sig;
                        break;
                    }

                    if confirm_trace.source == "signature_error" {
                        if !aggressive_follow {
                            match self
                                .try_jupiter_sell_with_confirm(
                                    &position_before_sell,
                                    token_amount,
                                    confirm_timeout_ms,
                                    expected_after_balance,
                                )
                                .await
                            {
                                Ok((fallback_sig, fallback_trace)) => {
                                    if fallback_trace.confirmed {
                                        info!(
                                            "Jupiter sell confirmed after Pumpfun failure: [{}] {} | source={} | sig_seen={} | rpc_ata_target={} | cache_ata_target={} | total={}",
                                            signal.group_name,
                                            &position_before_sell.token_mint.to_string()[..12],
                                            fallback_trace.source,
                                            render_optional_latency(fallback_trace.signature_seen),
                                            render_optional_latency(fallback_trace.rpc_ata_target),
                                            render_optional_latency(fallback_trace.cache_ata_target),
                                            format_latency(fallback_trace.total),
                                        );
                                        success = true;
                                        last_sig = fallback_sig;
                                        break;
                                    }

                                    if fallback_trace.source == "signature_error" {
                                        saw_signature_error = true;
                                        failure_reason = "jupiter sell failed on-chain".to_string();
                                        warn!(
                                            "Jupiter sell aborted after Pumpfun on-chain failure: [{}] {} | sig: {}",
                                            signal.group_name,
                                            &position_before_sell.token_mint.to_string()[..12],
                                            fallback_sig,
                                        );
                                        break;
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        "Jupiter sell fallback failed [{}]: {}",
                                        signal.group_name, err,
                                    );
                                }
                            }
                        }

                        saw_signature_error = true;
                        failure_reason = "sell failed on-chain".to_string();
                        warn!(
                            "Sell aborted after on-chain failure: [{}] {} | sig: {}",
                            signal.group_name,
                            &position_before_sell.token_mint.to_string()[..12],
                            sig,
                        );
                        break;
                    }
                }
                Err(err) => {
                    warn!("Pumpfun sell failed [{}]: {}", signal.group_name, err);
                    if !aggressive_follow {
                        match self
                            .try_jupiter_sell_with_confirm(
                                &position_before_sell,
                                token_amount,
                                confirm_timeout_ms,
                                expected_after_balance,
                            )
                            .await
                        {
                            Ok((sig, confirm_trace)) => {
                                if confirm_trace.confirmed {
                                    success = true;
                                    last_sig = sig;
                                    break;
                                }

                                if confirm_trace.source == "signature_error" {
                                    saw_signature_error = true;
                                    failure_reason = "jupiter sell failed on-chain".to_string();
                                    warn!(
                                        "Jupiter sell aborted after on-chain failure: [{}] {} | sig: {}",
                                        signal.group_name,
                                        &position_before_sell.token_mint.to_string()[..12],
                                        sig,
                                    );
                                    break;
                                }
                            }
                            Err(jupiter_err) => {
                                warn!(
                                    "Jupiter sell fallback failed [{}]: {}",
                                    signal.group_name, jupiter_err,
                                );
                            }
                        }
                    }
                }
            }

            if saw_signature_error {
                break;
            }
        }

        if success {
            if expected_after_balance == 0 {
                self.auto_sell
                    .mark_closed(&signal.position_key, last_sig.clone());
            } else {
                self.auto_sell
                    .apply_partial_sell(&signal.position_key, token_amount);
            }
            self.cleanup_mint_if_unused(&position_before_sell.token_mint);
            self.tg.send(TgEvent::SellSuccess {
                group_id: signal.position_key.group_id.clone(),
                group_name: signal.group_name,
                mint: position_before_sell.token_mint,
                token_name: if position_before_sell.token_name.is_empty() {
                    let ms = position_before_sell.token_mint.to_string();
                    format!("{}..{}", &ms[..6], &ms[ms.len() - 4..])
                } else {
                    position_before_sell.token_name.clone()
                },
                reason: signal.reason.to_string(),
                pnl_percent: signal.pnl_percent,
                tx_sig: last_sig,
                buy_sig: position_before_sell.buy_signature.clone(),
                hold_seconds: position_before_sell.held_seconds(),
                entry_sol_amount: position_before_sell.entry_sol_amount as f64 / 1e9,
                fully_closed: expected_after_balance == 0,
            });
        } else {
            let auto_attempt_cap_reached = self
                .auto_sell
                .get_position(&signal.position_key)
                .map(|pos| pos.max_sell_attempts_reached(MAX_AUTO_SELL_SIGNAL_ATTEMPTS))
                .unwrap_or(false);
            let suspend_auto_sell = signal.reason != SellReason::Manual
                && (saw_signature_error || auto_attempt_cap_reached);

            if suspend_auto_sell {
                if auto_attempt_cap_reached && !saw_signature_error {
                    failure_reason = format!(
                        "auto-sell suspended after {} failed cycles",
                        MAX_AUTO_SELL_SIGNAL_ATTEMPTS
                    );
                }
                self.auto_sell.suspend_auto_sell(
                    &signal.position_key,
                    previous_state,
                    MAX_AUTO_SELL_SIGNAL_ATTEMPTS,
                );
                warn!(
                    "Auto-sell suspended: [{}] {} | reason: {}",
                    signal.group_name,
                    &position_before_sell.token_mint.to_string()[..12],
                    failure_reason,
                );
            } else {
                self.auto_sell
                    .restore_after_sell_attempt(&signal.position_key, previous_state);
            }
            self.tg.send(TgEvent::SellFailed {
                group_id: signal.position_key.group_id.clone(),
                group_name: signal.group_name,
                mint: position_before_sell.token_mint,
                reason: failure_reason,
            });
        }
    }

    pub async fn handle_partial_sell(&self, group_id: &str, mint: &Pubkey, percent: u32) {
        let Some(position) = self.auto_sell.get_position_by_group_mint(group_id, mint) else {
            return;
        };

        let total_balance = position.token_amount;
        if total_balance == 0 {
            return;
        }

        let sell_amount = if percent >= 100 {
            total_balance
        } else {
            (total_balance as u128 * percent as u128 / 100) as u64
        };

        let signal = SellSignal {
            position_key: PositionKey {
                group_id: group_id.to_string(),
                token_mint: *mint,
            },
            group_name: position.group.name.clone(),
            reason: SellReason::Manual,
            current_price: position.current_price,
            pnl_percent: position.pnl_percent(),
        };

        if percent >= 100 {
            self.handle_sell_signal(signal).await;
            return;
        }

        let previous_state = position.state;
        if !self.auto_sell.mark_selling(&signal.position_key) {
            return;
        }

        let route_via_jupiter = match self.resolve_sell_snapshot(&position).await {
            Ok(snapshot) => {
                self.should_route_sell_to_jupiter(&position.group.name, mint, &snapshot)
                    .await
            }
            Err(err) => {
                warn!(
                    "Partial sell snapshot missing [{}]: {}",
                    position.group.name, err
                );
                false
            }
        };

        let sell_result = if route_via_jupiter {
            self.try_jupiter_sell(&position, sell_amount)
                .await
                .map(|sig| (sig, SellPathTimings::default()))
        } else {
            self.try_pumpfun_sell(&position, sell_amount, Instant::now())
                .await
        };

        match sell_result {
            Ok((sig, _)) => {
                self.auto_sell
                    .apply_partial_sell(&signal.position_key, sell_amount);
                self.tg.send(TgEvent::SellSuccess {
                    group_id: signal.position_key.group_id.clone(),
                    group_name: signal.group_name,
                    mint: *mint,
                    token_name: if position.token_name.is_empty() {
                        let ms = mint.to_string();
                        format!("{}..{}", &ms[..6], &ms[ms.len() - 4..])
                    } else {
                        position.token_name.clone()
                    },
                    reason: format!("manual {}%", percent),
                    pnl_percent: position.pnl_percent(),
                    tx_sig: sig,
                    buy_sig: position.buy_signature.clone(),
                    hold_seconds: position.held_seconds(),
                    entry_sol_amount: position.entry_sol_amount as f64 / 1e9,
                    fully_closed: false,
                });
            }
            Err(err) => {
                warn!("Partial sell failed [{}]: {}", position.group.name, err);
                self.auto_sell
                    .restore_after_sell_attempt(&signal.position_key, previous_state);
            }
        }
    }
}
