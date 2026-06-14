use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::persistence;
use super::position::{Position, PositionKey, PositionState, SellReason, SellSignal};
use crate::config::AppConfig;
use crate::grpc::{AccountUpdate, BondingCurveCache};
use crate::utils::sol_price::SolUsdPrice;

const MAX_AUTO_SELL_SIGNAL_ATTEMPTS: u32 = 5;

pub struct AutoSellManager {
    positions: Arc<DashMap<PositionKey, Position>>,
    config: AppConfig,
    bc_cache: BondingCurveCache,
    rpc_client: Arc<RpcClient>,
    sol_usd: SolUsdPrice,
}

impl AutoSellManager {
    pub fn new(
        config: AppConfig,
        bc_cache: BondingCurveCache,
        rpc_client: Arc<RpcClient>,
        sol_usd: SolUsdPrice,
    ) -> Self {
        Self {
            positions: Arc::new(DashMap::new()),
            config,
            bc_cache,
            rpc_client,
            sol_usd,
        }
    }

    fn save(&self) {
        persistence::save_positions(&self.positions);
    }

    pub fn add_position(&self, position: Position) {
        let key = position.key();
        info!(
            "Position opened: [{}] {} | state: {} | entry: {:.6} SOL",
            position.group.name,
            &position.token_mint.to_string()[..12],
            position.state,
            position.entry_price_sol,
        );
        self.positions.insert(key, position);
        self.save();
    }

    pub fn mark_submitted(&self, key: &PositionKey, signature: String) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.mark_submitted(signature);
        }
    }

    pub fn mark_confirming(&self, key: &PositionKey) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.mark_confirming();
        }
    }

    pub fn confirm_success(
        &self,
        key: &PositionKey,
        actual_token_amount: u64,
        bc_price_sol: Option<f64>,
    ) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.mark_active(actual_token_amount, bc_price_sol);
        }
    }

    pub fn update_entry_price(&self, key: &PositionKey, real_price: f64) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.entry_price_sol = real_price;
            pos.highest_price = pos.highest_price.max(real_price);
        }
    }

    pub fn mark_selling(&self, key: &PositionKey) -> bool {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.mark_selling()
        } else {
            false
        }
    }

    pub fn mark_closed(&self, key: &PositionKey, sell_signature: String) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.mark_closed(sell_signature);
        }
        self.save();
    }

    pub fn revert_to_active(&self, key: &PositionKey) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.revert_to_active();
        }
        self.save();
    }

    pub fn restore_after_sell_attempt(&self, key: &PositionKey, previous_state: PositionState) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.restore_after_sell_attempt(previous_state);
        }
        self.save();
    }

    pub fn suspend_auto_sell(
        &self,
        key: &PositionKey,
        previous_state: PositionState,
        max_attempts: u32,
    ) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.state = previous_state;
            pos.sell_attempts = pos.sell_attempts.max(max_attempts);
            warn!(
                "Position {} auto-sell suspended after repeated failures | attempts={}",
                &pos.token_mint.to_string()[..12],
                pos.sell_attempts,
            );
        }
        self.save();
    }

    pub fn apply_partial_sell(&self, key: &PositionKey, sold_amount: u64) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.apply_partial_sell(sold_amount);
        }
        self.save();
    }

    pub fn confirm_failed(&self, key: &PositionKey, reason: &str) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.mark_failed(reason);
        }
        self.save();
    }

    pub fn record_zero_balance_sell_skip(&self, key: &PositionKey) -> Option<u32> {
        let skips = if let Some(mut pos) = self.positions.get_mut(key) {
            Some(pos.record_zero_balance_sell_skip())
        } else {
            None
        };
        if skips.is_some() {
            self.save();
        }
        skips
    }

    pub fn get_position(&self, key: &PositionKey) -> Option<Position> {
        self.positions.get(key).map(|entry| entry.value().clone())
    }

    pub fn get_position_by_group_mint(&self, group_id: &str, mint: &Pubkey) -> Option<Position> {
        self.get_position(&PositionKey {
            group_id: group_id.to_string(),
            token_mint: *mint,
        })
    }

    pub fn get_positions_for_mint(&self, mint: &Pubkey) -> Vec<Position> {
        self.positions
            .iter()
            .filter(|entry| entry.key().token_mint == *mint)
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn open_position_count_for_mint(&self, mint: &Pubkey) -> usize {
        self.positions
            .iter()
            .filter(|entry| {
                entry.key().token_mint == *mint
                    && !matches!(entry.state, PositionState::Closed | PositionState::Failed)
            })
            .count()
    }

    pub fn position_count(&self) -> usize {
        self.positions.len()
    }

    pub fn get_open_position_mints(&self) -> HashSet<Pubkey> {
        self.positions
            .iter()
            .filter(|entry| !matches!(entry.state, PositionState::Closed | PositionState::Failed))
            .map(|entry| entry.key().token_mint)
            .collect()
    }

    pub fn get_active_positions(&self) -> Vec<Position> {
        self.positions
            .iter()
            .filter(|entry| {
                !matches!(
                    entry.state,
                    PositionState::Closed | PositionState::Failed | PositionState::Selling
                )
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_group_positions(&self, group_id: &str) -> Vec<Position> {
        self.positions
            .iter()
            .filter(|entry| {
                entry.key().group_id == group_id
                    && !matches!(
                        entry.state,
                        PositionState::Closed | PositionState::Failed | PositionState::Selling
                    )
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_sellable_positions(&self) -> Vec<PositionKey> {
        self.positions
            .iter()
            .filter(|entry| entry.can_sell())
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn update_token_info(&self, key: &PositionKey, token_name: String, entry_mcap_sol: f64) {
        if let Some(mut pos) = self.positions.get_mut(key) {
            pos.token_name = token_name;
            pos.entry_mcap_sol = entry_mcap_sol;
        }
    }

    pub fn start_grpc_monitor(
        &self,
        mut account_update_rx: mpsc::UnboundedReceiver<AccountUpdate>,
        sell_signal_tx: mpsc::UnboundedSender<SellSignal>,
    ) -> tokio::task::JoinHandle<()> {
        let positions = self.positions.clone();
        let sol_usd = self.sol_usd.clone();
        let bc_cache = self.bc_cache.clone();

        tokio::spawn(async move {
            info!("Auto-sell monitor started (group-aware)");

            while let Some(update) = account_update_rx.recv().await {
                match update {
                    AccountUpdate::BondingCurve(bc_update) => {
                        let mint = bc_update.mint;
                        let keys: Vec<PositionKey> = positions
                            .iter()
                            .filter(|entry| entry.key().token_mint == mint)
                            .map(|entry| entry.key().clone())
                            .collect();

                        for key in keys {
                            let signal = {
                                let mut pos = match positions.get_mut(&key) {
                                    Some(entry) => entry,
                                    None => continue,
                                };

                                let current_price = bc_update.state.price_sol();
                                if current_price <= 0.0 {
                                    continue;
                                }

                                pos.update_price(current_price);
                                Self::check_exit_conditions(&pos)
                            };

                            if let Some(signal) = signal {
                                let sol_price = sol_usd.get();
                                let entry_sol = {
                                    let pos = positions.get(&signal.position_key).unwrap();
                                    pos.entry_sol_amount as f64 / 1e9
                                };
                                let value_usd =
                                    entry_sol * (1.0 + signal.pnl_percent / 100.0) * sol_price;
                                info!(
                                    "SELL SIGNAL: [{}] {} | reason: {} | PnL: {:.2}% | value=${:.2}",
                                    signal.group_name,
                                    &signal.position_key.token_mint.to_string()[..12],
                                    signal.reason,
                                    signal.pnl_percent,
                                    value_usd,
                                );
                                if sell_signal_tx.send(signal).is_err() {
                                    error!("Sell signal channel closed");
                                    return;
                                }
                            }
                        }
                    }
                    AccountUpdate::AtaBalance(ata_update) => {
                        let keys: Vec<PositionKey> = positions
                            .iter()
                            .filter(|entry| entry.key().token_mint == ata_update.mint)
                            .map(|entry| entry.key().clone())
                            .collect();

                        for key in keys {
                            if let Some(pos) = positions.get(&key) {
                                if matches!(
                                    pos.state,
                                    PositionState::Submitted
                                        | PositionState::Confirming
                                        | PositionState::Selling
                                ) {
                                    debug!(
                                        "ATA update observed: [{}] {} | amount={}",
                                        pos.group.name,
                                        &ata_update.mint.to_string()[..12],
                                        ata_update.amount,
                                    );
                                }
                            }
                        }
                    }
                }
            }

            warn!("Account update channel closed, auto-sell monitor stopped");
        })
    }

    pub fn start_fallback_monitor(
        &self,
        sell_signal_tx: mpsc::UnboundedSender<SellSignal>,
    ) -> tokio::task::JoinHandle<()> {
        let positions = self.positions.clone();
        let config = self.config.clone();
        let bc_cache = self.bc_cache.clone();
        let rpc = self.rpc_client.clone();
        let user_pubkey = config.pubkey;
        let interval = Duration::from_secs(config.price_check_interval_secs);

        tokio::spawn(async move {
            info!("Fallback monitor started (interval: {:?})", interval);

            loop {
                tokio::time::sleep(interval).await;

                if positions.is_empty() {
                    continue;
                }

                let keys: Vec<PositionKey> =
                    positions.iter().map(|entry| entry.key().clone()).collect();

                for key in keys {
                    let Some(snapshot) = positions.get(&key).map(|entry| entry.value().clone())
                    else {
                        continue;
                    };

                    let needs_confirm_check = matches!(
                        snapshot.state,
                        PositionState::Submitted | PositionState::Confirming
                    ) && snapshot.held_seconds()
                        >= config.confirm_timeout_secs;

                    if needs_confirm_check {
                        let user_ata = snapshot
                            .sell_snapshot
                            .as_ref()
                            .map(|sell_snapshot| sell_snapshot.user_ata)
                            .unwrap_or_else(|| {
                                get_associated_token_address(&user_pubkey, &snapshot.token_mint)
                            });
                        let rpc_clone = rpc.clone();
                        let ata = user_ata;
                        let balance = tokio::task::spawn_blocking(move || {
                            rpc_clone
                                .get_token_account_balance(&ata)
                                .map(|value| value.amount.parse::<u64>().unwrap_or(0))
                                .unwrap_or(0)
                        })
                        .await
                        .unwrap_or(0);

                        if let Some(mut pos) = positions.get_mut(&key) {
                            if balance > pos.pre_buy_ata_balance {
                                let actual_delta = balance.saturating_sub(pos.pre_buy_ata_balance);
                                let assigned_amount = if actual_delta > 0
                                    && positions
                                        .iter()
                                        .filter(|entry| {
                                            entry.key().token_mint == pos.token_mint
                                                && !matches!(
                                                    entry.state,
                                                    PositionState::Closed | PositionState::Failed
                                                )
                                        })
                                        .count()
                                        <= 1
                                {
                                    actual_delta
                                } else if pos.token_amount > 0 {
                                    pos.token_amount
                                } else {
                                    actual_delta
                                };

                                let bc_price =
                                    bc_cache.get(&pos.token_mint).map(|state| state.price_sol());
                                info!(
                                    "Confirm fallback: [{}] {} | balance={} | assigned={}",
                                    pos.group.name,
                                    &pos.token_mint.to_string()[..12],
                                    balance,
                                    assigned_amount,
                                );
                                pos.mark_active(assigned_amount, bc_price);
                            } else {
                                warn!(
                                    "Confirm fallback failed: [{}] {} | balance={} <= before={}",
                                    pos.group.name,
                                    &pos.token_mint.to_string()[..12],
                                    balance,
                                    pos.pre_buy_ata_balance,
                                );
                                pos.mark_failed("buy not confirmed: balance unchanged");
                            }
                        }
                        continue;
                    }

                    let signal = {
                        let mut pos = match positions.get_mut(&key) {
                            Some(entry) => entry,
                            None => continue,
                        };

                        let max_hold = pos.group.max_hold_seconds;
                        if max_hold > 0
                            && pos.held_seconds() >= max_hold
                            && pos.can_sell()
                            && !pos.max_sell_attempts_reached(MAX_AUTO_SELL_SIGNAL_ATTEMPTS)
                        {
                            Some(SellSignal {
                                position_key: pos.key().clone(),
                                group_name: pos.group.name.clone(),
                                reason: SellReason::MaxLifetime,
                                current_price: pos.current_price,
                                pnl_percent: pos.pnl_percent(),
                            })
                        } else if let Some(bc_state) = bc_cache.get(&pos.token_mint) {
                            let price = bc_state.price_sol();
                            if price > 0.0 {
                                pos.update_price(price);
                            }
                            Self::check_exit_conditions(&pos)
                        } else {
                            None
                        }
                    };

                    if let Some(signal) = signal {
                        info!(
                            "SELL SIGNAL: [{}] {} | reason: {} | PnL: {:.2}%",
                            signal.group_name,
                            &signal.position_key.token_mint.to_string()[..12],
                            signal.reason,
                            signal.pnl_percent,
                        );
                        if sell_signal_tx.send(signal).is_err() {
                            error!("Sell signal channel closed");
                            return;
                        }
                    }
                }

                positions.retain(|_, pos| {
                    if pos.state == PositionState::Closed || pos.state == PositionState::Failed {
                        pos.created_at.elapsed().as_secs() < 60
                    } else {
                        true
                    }
                });
            }
        })
    }

    fn check_exit_conditions(pos: &Position) -> Option<SellSignal> {
        let pnl = pos.pnl_percent();
        let key = pos.key();

        if pos.group.max_hold_seconds > 0
            && pos.held_seconds() >= pos.group.max_hold_seconds
            && pos.can_sell()
            && !pos.max_sell_attempts_reached(MAX_AUTO_SELL_SIGNAL_ATTEMPTS)
        {
            return Some(SellSignal {
                position_key: key,
                group_name: pos.group.name.clone(),
                reason: SellReason::MaxLifetime,
                current_price: pos.current_price,
                pnl_percent: pnl,
            });
        }

        if pos.group.follow_sell_mode() {
            return None;
        }

        if pos.can_check_stop_loss() && pnl <= -pos.group.stop_loss_percent {
            return Some(SellSignal {
                position_key: key,
                group_name: pos.group.name.clone(),
                reason: SellReason::StopLoss,
                current_price: pos.current_price,
                pnl_percent: pnl,
            });
        }

        if pos.can_check_take_profit() && pnl >= pos.group.take_profit_percent {
            return Some(SellSignal {
                position_key: key,
                group_name: pos.group.name.clone(),
                reason: SellReason::TakeProfit,
                current_price: pos.current_price,
                pnl_percent: pnl,
            });
        }

        if pos.can_check_take_profit()
            && pos.group.trailing_stop_percent > 0.0
            && pos.highest_price > 0.0
            && pnl > 0.0
            && pos.drawdown_percent() >= pos.group.trailing_stop_percent
        {
            return Some(SellSignal {
                position_key: key,
                group_name: pos.group.name.clone(),
                reason: SellReason::TrailingStop,
                current_price: pos.current_price,
                pnl_percent: pnl,
            });
        }

        None
    }
}
