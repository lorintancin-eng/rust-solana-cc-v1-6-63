use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::autosell::{AutoSellManager, PositionKey};
use crate::grpc::{AtaBalanceCache, BondingCurveCache};
use crate::telegram::{TgEvent, TgNotifier};
use crate::utils::sol_price::SolUsdPrice;

pub struct BuyConfirmer;

const BUY_CONFIRM_FAST_POLL_MS: u64 = 25;
const BUY_CONFIRM_SLOW_POLL_MS: u64 = 80;
const BUY_CONFIRM_FAST_WINDOW_MS: u64 = 400;
const ATA_RENT: u64 = 2_039_280;

pub fn format_price_gmgn(usd: f64) -> String {
    if usd <= 0.0 {
        return "$0".to_string();
    }
    if usd >= 0.01 {
        return format!("${:.4}", usd);
    }
    if usd >= 0.000001 {
        return trim_trailing_zeros(format!("${:.8}", usd));
    }

    trim_trailing_zeros(format!("${:.12}", usd))
}

pub fn format_mcap_usd(usd: f64) -> String {
    if usd >= 1_000_000.0 {
        format!("${:.2}M", usd / 1_000_000.0)
    } else if usd >= 1_000.0 {
        format!("${:.2}K", usd / 1_000.0)
    } else {
        format!("${:.0}", usd)
    }
}

fn trim_trailing_zeros(value: String) -> String {
    if !value.contains('.') {
        return value;
    }

    let trimmed = value.trim_end_matches('0').trim_end_matches('.');
    trimmed.to_string()
}

impl BuyConfirmer {
    fn poll_interval(elapsed: Duration) -> Duration {
        if elapsed.as_millis() < BUY_CONFIRM_FAST_WINDOW_MS as u128 {
            Duration::from_millis(BUY_CONFIRM_FAST_POLL_MS)
        } else {
            Duration::from_millis(BUY_CONFIRM_SLOW_POLL_MS)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_confirm_task(
        rpc_client: Arc<RpcClient>,
        auto_sell: Arc<AutoSellManager>,
        bc_cache: BondingCurveCache,
        ata_cache: AtaBalanceCache,
        sol_usd: SolUsdPrice,
        position_key: PositionKey,
        group_name: String,
        mint: Pubkey,
        signature: Signature,
        user_pubkey: Pubkey,
        entry_sol_amount: u64,
        user_ata: Pubkey,
        estimated_tokens_raw: u64,
        pre_buy_ata_balance: u64,
        tg: TgNotifier,
    ) {
        tokio::spawn(async move {
            let start = Instant::now();
            let mint_short = &mint.to_string()[..12];
            let buy_sol = entry_sol_amount as f64 / 1e9;

            info!(
                "Buy confirm started: [{}] {} | sig: {}",
                group_name,
                mint_short,
                &signature.to_string()[..16],
            );

            let mut signature_confirmed = false;
            let mut token_balance = 0u64;
            let max_wait = Duration::from_secs(10);

            while start.elapsed() < max_wait {
                if token_balance == 0 {
                    if let Some(cached_balance) = ata_cache.get(&mint) {
                        if cached_balance > pre_buy_ata_balance {
                            token_balance = cached_balance;
                            info!(
                                "Buy confirm cache hit: [{}] {} | ata_balance={} | {}ms",
                                group_name,
                                mint_short,
                                cached_balance,
                                start.elapsed().as_millis(),
                            );
                        }
                    }
                }

                let status_task = if signature_confirmed {
                    None
                } else {
                    let rpc = rpc_client.clone();
                    let sig = signature;
                    Some(tokio::task::spawn_blocking(move || {
                        rpc.get_signature_statuses(&[sig])
                    }))
                };

                let ata_task = if token_balance > pre_buy_ata_balance {
                    None
                } else {
                    let rpc = rpc_client.clone();
                    let ata = user_ata;
                    Some(tokio::task::spawn_blocking(move || {
                        rpc.get_token_account_balance(&ata)
                    }))
                };

                if let Some(status_task) = status_task {
                    match status_task.await {
                        Ok(Ok(response)) => {
                            if let Some(Some(status)) = response.value.first() {
                                if let Some(err) = &status.err {
                                    warn!(
                                        "Buy confirm on-chain failure: [{}] {} | err: {:?} | {}ms",
                                        group_name,
                                        mint_short,
                                        err,
                                        start.elapsed().as_millis(),
                                    );
                                    auto_sell.confirm_failed(&position_key, "tx failed on-chain");
                                    tg.send(TgEvent::BuyFailed {
                                        group_id: position_key.group_id.clone(),
                                        group_name: group_name.clone(),
                                        mint,
                                        reason: format!("tx failed on-chain: {:?}", err),
                                    });
                                    return;
                                }

                                signature_confirmed = true;
                                debug!(
                                    "Buy confirm signature seen: [{}] {} | {}ms",
                                    group_name,
                                    mint_short,
                                    start.elapsed().as_millis(),
                                );
                            }
                        }
                        Ok(Err(err)) => {
                            debug!(
                                "Buy confirm signature RPC error: [{}] {} | {}",
                                group_name, mint_short, err,
                            );
                        }
                        Err(err) => {
                            debug!(
                                "Buy confirm signature task error: [{}] {} | {}",
                                group_name, mint_short, err,
                            );
                        }
                    }
                }

                if token_balance == 0 {
                    if let Some(cached_balance) = ata_cache.get(&mint) {
                        if cached_balance > pre_buy_ata_balance {
                            token_balance = cached_balance;
                            info!(
                                "Buy confirm cache hit: [{}] {} | ata_balance={} | {}ms",
                                group_name,
                                mint_short,
                                cached_balance,
                                start.elapsed().as_millis(),
                            );
                        }
                    }
                }

                if token_balance <= pre_buy_ata_balance {
                    if let Some(ata_task) = ata_task {
                        match ata_task.await {
                            Ok(Ok(balance)) => {
                                let parsed = balance.amount.parse::<u64>().unwrap_or(0);
                                if parsed > pre_buy_ata_balance {
                                    token_balance = parsed;
                                    info!(
                                        "Buy confirm RPC ATA hit: [{}] {} | ata_balance={} | {}ms",
                                        group_name,
                                        mint_short,
                                        parsed,
                                        start.elapsed().as_millis(),
                                    );
                                }
                            }
                            Ok(Err(err)) => {
                                debug!(
                                    "Buy confirm ATA RPC error: [{}] {} | {}",
                                    group_name, mint_short, err,
                                );
                            }
                            Err(err) => {
                                debug!(
                                    "Buy confirm ATA task error: [{}] {} | {}",
                                    group_name, mint_short, err,
                                );
                            }
                        }
                    }
                }

                if token_balance > pre_buy_ata_balance {
                    signature_confirmed = true;
                    break;
                }

                tokio::time::sleep(Self::poll_interval(start.elapsed())).await;
            }

            if signature_confirmed && token_balance <= pre_buy_ata_balance {
                warn!(
                    "Buy confirm fallback to getTransaction: [{}] {} | {}ms",
                    group_name,
                    mint_short,
                    start.elapsed().as_millis(),
                );

                let rpc = rpc_client.clone();
                let sig = signature;
                let tx_detail = tokio::task::spawn_blocking(move || {
                    rpc.get_transaction(
                        &sig,
                        solana_transaction_status::UiTransactionEncoding::JsonParsed,
                    )
                })
                .await;

                match tx_detail {
                    Ok(Ok(tx)) => {
                        if let Some(meta) = &tx.transaction.meta {
                            use solana_transaction_status::option_serializer::OptionSerializer;

                            if let OptionSerializer::Some(ref token_balances) =
                                meta.post_token_balances
                            {
                                let user_str = user_pubkey.to_string();
                                for tb in token_balances {
                                    let owner_match = match &tb.owner {
                                        OptionSerializer::Some(owner) => owner == &user_str,
                                        _ => false,
                                    };

                                    if owner_match {
                                        if let Ok(amount) = tb.ui_token_amount.amount.parse::<u64>()
                                        {
                                            if amount > pre_buy_ata_balance {
                                                token_balance = amount;
                                                info!(
                                                    "Buy confirm tx detail hit: [{}] {} | ata_balance={} | {}ms",
                                                    group_name,
                                                    mint_short,
                                                    token_balance,
                                                    start.elapsed().as_millis(),
                                                );
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(err)) => {
                        warn!(
                            "Buy confirm getTransaction failed: [{}] {} | {}",
                            group_name, mint_short, err,
                        );
                    }
                    Err(err) => {
                        warn!(
                            "Buy confirm getTransaction task failed: [{}] {} | {}",
                            group_name, mint_short, err,
                        );
                    }
                }
            }

            if token_balance > pre_buy_ata_balance {
                let actual_delta = token_balance.saturating_sub(pre_buy_ata_balance);
                let display_tokens = actual_delta as f64 / 1e6;
                let bc_price = bc_cache
                    .get(&mint)
                    .map(|state| state.price_sol())
                    .filter(|price| *price > 0.0);
                let current_market_price = bc_price.unwrap_or(0.0);

                let entry_price = if let Some(price) = bc_price {
                    price
                } else if display_tokens > 0.0 {
                    buy_sol / display_tokens
                } else {
                    0.0
                };

                let estimated_display = estimated_tokens_raw as f64 / 1e6;
                let slippage_pct = if estimated_display > 0.0 {
                    (estimated_display - display_tokens) / estimated_display * 100.0
                } else {
                    0.0
                };

                let pnl = if entry_price > 0.0 && current_market_price > 0.0 {
                    ((current_market_price - entry_price) / entry_price) * 100.0
                } else {
                    0.0
                };

                let sol_price_usd = sol_usd.get();
                let value_sol = buy_sol * (1.0 + pnl / 100.0);
                let value_usd = value_sol * sol_price_usd;
                let cost_usd = format_price_gmgn(entry_price * sol_price_usd);

                let mcap_sol = current_market_price * crate::processor::pumpfun::PUMP_TOTAL_SUPPLY;
                let mcap_usd = mcap_sol * sol_price_usd;
                let entry_price_for_position = if entry_price > 0.0 {
                    Some(entry_price)
                } else {
                    bc_price
                };

                let open_positions = auto_sell.open_position_count_for_mint(&mint);
                let assigned_amount = if open_positions <= 1 && actual_delta > 0 {
                    actual_delta
                } else if estimated_tokens_raw > 0 {
                    estimated_tokens_raw
                } else {
                    actual_delta
                };

                auto_sell.confirm_success(
                    &position_key,
                    assigned_amount.max(1),
                    entry_price_for_position,
                );

                let mcap_str = format_mcap_usd(mcap_usd);
                let info_position_key = position_key.clone();
                let rpc_info = rpc_client.clone();
                let auto_sell_info = auto_sell.clone();
                let mint_info = mint;
                let entry_mcap_sol_val = mcap_sol;
                tokio::spawn(async move {
                    let token_info =
                        crate::utils::token_info::fetch_token_info(&rpc_info, &mint_info).await;
                    let name = if token_info.name.is_empty() {
                        short_pubkey(&mint_info)
                    } else {
                        token_info.name.clone()
                    };
                    auto_sell_info.update_token_info(&info_position_key, name, entry_mcap_sol_val);
                });

                let token_name_short = short_pubkey(&mint);

                info!(
                    "Buy confirmed: [{}] {} | {:.0} tokens | cost={} | mcap={} | pnl={:.2}% | slippage={:.1}% | value={:.4} SOL (${:.2}) | {}ms",
                    group_name,
                    mint_short,
                    display_tokens,
                    cost_usd,
                    format_mcap_usd(mcap_usd),
                    pnl,
                    slippage_pct,
                    value_sol,
                    value_usd,
                    start.elapsed().as_millis(),
                );

                tg.send(TgEvent::BuyConfirmed {
                    group_id: position_key.group_id.clone(),
                    group_name: group_name.clone(),
                    mint,
                    token_name: token_name_short,
                    spent_sol: buy_sol,
                    cost_price_usd: cost_usd,
                    mcap_usd: mcap_str,
                });

                let bg_position_key = position_key.clone();
                let rpc_bg = rpc_client.clone();
                let auto_sell_bg = auto_sell.clone();
                tokio::spawn(async move {
                    if let Some(actual_sol) =
                        Self::get_actual_sol_spent(&rpc_bg, signature, &user_pubkey).await
                    {
                        let real_price = if display_tokens > 0.0 {
                            actual_sol / display_tokens
                        } else {
                            0.0
                        };

                        if real_price > 0.0 {
                            auto_sell_bg.update_entry_price(&bg_position_key, real_price);
                            debug!(
                                "Buy entry price updated: {} | {:.10} SOL/token",
                                &mint.to_string()[..12],
                                real_price,
                            );
                        }
                    }
                });
            } else if signature_confirmed {
                warn!(
                    "Buy confirmed but ATA unchanged: [{}] {} | {}ms",
                    group_name,
                    mint_short,
                    start.elapsed().as_millis(),
                );
                auto_sell.confirm_failed(&position_key, "confirmed but zero balance");
                tg.send(TgEvent::BuyFailed {
                    group_id: position_key.group_id.clone(),
                    group_name: group_name.clone(),
                    mint,
                    reason: "transaction confirmed but ATA balance did not increase".to_string(),
                });
            } else {
                warn!(
                    "Buy confirm timeout: [{}] {} | {}ms",
                    group_name,
                    mint_short,
                    start.elapsed().as_millis(),
                );
                auto_sell.confirm_failed(&position_key, "timeout with zero balance");
                tg.send(TgEvent::BuyFailed {
                    group_id: position_key.group_id.clone(),
                    group_name,
                    mint,
                    reason: "buy confirmation timed out and ATA balance stayed unchanged"
                        .to_string(),
                });
            }
        });
    }

    async fn get_actual_sol_spent(
        rpc_client: &Arc<RpcClient>,
        signature: Signature,
        user_pubkey: &Pubkey,
    ) -> Option<f64> {
        let rpc = rpc_client.clone();
        let user = *user_pubkey;

        let result = tokio::task::spawn_blocking(move || {
            rpc.get_transaction(
                &signature,
                solana_transaction_status::UiTransactionEncoding::Json,
            )
        })
        .await;

        match result {
            Ok(Ok(tx)) => {
                let meta = tx.transaction.meta.as_ref()?;
                let account_keys: Vec<String> = match &tx.transaction.transaction {
                    solana_transaction_status::EncodedTransaction::Json(ui_tx) => {
                        match &ui_tx.message {
                            solana_transaction_status::UiMessage::Raw(msg) => {
                                msg.account_keys.clone()
                            }
                            solana_transaction_status::UiMessage::Parsed(msg) => msg
                                .account_keys
                                .iter()
                                .map(|entry| entry.pubkey.clone())
                                .collect(),
                        }
                    }
                    _ => return None,
                };

                let user_str = user.to_string();
                let user_idx = account_keys.iter().position(|key| key == &user_str)?;

                let pre_balance = meta.pre_balances.get(user_idx)?;
                let post_balance = meta.post_balances.get(user_idx)?;
                if pre_balance <= post_balance {
                    return None;
                }

                let total_spent = pre_balance - post_balance;
                let fee = meta.fee;
                let mut deductions = fee;

                use solana_transaction_status::option_serializer::OptionSerializer;

                let has_new_ata = if let (
                    OptionSerializer::Some(ref pre_tb),
                    OptionSerializer::Some(ref post_tb),
                ) = (&meta.pre_token_balances, &meta.post_token_balances)
                {
                    let pre_has_user = pre_tb.iter().any(|tb| {
                        matches!(&tb.owner, OptionSerializer::Some(owner) if owner == &user_str)
                    });
                    let post_has_user = post_tb.iter().any(|tb| {
                        matches!(&tb.owner, OptionSerializer::Some(owner) if owner == &user_str)
                    });
                    !pre_has_user && post_has_user
                } else {
                    false
                };

                if has_new_ata {
                    deductions += ATA_RENT;
                }

                let token_cost = total_spent.saturating_sub(deductions);
                let sol = token_cost as f64 / 1e9;

                debug!(
                    "Actual SOL spent: {:.6} SOL (total={}, fee={}, ata_rent={}, token_cost={})",
                    sol,
                    total_spent,
                    fee,
                    if has_new_ata { ATA_RENT } else { 0 },
                    token_cost,
                );

                Some(sol)
            }
            Ok(Err(err)) => {
                debug!("getTransaction failed: {}", err);
                None
            }
            Err(err) => {
                debug!("getTransaction join error: {}", err);
                None
            }
        }
    }
}

fn short_pubkey(pubkey: &Pubkey) -> String {
    let value = pubkey.to_string();
    format!("{}..{}", &value[..6], &value[value.len() - 4..])
}
