# Project Sequence Diagram

This document captures the current runtime sequence of the `copy-trader` system.

Scope:
- binary startup
- live trade intake
- consensus and buy execution
- buy confirmation and position activation
- auto-sell monitoring and sell execution
- Telegram control plane

Important current limitation:
- The runtime parser in `src/grpc/subscriber.rs` currently only feeds Pump.fun trades into the main execution path.
- PumpSwap / Raydium processors exist in `src/processor/`, but they are not fully wired into the live trade intake path yet.

## 1. Startup And Runtime Wiring

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Bin as copy-trader
    participant Config as AppConfig
    participant Groups as GroupManager
    participant BH as BlockhashCache
    participant Price as SolUsdPrice
    participant TradeSub as GrpcSubscriber
    participant AcctSub as AccountSubscriber
    participant AutoSell as AutoSellManager
    participant SellExec as SellExecutor
    participant TG as TgBot

    User->>Bin: start process
    Bin->>Config: from_env()
    Config-->>Bin: RPC, gRPC, wallet, Jito, 0slot, autosell, TG config
    Bin->>Groups: load_or_default()
    Groups-->>Bin: copy groups, selected group, blocklist, zero-slot flag
    Bin->>BH: init_blockhash_cache()
    Bin->>BH: start_refresh_task()
    Bin->>Price: init(default price)
    Bin->>Price: start_refresh_task()
    Bin->>AutoSell: new(config, bc_cache, rpc, sol_price)
    Bin->>AcctSub: new(grpc_account_url, token, bc_cache, ata_cache)
    Bin->>SellExec: new(...)

    alt Telegram configured
        Bin->>TG: from_parts(...)
        Bin->>TG: run()
    end

    alt auto-sell enabled
        Bin->>AutoSell: start_grpc_monitor(account_update_rx, sell_signal_tx)
        Bin->>AutoSell: start_fallback_monitor(sell_signal_tx)
    end

    par trade stream loop
        Bin->>TradeSub: subscribe(trade_tx)
    and account stream loop
        Bin->>AcctSub: subscribe(account_update_tx)
    and wallet hot reload loop
        Bin->>TradeSub: update_target_wallets(all_target_wallets)
    end
```

## 2. Trade Intake To Buy Trigger

```mermaid
sequenceDiagram
    autonumber
    actor Target as Target Wallet
    participant Rabbit as RabbitStream gRPC
    participant TradeSub as GrpcSubscriber
    participant Main as main.rs
    participant Groups as GroupManager
    participant Prefetch as PrefetchCache
    participant AcctSub as AccountSubscriber
    participant Consensus as ConsensusEngine
    participant BuyExec as execute_buy()
    participant AutoSell as AutoSellManager

    Target->>Rabbit: broadcast swap transaction
    Rabbit-->>TradeSub: transaction update
    TradeSub->>TradeSub: parse_transaction()
    TradeSub->>TradeSub: try_parse_instruction()
    alt recognized live path
        TradeSub-->>Main: DetectedTrade
    else not recognized
        TradeSub-->>Main: skip
    end

    Main->>Main: should_skip_signature()
    Main->>Groups: groups_for_wallet(source_wallet)
    Groups-->>Main: matching enabled groups
    Main->>Main: extract_token_info()
    Main->>Groups: is_blocked(mint)

    alt any group wants entry
        Main->>Prefetch: prefetch_token(mint, token_program, accounts, source_wallet, signature, quality)
        Prefetch-->>Main: prefetched token data
        Main->>AcctSub: track_bonding_curve(mint, bonding_curve)
        Main->>AcctSub: track_ata(mint, user_ata)
    end

    opt target wallet sold and group follow-sell is enabled
        Main->>AutoSell: get_position_by_group_mint(group_id, mint)
        AutoSell-->>Main: existing position
        Main-->>AutoSell: emit SellSignal(FollowSell)
    end

    alt group consensus_min_wallets <= 1
        Main->>BuyExec: direct execute_buy(...)
    else group consensus_min_wallets > 1
        Main->>Consensus: submit_signal(BuySignal)
        Consensus->>Consensus: collect wallet votes by group + mint
        alt threshold reached
            Consensus-->>Main: ConsensusTrigger via consensus_rx
            Main->>BuyExec: execute_buy(...) using canonical signal
        else threshold not reached
            Consensus-->>Main: wait for more wallets or timeout cleanup
        end
    end
```

## 3. Buy Execution And Confirmation

```mermaid
sequenceDiagram
    autonumber
    participant BuyExec as execute_buy()
    participant Prefetch as PrefetchCache
    participant BC as BondingCurveCache
    participant PF as PumpfunProcessor
    participant BH as BlockhashCache
    participant Builder as TxBuilder
    participant Sender as TxSender
    participant Chain as RPC / Jito / 0slot
    participant Confirm as BuyConfirmer
    participant AutoSell as AutoSellManager
    participant TG as TgNotifier

    BuyExec->>Prefetch: get() or get_or_wait()
    Prefetch-->>BuyExec: prefetched mint / ATA / bonding curve / mirror accounts
    BuyExec->>BC: get(mint)
    alt bonding curve cache miss
        BuyExec->>PF: prefetch_bonding_curve()
        PF-->>BC: update bonding curve state
    end

    alt cached bonding curve available
        BuyExec->>PF: buy_from_cached_state(...)
        PF-->>BuyExec: MirrorInstruction + estimated tokens
    else target instruction available
        BuyExec->>PF: buy_from_target_instruction(...)
        PF-->>BuyExec: MirrorInstruction + estimated tokens
    else no usable quote path
        BuyExec-->>TG: Buy skipped / failed
    end

    BuyExec->>AutoSell: create Position(Pending)
    BuyExec->>BH: get_sync()

    alt zero-slot buy enabled
        BuyExec->>Builder: build_0slot_transaction(...)
    else jito enabled
        BuyExec->>Builder: build_jito_bundle_transaction(...)
    else standard path
        BuyExec->>Builder: build_transaction(...)
    end

    Builder-->>BuyExec: signed VersionedTransaction
    BuyExec->>Sender: fire_and_forget(...)
    Sender->>Chain: send over zero-slot / RPC / Jito channels
    Sender-->>BuyExec: signature
    BuyExec-->>TG: BuySubmitted

    opt auto-sell enabled
        BuyExec->>AutoSell: add_position()
        BuyExec->>AutoSell: mark_submitted()
        BuyExec->>AutoSell: mark_confirming()
        BuyExec->>Confirm: spawn_confirm_task(...)
    end

    Confirm->>Chain: poll signature status
    Confirm->>Chain: poll ATA balance / transaction detail
    alt buy confirmed
        Confirm->>AutoSell: confirm_success()
        Confirm->>AutoSell: update_entry_price() in background when exact SOL spent is known
        Confirm-->>TG: BuyConfirmed
    else failed or timeout
        Confirm->>AutoSell: confirm_failed()
        Confirm-->>TG: BuyFailed
    end
```

## 4. Auto-Sell Monitoring And Sell Execution

```mermaid
sequenceDiagram
    autonumber
    participant AcctSub as AccountSubscriber
    participant BC as BondingCurveCache
    participant ATA as AtaBalanceCache
    participant AutoSell as AutoSellManager
    participant Fallback as Fallback Monitor
    participant SellExec as SellExecutor
    participant PF as PumpfunProcessor
    participant Jupiter as JupiterSeller
    participant Sender as TxSender
    participant Chain as RPC / Jito
    participant TG as TgNotifier
    participant Perf as GroupPerformanceStore

    AcctSub-->>BC: bonding curve account updates
    AcctSub-->>ATA: ATA balance updates
    AcctSub-->>AutoSell: AccountUpdate channel

    AutoSell->>AutoSell: update position price
    AutoSell->>AutoSell: check_exit_conditions()
    alt TP / SL / trailing / follow-sell / max-hold triggered
        AutoSell-->>SellExec: SellSignal
    end

    Fallback->>AutoSell: periodic scan
    Fallback->>AutoSell: confirm submitted positions if gRPC missed ATA update
    Fallback->>AutoSell: emit SellSignal when max hold or price rule is hit

    SellExec->>AutoSell: get_position()
    SellExec->>SellExec: resolve_sell_snapshot()
    SellExec->>PF: is_bonding_curve_migrated()

    alt migrated to external pool
        SellExec->>Jupiter: build_sell_transaction()
        Jupiter-->>SellExec: signed sell tx bytes
        SellExec->>Sender: fire_and_forget_without_0slot()
    else still Pump.fun curve
        SellExec->>PF: build_sell_instruction_from_mirror()
        SellExec->>Sender: fire_and_forget_without_0slot()
    end

    Sender->>Chain: send sell transaction
    SellExec->>Chain: wait_sell_confirm()
    alt confirmed
        alt full close
            SellExec->>AutoSell: mark_closed()
            SellExec->>AutoSell: cleanup mint caches if unused
            SellExec-->>TG: SellSuccess
            TG->>Perf: record_closed_trade()
        else partial close
            SellExec->>AutoSell: apply_partial_sell()
            SellExec-->>TG: SellSuccess
        end
    else failed
        SellExec->>AutoSell: restore_after_sell_attempt() or suspend_auto_sell()
        SellExec-->>TG: SellFailed
        TG->>Perf: record_sell_failed()
    end
```

## 5. Telegram Control Plane

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant TGAPI as Telegram API
    participant TG as TgBot
    participant Groups as GroupManager
    participant AutoSell as AutoSellManager
    participant Consensus as ConsensusEngine
    participant SellExec as SellExecutor
    participant Perf as GroupPerformanceStore
    participant MainState as is_running flag

    User->>TGAPI: send command or callback
    TGAPI-->>TG: getUpdates result
    TG->>TG: handle_message() / handle_callback_v2()

    alt /start
        TG->>MainState: set true
        TG-->>User: bot running
    else /stop
        TG->>MainState: set false
        TG-->>User: bot stopped
    else /groups /groupadd /groupdel /usegroup /set
        TG->>Groups: load / mutate groups
        Groups-->>TG: persisted copy_groups.json state
        TG-->>User: updated group view
    else /pos
        TG->>AutoSell: get_active_positions() or get_group_positions()
        AutoSell-->>TG: positions
        TG-->>User: position list and sell buttons
    else /sellall or position sell callback
        TG->>SellExec: handle_partial_sell() or emit manual SellSignal
        SellExec-->>TG: later SellSuccess / SellFailed event
    else /status
        TG->>Consensus: pending_count()
        TG->>AutoSell: position_count()
        TG-->>User: runtime status
    else /gstats
        TG->>Perf: render_overview_html()
        Perf-->>TG: group performance report
        TG-->>User: performance summary
    end
```

## Source Map

- `src/main.rs`
  - startup and orchestration
  - trade intake loop
  - direct buy path
  - consensus-triggered buy path
- `src/config.rs`
  - env config loading
- `src/groups.rs`
  - group persistence and wallet-to-group mapping
- `src/grpc/subscriber.rs`
  - live trade parsing
- `src/grpc/account_subscriber.rs`
  - live bonding curve and ATA updates
- `src/processor/pumpfun.rs`
  - Pump.fun quote logic and mirror instruction construction
- `src/tx/builder.rs`
  - V0 transaction assembly
- `src/tx/sender.rs`
  - multi-channel low-latency send path
- `src/tx/confirm.rs`
  - buy confirmation
- `src/autosell/manager.rs`
  - price monitoring and sell signal generation
- `src/tx/sell_executor.rs`
  - sell routing and sell confirmation
- `src/telegram.rs`
  - Telegram command handling and event notifications
- `src/group_stats.rs`
  - group performance aggregation
