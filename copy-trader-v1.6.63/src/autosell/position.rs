use solana_sdk::pubkey::Pubkey;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::groups::CopyGroup;

// ============================================
// 仓位状态机
// Pending → Submitted → Confirming → Active → Selling → Closed
//    │          │           │                     │
//    └──────────┴───────────┴─── Failed ──────────┘
// ============================================

/// 仓位状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionState {
    /// 共识触发，交易已构建但未发送
    Pending,
    /// 交易已发送到至少一个通道
    Submitted,
    /// 等待链上确认（止损已生效）
    Confirming,
    /// 确认成功，完整功能启用（止盈+止损+追踪止损）
    Active,
    /// 正在执行卖出
    Selling,
    /// 已关闭
    Closed,
    /// 失败（任何阶段）
    Failed,
}

impl std::fmt::Display for PositionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionState::Pending => write!(f, "PENDING"),
            PositionState::Submitted => write!(f, "SUBMITTED"),
            PositionState::Confirming => write!(f, "CONFIRMING"),
            PositionState::Active => write!(f, "ACTIVE"),
            PositionState::Selling => write!(f, "SELLING"),
            PositionState::Closed => write!(f, "CLOSED"),
            PositionState::Failed => write!(f, "FAILED"),
        }
    }
}

/// 仓位信息（带状态机）
#[derive(Debug, Clone)]
pub struct SellAccountSnapshot {
    pub bonding_curve: Pubkey,
    pub associated_bonding_curve: Pubkey,
    pub user_ata: Pubkey,
    pub token_program: Pubkey,
    pub mirror_accounts: Vec<Pubkey>,
    pub source_wallet: Pubkey,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub group: CopyGroup,
    pub token_mint: Pubkey,
    pub state: PositionState,

    // 买入信息
    pub entry_sol_amount: u64,
    pub token_amount: u64,
    pub entry_price_sol: f64,

    // 代币信息（确认后填充）
    pub token_name: String,
    pub entry_mcap_sol: f64,

    // 追踪止损
    pub highest_price: f64,
    pub current_price: f64,

    // 时间
    pub created_at: Instant,
    pub confirmed_at: Option<Instant>,

    // 来源
    pub source_wallet: Pubkey,
    pub buy_signature: String,
    pub sell_signature: Option<String>,
    pub sell_snapshot: Option<SellAccountSnapshot>,
    pub pre_buy_ata_balance: u64,

    // 重试计数
    pub sell_attempts: u32,
    pub zero_balance_sell_skips: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionKey {
    pub group_id: String,
    pub token_mint: Pubkey,
}

impl Position {
    /// 创建新仓位（Pending 状态）
    pub fn new(
        group: CopyGroup,
        token_mint: Pubkey,
        entry_sol_amount: u64,
        entry_price_sol: f64,
        source_wallet: Pubkey,
        pre_buy_ata_balance: u64,
    ) -> Self {
        Self {
            group,
            token_mint,
            state: PositionState::Pending,
            entry_sol_amount,
            token_amount: 0,
            entry_price_sol,
            token_name: String::new(),
            entry_mcap_sol: 0.0,
            highest_price: entry_price_sol,
            current_price: entry_price_sol,
            created_at: Instant::now(),
            confirmed_at: None,
            source_wallet,
            buy_signature: String::new(),
            sell_signature: None,
            sell_snapshot: None,
            pre_buy_ata_balance,
            sell_attempts: 0,
            zero_balance_sell_skips: 0,
        }
    }

    pub fn key(&self) -> PositionKey {
        PositionKey {
            group_id: self.group.id.clone(),
            token_mint: self.token_mint,
        }
    }

    pub fn set_sell_snapshot(&mut self, snapshot: SellAccountSnapshot) {
        self.sell_snapshot = Some(snapshot);
    }

    pub fn set_token_amount_estimate(&mut self, token_amount: u64) {
        if self.token_amount == 0 && token_amount > 0 {
            self.token_amount = token_amount;
        }
    }

    // ============================================
    // 状态转换方法
    // ============================================

    /// Pending → Submitted: 交易已发送
    pub fn mark_submitted(&mut self, signature: String) -> bool {
        if self.state != PositionState::Pending {
            warn!(
                "Cannot mark submitted: {} is in {} state",
                &self.token_mint.to_string()[..12],
                self.state,
            );
            return false;
        }
        self.buy_signature = signature;
        self.state = PositionState::Submitted;
        info!(
            "Position {} → SUBMITTED | sig: {}",
            &self.token_mint.to_string()[..12],
            &self.buy_signature[..12.min(self.buy_signature.len())],
        );
        true
    }

    /// Submitted → Confirming: 开始等待链上确认（止损立即生效）
    pub fn mark_confirming(&mut self) -> bool {
        if self.state != PositionState::Submitted {
            return false;
        }
        self.state = PositionState::Confirming;
        debug!(
            "Position {} → CONFIRMING (stop-loss active)",
            &self.token_mint.to_string()[..12],
        );
        true
    }

    /// Confirming → Active: 链上确认成功，修正真实价格
    /// actual_token_amount: raw token amount (含 6 位小数精度)
    /// bc_price_sol: bonding curve 现货价格 (SOL/token)，用作成本价
    ///   如果为 0 或 None，则回退到 sol_spent / tokens 计算
    pub fn mark_active(&mut self, actual_token_amount: u64, bc_price_sol: Option<f64>) -> bool {
        if self.state != PositionState::Confirming && self.state != PositionState::Submitted {
            return false;
        }
        self.token_amount = actual_token_amount;
        if actual_token_amount > 0 {
            let display_tokens = actual_token_amount as f64 / 1e6;

            // 优先用 bonding curve 现货价格作为成本价
            // 回退: sol_spent / tokens（包含 ATA 租金等，不够准确）
            self.entry_price_sol = match bc_price_sol {
                Some(p) if p > 0.0 => p,
                _ if self.entry_sol_amount > 0 => {
                    (self.entry_sol_amount as f64 / 1e9) / display_tokens
                }
                _ => 0.0,
            };

            self.highest_price = self.entry_price_sol;
            self.current_price = self.entry_price_sol;
        }
        self.state = PositionState::Active;
        self.confirmed_at = Some(Instant::now());
        info!(
            "Position {} → ACTIVE | {:.0} tokens | entry price: {:.10} SOL",
            &self.token_mint.to_string()[..12],
            actual_token_amount as f64 / 1e6,
            self.entry_price_sol,
        );
        true
    }

    /// Active/Confirming → Selling: 开始卖出
    pub fn mark_selling(&mut self) -> bool {
        if self.state != PositionState::Active
            && self.state != PositionState::Confirming
            && self.state != PositionState::Submitted
        {
            return false;
        }
        self.state = PositionState::Selling;
        self.sell_attempts += 1;
        self.zero_balance_sell_skips = 0;
        debug!(
            "Position {} → SELLING (attempt #{})",
            &self.token_mint.to_string()[..12],
            self.sell_attempts,
        );
        true
    }

    /// Selling → Closed: 卖出成功
    pub fn mark_closed(&mut self, sell_signature: String) -> bool {
        if self.state != PositionState::Selling {
            return false;
        }
        self.sell_signature = Some(sell_signature);
        self.state = PositionState::Closed;
        let pnl = self.pnl_percent();
        info!(
            "Position {} → CLOSED | PnL: {:.2}% | attempts: {}",
            &self.token_mint.to_string()[..12],
            pnl,
            self.sell_attempts,
        );
        true
    }

    /// 任何状态 → Failed
    pub fn mark_failed(&mut self, reason: &str) -> bool {
        let old_state = self.state;
        self.state = PositionState::Failed;
        warn!(
            "Position {} → FAILED (was {}) | reason: {}",
            &self.token_mint.to_string()[..12],
            old_state,
            reason,
        );
        true
    }

    /// Selling → Active: 卖出失败，回退到 Active（允许重试）
    pub fn revert_to_active(&mut self) -> bool {
        if self.state != PositionState::Selling {
            return false;
        }
        self.state = PositionState::Active;
        debug!(
            "Position {} reverted to ACTIVE (sell attempt #{} failed)",
            &self.token_mint.to_string()[..12],
            self.sell_attempts,
        );
        true
    }

    pub fn restore_after_sell_attempt(&mut self, previous_state: PositionState) -> bool {
        self.state = previous_state;
        true
    }

    pub fn record_zero_balance_sell_skip(&mut self) -> u32 {
        self.zero_balance_sell_skips = self.zero_balance_sell_skips.saturating_add(1);
        self.zero_balance_sell_skips
    }

    pub fn apply_partial_sell(&mut self, sold_amount: u64) -> bool {
        if sold_amount == 0 || sold_amount > self.token_amount {
            return false;
        }
        self.token_amount = self.token_amount.saturating_sub(sold_amount);
        self.state = PositionState::Active;
        true
    }

    // ============================================
    // 价格更新和 PnL 计算
    // ============================================

    /// 更新当前价格，返回是否创新高
    pub fn update_price(&mut self, price: f64) -> bool {
        self.current_price = price;
        if price > self.highest_price {
            self.highest_price = price;
            true
        } else {
            false
        }
    }

    /// 计算盈亏百分比
    pub fn pnl_percent(&self) -> f64 {
        if self.entry_price_sol > 0.0 {
            ((self.current_price - self.entry_price_sol) / self.entry_price_sol) * 100.0
        } else {
            0.0
        }
    }

    /// 计算从最高点回撤百分比
    pub fn drawdown_percent(&self) -> f64 {
        if self.highest_price > 0.0 {
            ((self.highest_price - self.current_price) / self.highest_price) * 100.0
        } else {
            0.0
        }
    }

    /// 持仓时间（秒）
    pub fn held_seconds(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// 止损：仅 Active 状态（CONFIRMING 阶段价格不准确，买入未确认时不触发止损）
    pub fn can_check_stop_loss(&self) -> bool {
        self.state == PositionState::Active
    }

    /// 止盈 + 移动止损：仅 Active（确认后才生效，防报价虚高触发假止盈）
    pub fn can_check_take_profit(&self) -> bool {
        self.state == PositionState::Active
    }

    /// 是否可以卖出
    pub fn can_sell(&self) -> bool {
        matches!(
            self.state,
            PositionState::Active | PositionState::Confirming
        )
    }

    /// 最大重试次数
    pub fn max_sell_attempts_reached(&self, max: u32) -> bool {
        self.sell_attempts >= max
    }
}

/// 卖出原因
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SellReason {
    TakeProfit,
    StopLoss,
    TrailingStop,
    MaxLifetime,
    Manual,
    /// 跟聪明钱卖出（目标钱包卖出同一代币时触发）
    FollowSell,
}

impl std::fmt::Display for SellReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SellReason::TakeProfit => write!(f, "TAKE_PROFIT"),
            SellReason::StopLoss => write!(f, "STOP_LOSS"),
            SellReason::TrailingStop => write!(f, "TRAILING_STOP"),
            SellReason::MaxLifetime => write!(f, "MAX_LIFETIME"),
            SellReason::Manual => write!(f, "MANUAL"),
            SellReason::FollowSell => write!(f, "FOLLOW_SELL"),
        }
    }
}

/// 卖出信号
#[derive(Debug, Clone)]
pub struct SellSignal {
    pub position_key: PositionKey,
    pub group_name: String,
    pub reason: SellReason,
    pub current_price: f64,
    pub pnl_percent: f64,
}