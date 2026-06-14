use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::groups::GroupManager;

const GROUP_STATS_FILE: &str = "group_stats.json";
const GROUP_TRADE_HISTORY_FILE: &str = "group_trade_history.jsonl";
const GROUP_STATS_REPORT_FILE: &str = "group_stats_report.md";
const WIN_THRESHOLD_PERCENT: f64 = 0.5;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TradeOutcome {
    Win,
    Loss,
    Flat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTradeRecord {
    pub group_id: String,
    pub group_name: String,
    pub mint: String,
    pub token_name: String,
    pub buy_signature: String,
    pub sell_signature: String,
    pub exit_reason: String,
    pub entry_sol_amount: f64,
    pub pnl_percent: f64,
    pub hold_seconds: u64,
    pub opened_at_unix_secs: u64,
    pub closed_at_unix_secs: u64,
    pub outcome: TradeOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupPerformanceSummary {
    pub group_id: String,
    pub group_name: String,
    pub buy_confirmed_count: u64,
    pub buy_failed_count: u64,
    pub sell_failed_count: u64,
    pub closed_trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub flats: u64,
    pub avg_pnl_percent: f64,
    pub net_pnl_percent_sum: f64,
    pub best_trade_percent: Option<f64>,
    pub worst_trade_percent: Option<f64>,
    pub avg_hold_seconds: f64,
    pub last_updated_unix_secs: u64,
}

impl GroupPerformanceSummary {
    fn blank(group_id: &str, group_name: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            group_name: group_name.to_string(),
            ..Default::default()
        }
    }

    pub fn win_rate_percent(&self) -> f64 {
        if self.closed_trades == 0 {
            0.0
        } else {
            self.wins as f64 * 100.0 / self.closed_trades as f64
        }
    }

    fn touch(&mut self) {
        self.last_updated_unix_secs = now_unix_secs();
    }

    fn apply_group_name(&mut self, group_name: &str) {
        self.group_name = group_name.to_string();
        self.touch();
    }

    fn record_buy_confirmed(&mut self) {
        self.buy_confirmed_count += 1;
        self.touch();
    }

    fn record_buy_failed(&mut self) {
        self.buy_failed_count += 1;
        self.touch();
    }

    fn record_sell_failed(&mut self) {
        self.sell_failed_count += 1;
        self.touch();
    }

    fn record_closed_trade(&mut self, record: &ClosedTradeRecord) {
        self.closed_trades += 1;
        self.net_pnl_percent_sum += record.pnl_percent;
        self.avg_pnl_percent = self.net_pnl_percent_sum / self.closed_trades as f64;

        let total_hold = self.avg_hold_seconds * (self.closed_trades.saturating_sub(1) as f64)
            + record.hold_seconds as f64;
        self.avg_hold_seconds = total_hold / self.closed_trades as f64;

        self.best_trade_percent = Some(
            self.best_trade_percent
                .map(|best| best.max(record.pnl_percent))
                .unwrap_or(record.pnl_percent),
        );
        self.worst_trade_percent = Some(
            self.worst_trade_percent
                .map(|worst| worst.min(record.pnl_percent))
                .unwrap_or(record.pnl_percent),
        );

        match record.outcome {
            TradeOutcome::Win => self.wins += 1,
            TradeOutcome::Loss => self.losses += 1,
            TradeOutcome::Flat => self.flats += 1,
        }
        self.touch();
    }
}

pub struct GroupPerformanceStore {
    summaries: RwLock<HashMap<String, GroupPerformanceSummary>>,
}

impl GroupPerformanceStore {
    pub fn load_or_default() -> Self {
        let summaries = if Path::new(GROUP_STATS_FILE).exists() {
            fs::read_to_string(GROUP_STATS_FILE)
                .ok()
                .and_then(|raw| serde_json::from_str::<Vec<GroupPerformanceSummary>>(&raw).ok())
                .map(|items| {
                    items
                        .into_iter()
                        .map(|item| (item.group_id.clone(), item))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            HashMap::new()
        };

        Self {
            summaries: RwLock::new(summaries),
        }
    }

    pub fn sync_groups(&self, groups: &GroupManager) {
        let mut summaries = self.summaries.write().unwrap();
        for group in groups.all_groups() {
            summaries
                .entry(group.id.clone())
                .and_modify(|summary| summary.apply_group_name(&group.name))
                .or_insert_with(|| GroupPerformanceSummary::blank(&group.id, &group.name));
        }
        drop(summaries);
        self.persist(groups);
    }

    pub fn record_buy_confirmed(&self, groups: &GroupManager, group_id: &str, group_name: &str) {
        self.update_summary(groups, group_id, group_name, |summary| {
            summary.record_buy_confirmed();
        });
    }

    pub fn record_buy_failed(&self, groups: &GroupManager, group_id: &str, group_name: &str) {
        self.update_summary(groups, group_id, group_name, |summary| {
            summary.record_buy_failed();
        });
    }

    pub fn record_sell_failed(&self, groups: &GroupManager, group_id: &str, group_name: &str) {
        self.update_summary(groups, group_id, group_name, |summary| {
            summary.record_sell_failed();
        });
    }

    pub fn record_closed_trade(&self, groups: &GroupManager, record: ClosedTradeRecord) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(GROUP_TRADE_HISTORY_FILE)
        {
            if let Ok(json) = serde_json::to_string(&record) {
                let _ = writeln!(file, "{}", json);
            }
        }

        self.update_summary(groups, &record.group_id, &record.group_name, |summary| {
            summary.record_closed_trade(&record);
        });
    }

    pub fn render_overview_html(&self, groups: &GroupManager) -> String {
        let report = self.snapshot(groups);
        let mut text = String::from("<b>组合绩效报表</b>");
        text.push_str("\n\n说明: 胜率仅统计已完整平仓的交易，未平仓浮盈浮亏不计入。");
        text.push_str("\n平局区间: -0.5% ~ +0.5%");

        for summary in report {
            text.push_str(&format!(
                "\n\n<b>{}</b> ({}){}\n已平仓: {}\n胜率: {:.1}% | 胜/负/平: {}/{}/{}\n平均PnL: {:+.2}% | 累计PnL: {:+.2}%\n最佳/最差: {} / {}\n买入成功: {} | 买入失败: {} | 卖出失败: {}\n平均持仓: {}",
                summary.group_name,
                summary.group_id,
                if summary.closed_trades == 0 && summary.buy_confirmed_count == 0 && summary.buy_failed_count == 0 {
                    " <i>(暂无统计)</i>"
                } else {
                    ""
                },
                summary.closed_trades,
                summary.win_rate_percent(),
                summary.wins,
                summary.losses,
                summary.flats,
                summary.avg_pnl_percent,
                summary.net_pnl_percent_sum,
                format_optional_percent(summary.best_trade_percent),
                format_optional_percent(summary.worst_trade_percent),
                summary.buy_confirmed_count,
                summary.buy_failed_count,
                summary.sell_failed_count,
                format_hold_seconds(summary.avg_hold_seconds.round() as u64),
            ));
        }

        text.push_str(&format!(
            "\n\n本地文件:\n<code>{}</code>\n<code>{}</code>\n<code>{}</code>",
            GROUP_STATS_FILE, GROUP_TRADE_HISTORY_FILE, GROUP_STATS_REPORT_FILE
        ));

        text
    }

    fn update_summary<F>(&self, groups: &GroupManager, group_id: &str, group_name: &str, mutator: F)
    where
        F: FnOnce(&mut GroupPerformanceSummary),
    {
        let mut summaries = self.summaries.write().unwrap();
        let summary = summaries
            .entry(group_id.to_string())
            .or_insert_with(|| GroupPerformanceSummary::blank(group_id, group_name));
        summary.apply_group_name(group_name);
        mutator(summary);
        drop(summaries);
        self.persist(groups);
    }

    fn persist(&self, groups: &GroupManager) {
        self.save_json(groups);
        self.save_markdown_report(groups);
    }

    fn save_json(&self, groups: &GroupManager) {
        let snapshot = self.snapshot(groups);
        if let Ok(json) = serde_json::to_string_pretty(&snapshot) {
            let _ = fs::write(GROUP_STATS_FILE, json);
        }
    }

    fn save_markdown_report(&self, groups: &GroupManager) {
        let snapshot = self.snapshot(groups);
        let mut body = String::from("# 组合绩效报表\n\n");
        body.push_str("说明: 胜率仅统计已完整平仓的交易，未平仓浮盈浮亏不计入。\n\n");
        body.push_str("平局区间: -0.5% ~ +0.5%\n");

        for summary in snapshot {
            body.push_str(&format!(
                "\n## {} ({})\n\n- 已平仓: {}\n- 胜率: {:.1}%\n- 胜/负/平: {}/{}/{}\n- 平均PnL: {:+.2}%\n- 累计PnL: {:+.2}%\n- 最佳交易: {}\n- 最差交易: {}\n- 买入成功: {}\n- 买入失败: {}\n- 卖出失败: {}\n- 平均持仓: {}\n",
                summary.group_name,
                summary.group_id,
                summary.closed_trades,
                summary.win_rate_percent(),
                summary.wins,
                summary.losses,
                summary.flats,
                summary.avg_pnl_percent,
                summary.net_pnl_percent_sum,
                format_optional_percent(summary.best_trade_percent),
                format_optional_percent(summary.worst_trade_percent),
                summary.buy_confirmed_count,
                summary.buy_failed_count,
                summary.sell_failed_count,
                format_hold_seconds(summary.avg_hold_seconds.round() as u64),
            ));
        }

        body.push_str(&format!(
            "\n文件:\n- {}\n- {}\n- {}\n",
            GROUP_STATS_FILE, GROUP_TRADE_HISTORY_FILE, GROUP_STATS_REPORT_FILE
        ));

        let _ = fs::write(GROUP_STATS_REPORT_FILE, body);
    }

    fn snapshot(&self, groups: &GroupManager) -> Vec<GroupPerformanceSummary> {
        let current_groups = groups.all_groups();
        let current_group_ids: BTreeMap<String, String> = current_groups
            .iter()
            .map(|group| (group.id.clone(), group.name.clone()))
            .collect();

        let summaries = self.summaries.read().unwrap();
        let mut merged: BTreeMap<String, GroupPerformanceSummary> = BTreeMap::new();

        for (group_id, group_name) in current_group_ids {
            let summary = summaries
                .get(&group_id)
                .cloned()
                .map(|mut item| {
                    item.group_name = group_name.clone();
                    item
                })
                .unwrap_or_else(|| GroupPerformanceSummary::blank(&group_id, &group_name));
            merged.insert(group_id, summary);
        }

        for (group_id, summary) in summaries.iter() {
            merged
                .entry(group_id.clone())
                .or_insert_with(|| summary.clone());
        }

        let mut items: Vec<_> = merged.into_values().collect();
        items.sort_by(|a, b| {
            b.net_pnl_percent_sum
                .partial_cmp(&a.net_pnl_percent_sum)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    b.win_rate_percent()
                        .partial_cmp(&a.win_rate_percent())
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| b.closed_trades.cmp(&a.closed_trades))
        });
        items
    }
}

pub fn build_closed_trade_record(
    group_id: String,
    group_name: String,
    mint: String,
    token_name: String,
    buy_signature: String,
    sell_signature: String,
    exit_reason: String,
    entry_sol_amount: f64,
    pnl_percent: f64,
    hold_seconds: u64,
) -> ClosedTradeRecord {
    let closed_at_unix_secs = now_unix_secs();
    let opened_at_unix_secs = closed_at_unix_secs.saturating_sub(hold_seconds);
    ClosedTradeRecord {
        group_id,
        group_name,
        mint,
        token_name,
        buy_signature,
        sell_signature,
        exit_reason,
        entry_sol_amount,
        pnl_percent,
        hold_seconds,
        opened_at_unix_secs,
        closed_at_unix_secs,
        outcome: classify_trade_outcome(pnl_percent),
    }
}

fn classify_trade_outcome(pnl_percent: f64) -> TradeOutcome {
    if pnl_percent > WIN_THRESHOLD_PERCENT {
        TradeOutcome::Win
    } else if pnl_percent < -WIN_THRESHOLD_PERCENT {
        TradeOutcome::Loss
    } else {
        TradeOutcome::Flat
    }
}

fn format_optional_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:+.2}%", value))
        .unwrap_or_else(|| "-".to_string())
}

fn format_hold_seconds(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{:.1}h", seconds as f64 / 3600.0)
    } else if seconds >= 60 {
        format!("{:.1}m", seconds as f64 / 60.0)
    } else {
        format!("{}s", seconds)
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
