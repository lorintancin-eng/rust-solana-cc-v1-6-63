use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::autosell::{AutoSellManager, Position, SellReason, SellSignal};
use crate::config::{AppConfig, SELL_MODE_FOLLOW, SELL_MODE_TP_SL};
use crate::consensus::ConsensusEngine;
use crate::group_stats::{build_closed_trade_record, GroupPerformanceStore};
use crate::groups::{CopyGroup, GroupManager, ENTRY_MODE_SMART_BUY, ENTRY_MODE_SMART_SELL};
use crate::tx::sell_executor::SellExecutor;
use crate::utils::sol_price::SolUsdPrice;

pub enum TgEvent {
    ConsensusReached {
        group_name: String,
        mint: Pubkey,
        wallets: Vec<Pubkey>,
    },
    BuySubmitted {
        group_name: String,
        mint: Pubkey,
        sol_amount: f64,
        latency_ms: u64,
    },
    BuyConfirmed {
        group_id: String,
        group_name: String,
        mint: Pubkey,
        token_name: String,
        spent_sol: f64,
        cost_price_usd: String,
        mcap_usd: String,
    },
    BuyFailed {
        group_id: String,
        group_name: String,
        mint: Pubkey,
        reason: String,
    },
    SellSuccess {
        group_id: String,
        group_name: String,
        mint: Pubkey,
        token_name: String,
        reason: String,
        pnl_percent: f64,
        tx_sig: String,
        buy_sig: String,
        hold_seconds: u64,
        entry_sol_amount: f64,
        fully_closed: bool,
    },
    SellFailed {
        group_id: String,
        group_name: String,
        mint: Pubkey,
        reason: String,
    },
}

#[derive(Clone)]
pub struct TgNotifier {
    tx: mpsc::UnboundedSender<TgEvent>,
    enabled: bool,
}

impl TgNotifier {
    pub fn send(&self, event: TgEvent) {
        if self.enabled {
            let _ = self.tx.send(event);
        }
    }

    pub fn noop() -> Self {
        let (tx, _) = mpsc::unbounded_channel();
        Self { tx, enabled: false }
    }

    pub fn from_sender(tx: mpsc::UnboundedSender<TgEvent>) -> Self {
        Self { tx, enabled: true }
    }
}

pub struct TgStats {
    pub started_at: Instant,
    pub grpc_events: AtomicU64,
    pub buy_attempts: AtomicU64,
    pub buy_success: AtomicU64,
    pub buy_failed: AtomicU64,
}

impl TgStats {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            grpc_events: AtomicU64::new(0),
            buy_attempts: AtomicU64::new(0),
            buy_success: AtomicU64::new(0),
            buy_failed: AtomicU64::new(0),
        }
    }
}

pub struct TgBot {
    token: String,
    chat_id: String,
    http: reqwest::Client,
    auto_sell: Arc<AutoSellManager>,
    consensus: Arc<ConsensusEngine>,
    config: AppConfig,
    groups: Arc<GroupManager>,
    sell_signal_tx: mpsc::UnboundedSender<SellSignal>,
    _sell_executor: Arc<SellExecutor>,
    is_running: Arc<AtomicBool>,
    stats: Arc<TgStats>,
    _sol_usd: SolUsdPrice,
    performance: GroupPerformanceStore,
    event_rx: Option<mpsc::UnboundedReceiver<TgEvent>>,
}

impl TgBot {
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        config: AppConfig,
        groups: Arc<GroupManager>,
        auto_sell: Arc<AutoSellManager>,
        consensus: Arc<ConsensusEngine>,
        sell_signal_tx: mpsc::UnboundedSender<SellSignal>,
        sell_executor: Arc<SellExecutor>,
        is_running: Arc<AtomicBool>,
        stats: Arc<TgStats>,
        sol_usd: SolUsdPrice,
        event_rx: mpsc::UnboundedReceiver<TgEvent>,
    ) -> Self {
        Self {
            token: config.telegram_bot_token.clone().unwrap_or_default(),
            chat_id: config.telegram_chat_id.clone().unwrap_or_default(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("telegram client"),
            auto_sell,
            consensus,
            config,
            groups,
            sell_signal_tx,
            _sell_executor: sell_executor,
            is_running,
            stats,
            _sol_usd: sol_usd,
            performance: GroupPerformanceStore::load_or_default(),
            event_rx: Some(event_rx),
        }
    }

    pub async fn run(mut self) {
        let mut offset: i64 = 0;
        let mut event_rx = self.event_rx.take().expect("event_rx already taken");

        self.performance.sync_groups(&self.groups);
        self.set_bot_commands().await;
        self.send_msg_kb(
            "<b>Solana 跟单机器人已上线</b>\n\n使用下方菜单可快速查看组合、设置组合和启停监听。",
            group_menu_keyboard_v2(self.groups.zero_slot_buy_enabled()),
        )
        .await;

        loop {
            tokio::select! {
                Some(event) = event_rx.recv() => self.handle_event(event).await,
                result = self.get_updates(offset) => {
                    match result {
                        Ok(updates) => {
                            for update in updates {
                                let uid = update["update_id"].as_i64().unwrap_or(0);
                                if uid >= offset {
                                    offset = uid + 1;
                                }
                                self.handle_update(&update).await;
                            }
                        }
                        Err(err) => {
                            debug!("telegram getUpdates error: {}", err);
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        }
                    }
                }
            }
        }
    }

    async fn set_bot_commands(&self) {
        let url = format!("https://api.telegram.org/bot{}/setMyCommands", self.token);
        let body = json!({
            "commands": [
                {"command": "help", "description": "查看命令说明与操作提示"},
                {"command": "start", "description": "启动链上监听与跟单"},
                {"command": "stop", "description": "停止链上监听与跟单"},
                {"command": "status", "description": "查看运行状态与当前选中组合"},
                {"command": "groups", "description": "查看所有组合与快捷操作菜单"},
                {"command": "groupadd", "description": "新增组合，例如 /groupadd 组合1"},
                {"command": "groupdel", "description": "删除组合，例如 /groupdel g1"},
                {"command": "usegroup", "description": "切换当前组合，例如 /usegroup g1"},
                {"command": "groupon", "description": "启用组合，例如 /groupon g1"},
                {"command": "groupoff", "description": "停用组合，例如 /groupoff g1"},
                {"command": "set", "description": "设置当前组合参数，例如 /set buy 0.003"},
                {"command": "wallets", "description": "查看当前组合的钱包列表"},
                {"command": "addwallet", "description": "给当前组合添加钱包"},
                {"command": "rmwallet", "description": "从当前组合移除钱包"},
                {"command": "renamegroup", "description": "修改当前组合名称"},
                {"command": "buymode", "description": "切换跟单买入触发模式"},
                {"command": "sellmode", "description": "查看或切换卖出模式"},
                {"command": "pos", "description": "查看持仓列表并手动卖出，可带 group_id"},
                {"command": "sellall", "description": "手动卖出当前组合或指定组合持仓"},
                {"command": "stats", "description": "查看运行统计数据"},
                {"command": "gstats", "description": "查看组合绩效报表"}
            ]
        });
        let _ = self.http.post(&url).json(&body).send().await;
    }

    async fn get_updates(&self, offset: i64) -> anyhow::Result<Vec<serde_json::Value>> {
        let url = format!("https://api.telegram.org/bot{}/getUpdates", self.token);
        let resp: serde_json::Value = self
            .http
            .get(&url)
            .query(&[
                ("offset", offset.to_string()),
                ("timeout", "30".to_string()),
                (
                    "allowed_updates",
                    r#"["message","callback_query"]"#.to_string(),
                ),
            ])
            .send()
            .await?
            .json()
            .await?;

        if resp["ok"].as_bool() != Some(true) {
            anyhow::bail!("telegram getUpdates failed: {}", resp);
        }

        Ok(resp["result"].as_array().cloned().unwrap_or_default())
    }

    async fn send_msg(&self, text: &str) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        let body = json!({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });
        let _ = self.http.post(&url).json(&body).send().await;
    }

    async fn send_msg_kb(&self, text: &str, reply_markup: serde_json::Value) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        let body = json!({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
            "reply_markup": reply_markup,
        });
        let _ = self.http.post(&url).json(&body).send().await;
    }

    async fn edit_msg(&self, message_id: i64, text: &str, reply_markup: serde_json::Value) {
        let url = format!("https://api.telegram.org/bot{}/editMessageText", self.token);
        let body = json!({
            "chat_id": self.chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
            "reply_markup": reply_markup,
        });
        let _ = self.http.post(&url).json(&body).send().await;
    }

    async fn answer_cb(&self, callback_id: &str, text: Option<&str>) {
        let url = format!(
            "https://api.telegram.org/bot{}/answerCallbackQuery",
            self.token
        );
        let body = json!({
            "callback_query_id": callback_id,
            "text": text.unwrap_or(""),
            "show_alert": false,
        });
        let _ = self.http.post(&url).json(&body).send().await;
    }

    async fn handle_update(&self, update: &serde_json::Value) {
        if let Some(msg) = update.get("message") {
            self.handle_message(msg).await;
        } else if let Some(cb) = update.get("callback_query") {
            self.handle_callback_v2(cb).await;
        }
    }

    async fn handle_message(&self, msg: &serde_json::Value) {
        let chat_id = msg["chat"]["id"].as_i64().unwrap_or(0).to_string();
        if chat_id != self.chat_id {
            return;
        }

        let Some(text) = msg["text"].as_str() else {
            return;
        };

        let parts: Vec<&str> = text.split_whitespace().collect();
        let cmd = parts
            .first()
            .copied()
            .unwrap_or("")
            .split('@')
            .next()
            .unwrap_or("");

        match cmd {
            "/help" => self.cmd_help().await,
            "/start" => self.cmd_start().await,
            "/stop" => self.cmd_stop().await,
            "/status" => self.cmd_status().await,
            "/groups" => self.cmd_groups().await,
            "/groupadd" => self.cmd_groupadd(&parts[1..]).await,
            "/groupdel" => self.cmd_groupdel(&parts[1..]).await,
            "/usegroup" => self.cmd_usegroup(&parts[1..]).await,
            "/groupon" => self.cmd_group_enabled(&parts[1..], true).await,
            "/groupoff" => self.cmd_group_enabled(&parts[1..], false).await,
            "/set" => self.cmd_set(&parts[1..]).await,
            "/wallets" => self.cmd_wallets().await,
            "/addwallet" => self.cmd_addwallet(&parts[1..]).await,
            "/rmwallet" => self.cmd_rmwallet(&parts[1..]).await,
            "/renamegroup" => self.cmd_renamegroup(&parts[1..]).await,
            "/buymode" => self.cmd_buymode(&parts[1..]).await,
            "/sellmode" => self.cmd_sellmode(&parts[1..]).await,
            "/pos" => self.cmd_positions(&parts[1..]).await,
            "/sellall" => self.cmd_sellall(&parts[1..]).await,
            "/stats" => self.cmd_stats().await,
            "/gstats" => self.cmd_group_stats().await,
            _ => {}
        }
    }

    async fn handle_callback(&self, cb: &serde_json::Value) {
        let callback_id = cb["id"].as_str().unwrap_or_default();
        let chat_id = cb["message"]["chat"]["id"]
            .as_i64()
            .unwrap_or(0)
            .to_string();
        if chat_id != self.chat_id {
            return;
        }

        let message_id = cb["message"]["message_id"].as_i64().unwrap_or(0);
        let data = cb["data"].as_str().unwrap_or_default();
        let parts: Vec<&str> = data.split(':').collect();

        match parts.as_slice() {
            ["gm", "main"] => {
                self.answer_cb(callback_id, None).await;
                self.edit_msg(
                    message_id,
                    "<b>组合菜单</b>\n\n请选择要执行的操作。",
                    group_menu_keyboard_v2(self.groups.zero_slot_buy_enabled()),
                )
                .await;
            }
            ["gm", "toggle_zero_slot_buy"] => {
                if self.config.zero_slot_urls.is_empty() {
                    self.answer_cb(callback_id, Some("未配置 0slot 端点")).await;
                    return;
                }
                let enabled = self.groups.toggle_zero_slot_buy_enabled();
                self.answer_cb(
                    callback_id,
                    Some(if enabled {
                        "0slot 买入通道已开启"
                    } else {
                        "0slot 买入通道已关闭"
                    }),
                )
                .await;
                self.edit_msg(
                    message_id,
                    "<b>组合菜单</b>\n\n请选择要执行的操作。",
                    group_menu_keyboard_v2(enabled),
                )
                .await;
            }
            ["gm", "overview"] => {
                self.answer_cb(callback_id, None).await;
                self.performance.sync_groups(&self.groups);
                self.edit_msg(
                    message_id,
                    &format_groups_overview(&self.groups, self.groups.selected_group_id()),
                    groups_overview_keyboard(&self.groups),
                )
                .await;
            }
            ["gm", "perf"] => {
                self.answer_cb(callback_id, None).await;
                self.performance.sync_groups(&self.groups);
                self.edit_msg(
                    message_id,
                    &self.performance.render_overview_html(&self.groups),
                    group_performance_keyboard(),
                )
                .await;
            }
            ["gm", "positions"] => {
                self.answer_cb(callback_id, None).await;
                let positions = sorted_positions(self.auto_sell.get_active_positions());
                self.edit_msg(
                    message_id,
                    &format_positions_v2(&positions, self._sol_usd.get()),
                    positions_list_keyboard(&positions),
                )
                .await;
            }
            ["gm", "add"] => {
                let name = format!("组合{}", self.groups.all_groups().len() + 1);
                let group = self.groups.add_group(name, &self.config);
                self.answer_cb(callback_id, Some("已新增组合")).await;
                self.edit_msg(
                    message_id,
                    &format_group_detail_v2(&group, true),
                    group_detail_keyboard(&group),
                )
                .await;
            }
            ["gm", "pick", action] => {
                self.answer_cb(callback_id, None).await;
                self.edit_msg(
                    message_id,
                    &format!("请选择要执行“{}”的组合。", picker_action_label(action)),
                    group_picker_keyboard(&self.groups, action),
                )
                .await;
            }
            ["gm", "view", group_id] => {
                self.answer_cb(callback_id, None).await;
                if let Some(group) = self.groups.get_group(group_id) {
                    self.edit_msg(
                        message_id,
                        &format_group_detail_v2(
                            &group,
                            self.groups.selected_group_id().as_deref() == Some(group_id),
                        ),
                        group_detail_keyboard(&group),
                    )
                    .await;
                }
            }
            ["gm", "set", group_id] => {
                self.answer_cb(callback_id, None).await;
                if let Some(group) = self.groups.get_group(group_id) {
                    self.edit_msg(
                        message_id,
                        &format!(
                            "<b>组合参数设置</b>\n\n{}\n\n点击下方参数按钮可选择预设值，也可以先用 <code>/usegroup {}</code> 再执行 <code>/set key value</code> 输入自定义值。",
                            format_group_compact_v2(&group),
                            group.id,
                        ),
                        group_setting_menu_keyboard_v2(&group),
                    )
                    .await;
                }
            }
            ["gw", "a", group_id] => {
                self.answer_cb(callback_id, None).await;
                if let Some(group) = self.groups.get_group(group_id) {
                    self.edit_msg(
                        message_id,
                        &format!(
                            "<b>添加钱包</b>\n\n组合: <b>{}</b> ({})\n\n先执行 <code>/usegroup {}</code>\n再执行 <code>/addwallet 钱包地址</code>\n\n当前钱包数: {}",
                            group.name,
                            group.id,
                            group.id,
                            group.wallets.len(),
                        ),
                        group_wallet_hint_keyboard(&group),
                    )
                    .await;
                }
            }
            ["gw", "r", group_id] => {
                self.answer_cb(callback_id, None).await;
                if let Some(group) = self.groups.get_group(group_id) {
                    self.edit_msg(
                        message_id,
                        &format!(
                            "<b>删除钱包</b>\n\n组合: <b>{}</b> ({})\n请选择要删除的钱包。",
                            group.name, group.id
                        ),
                        group_wallet_remove_keyboard(&group),
                    )
                    .await;
                }
            }
            ["gw", "x", group_id, wallet_raw] => {
                self.answer_cb(callback_id, None).await;
                match Pubkey::from_str(wallet_raw) {
                    Ok(wallet) => match self.groups.remove_wallet(group_id, &wallet) {
                        Ok(()) => {
                            if let Some(group) = self.groups.get_group(group_id) {
                                self.edit_msg(
                                    message_id,
                                    &format_group_detail_v2(
                                        &group,
                                        self.groups.selected_group_id().as_deref()
                                            == Some(group_id),
                                    ),
                                    group_detail_keyboard(&group),
                                )
                                .await;
                            }
                        }
                        Err(err) => self.answer_cb(callback_id, Some(&err)).await,
                    },
                    Err(_) => self.answer_cb(callback_id, Some("钱包地址无效")).await,
                }
            }
            ["gn", group_id] => {
                self.answer_cb(callback_id, None).await;
                if let Some(group) = self.groups.get_group(group_id) {
                    self.edit_msg(
                        message_id,
                        &format!(
                            "<b>修改组合名称</b>\n\n当前组合: <b>{}</b> ({})\n\n先执行 <code>/usegroup {}</code>\n再执行 <code>/renamegroup 新名称</code>",
                            group.name, group.id, group.id
                        ),
                        group_rename_keyboard(&group),
                    )
                    .await;
                }
            }
            ["gm", "del", group_id] => match self.groups.delete_group(group_id) {
                Ok(()) => {
                    self.answer_cb(callback_id, Some("组合已删除")).await;
                    self.edit_msg(
                        message_id,
                        &format_groups_overview(&self.groups, self.groups.selected_group_id()),
                        groups_overview_keyboard(&self.groups),
                    )
                    .await;
                }
                Err(err) => {
                    self.answer_cb(callback_id, Some(&err)).await;
                }
            },
            ["gm", "on", group_id] => match self.groups.set_group_enabled(group_id, true) {
                Ok(()) => {
                    self.answer_cb(callback_id, Some("组合已启用")).await;
                    if let Some(group) = self.groups.get_group(group_id) {
                        self.edit_msg(
                            message_id,
                            &format_group_detail_v2(
                                &group,
                                self.groups.selected_group_id().as_deref() == Some(group_id),
                            ),
                            group_detail_keyboard(&group),
                        )
                        .await;
                    }
                }
                Err(err) => self.answer_cb(callback_id, Some(&err)).await,
            },
            ["gm", "off", group_id] => match self.groups.set_group_enabled(group_id, false) {
                Ok(()) => {
                    self.answer_cb(callback_id, Some("组合已停用")).await;
                    if let Some(group) = self.groups.get_group(group_id) {
                        self.edit_msg(
                            message_id,
                            &format_group_detail_v2(
                                &group,
                                self.groups.selected_group_id().as_deref() == Some(group_id),
                            ),
                            group_detail_keyboard(&group),
                        )
                        .await;
                    }
                }
                Err(err) => self.answer_cb(callback_id, Some(&err)).await,
            },
            ["gm", "use", group_id] => match self.groups.set_selected_group(group_id) {
                Ok(()) => {
                    self.answer_cb(callback_id, Some("已切换当前组合")).await;
                    if let Some(group) = self.groups.get_group(group_id) {
                        self.edit_msg(
                            message_id,
                            &format_group_detail_v2(&group, true),
                            group_detail_keyboard(&group),
                        )
                        .await;
                    }
                }
                Err(err) => self.answer_cb(callback_id, Some(&err)).await,
            },
            ["gm", "key", group_id, key] => {
                self.answer_cb(callback_id, None).await;
                if let Some(group) = self.groups.get_group(group_id) {
                    self.edit_msg(
                        message_id,
                        &format!(
                            "<b>设置组合参数</b>\n\n组合: <b>{}</b> ({})\n参数: <b>{}</b>\n当前值: {}\n\n请选择一个预设值。\n\n{}",
                            group.name,
                            group.id,
                            setting_label_v2(key),
                            group_value_text(&group, key),
                            setting_custom_hint(&group, key),
                        ),
                        group_setting_value_keyboard(group_id, key),
                    )
                    .await;
                }
            }
            ["gm", "val", group_id, key, value] => {
                self.answer_cb(callback_id, Some("参数已更新")).await;
                if let Some(mut group) = self.groups.get_group(group_id) {
                    match apply_group_setting_value(&mut group, key, value) {
                        Ok(message) => {
                            self.groups.replace_group(group.clone());
                            self.edit_msg(
                                message_id,
                                &format!(
                                    "<b>参数更新成功</b>\n\n{}\n\n{}",
                                    message,
                                    format_group_compact_v2(&group),
                                ),
                                group_setting_menu_keyboard_v2(&group),
                            )
                            .await;
                        }
                        Err(err) => {
                            self.edit_msg(
                                message_id,
                                &format!(
                                    "<b>参数更新失败</b>\n\n{}\n\n请选择其他预设值，或使用 <code>/usegroup {}</code> 后执行 <code>/set {} 自定义值</code>。",
                                    err, group.id, key,
                                ),
                                group_setting_value_keyboard(group_id, key),
                            )
                            .await;
                        }
                    }
                }
            }
            _ => {
                warn!("unknown telegram callback: {}", data);
                self.answer_cb(callback_id, Some("未识别的操作")).await;
            }
        }
    }

    async fn handle_callback_v2(&self, cb: &serde_json::Value) {
        let chat_id = cb["message"]["chat"]["id"]
            .as_i64()
            .unwrap_or(0)
            .to_string();
        if chat_id != self.chat_id {
            return;
        }

        let callback_id = cb["id"].as_str().unwrap_or_default();
        let message_id = cb["message"]["message_id"].as_i64().unwrap_or(0);
        let data = cb["data"].as_str().unwrap_or_default();
        let parts: Vec<&str> = data.split(':').collect();

        match parts.as_slice() {
            ["gm", "positions"] => {
                self.answer_cb(callback_id, None).await;
                let positions = sorted_positions(self.auto_sell.get_active_positions());
                self.edit_msg(
                    message_id,
                    &format_positions_v2(&positions, self._sol_usd.get()),
                    positions_list_keyboard(&positions),
                )
                .await;
            }
            ["ps", "list"] => {
                self.answer_cb(callback_id, None).await;
                let positions = sorted_positions(self.auto_sell.get_active_positions());
                self.edit_msg(
                    message_id,
                    &format_positions_v2(&positions, self._sol_usd.get()),
                    positions_list_keyboard(&positions),
                )
                .await;
            }
            ["ps", "view", group_id, mint_raw] => {
                self.answer_cb(callback_id, None).await;
                match Pubkey::from_str(mint_raw) {
                    Ok(mint) => {
                        if let Some(position) =
                            self.auto_sell.get_position_by_group_mint(group_id, &mint)
                        {
                            self.edit_msg(
                                message_id,
                                &format_position_detail(&position, self._sol_usd.get()),
                                position_detail_keyboard(&position),
                            )
                            .await;
                        } else {
                            let positions = sorted_positions(self.auto_sell.get_active_positions());
                            self.edit_msg(
                                message_id,
                                &format_positions_v2(&positions, self._sol_usd.get()),
                                positions_list_keyboard(&positions),
                            )
                            .await;
                        }
                    }
                    Err(_) => {
                        self.answer_cb(callback_id, Some("代币地址无效")).await;
                    }
                }
            }
            ["ps", "sell", group_id, mint_raw, percent_raw] => {
                let mint = match Pubkey::from_str(mint_raw) {
                    Ok(mint) => mint,
                    Err(_) => {
                        self.answer_cb(callback_id, Some("代币地址无效")).await;
                        return;
                    }
                };
                let percent = match percent_raw.parse::<u32>() {
                    Ok(percent @ 1..=100) => percent,
                    _ => {
                        self.answer_cb(callback_id, Some("卖出比例无效")).await;
                        return;
                    }
                };

                self.answer_cb(callback_id, Some("卖出指令已发送")).await;
                self._sell_executor
                    .handle_partial_sell(group_id, &mint, percent)
                    .await;

                if let Some(position) = self.auto_sell.get_position_by_group_mint(group_id, &mint) {
                    self.edit_msg(
                        message_id,
                        &format_position_detail(&position, self._sol_usd.get()),
                        position_detail_keyboard(&position),
                    )
                    .await;
                } else {
                    let positions = sorted_positions(self.auto_sell.get_active_positions());
                    self.edit_msg(
                        message_id,
                        &format_positions_v2(&positions, self._sol_usd.get()),
                        positions_list_keyboard(&positions),
                    )
                    .await;
                }
            }
            _ => self.handle_callback(cb).await,
        }
    }

    async fn cmd_help(&self) {
        self.send_msg_kb(
            "<b>命令说明</b>\n\n\
<code>/start</code> 启动监听与跟单\n\
<code>/stop</code> 停止监听与跟单\n\
<code>/status</code> 查看运行状态\n\
<code>/groups</code> 查看所有组合与快捷菜单\n\
<code>/groupadd 名称</code> 新增组合\n\
<code>/groupdel g1</code> 删除组合\n\
<code>/usegroup g1</code> 切换当前组合\n\
<code>/groupon g1</code> 启用组合\n\
<code>/groupoff g1</code> 停用组合\n\
<code>/set buy 0.003</code> 设置当前组合参数\n\
<code>/wallets</code> 查看当前组合钱包\n\
<code>/addwallet 地址</code> 添加钱包\n\
<code>/rmwallet 地址</code> 移除钱包\n\
<code>/renamegroup 新名称</code> 修改当前组合名称\n\
<code>/buymode buy|sell</code> 切换跟单买入模式\n\
<code>/sellmode follow|tp_sl</code> 切换卖出模式\n\
<code>/pos [group_id]</code> 查看持仓\n\
<code>/sellall [group_id]</code> 手动全卖\n\
<code>/stats</code> 查看运行统计\n\
<code>/gstats</code> 查看组合绩效\n\n\
支持快捷设置的参数键：<code>buy</code>、<code>min_buy</code>、<code>tp</code>、<code>sl</code>、<code>trailing</code>、<code>slippage</code>、<code>sell_slippage</code>、<code>consensus</code>、<code>hold</code>、<code>tip_buy</code>、<code>tip_sell</code>、<code>zero_slot_tip</code>、<code>buy_mode</code>、<code>mode</code>、<code>enabled</code>",
            group_menu_keyboard_v2(self.groups.zero_slot_buy_enabled()),
        )
        .await;
    }

    async fn cmd_start(&self) {
        self.is_running.store(true, Ordering::Relaxed);
        self.send_msg("已启动监听与跟单。").await;
    }

    async fn cmd_stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        self.send_msg("已停止监听与跟单。").await;
    }

    async fn cmd_status(&self) {
        let mut text = format!(
            "<b>运行状态</b>\n\n状态: <b>{}</b>\n组合数: {}\n开放持仓: {}\n待共识信号: {}",
            if self.is_running.load(Ordering::Relaxed) {
                "RUNNING"
            } else {
                "STOPPED"
            },
            self.groups.all_groups().len(),
            self.auto_sell.get_active_positions().len(),
            self.consensus.pending_count(),
        );

        text.push_str(&format!(
            "\n0slot 买入通道: {}",
            zero_slot_buy_status_label(self.groups.zero_slot_buy_enabled())
        ));

        if let Some(group) = self.groups.selected_group() {
            text.push_str("\n\n");
            text.push_str(&format_group_detail_v2(&group, true));
        }

        self.send_msg(&text).await;
    }

    async fn cmd_groups(&self) {
        self.performance.sync_groups(&self.groups);
        self.send_msg_kb(
            &format_groups_overview(&self.groups, self.groups.selected_group_id()),
            groups_overview_keyboard(&self.groups),
        )
        .await;
    }

    async fn cmd_groupadd(&self, args: &[&str]) {
        let name = if args.is_empty() {
            format!("组合{}", self.groups.all_groups().len() + 1)
        } else {
            args.join(" ")
        };

        let group = self.groups.add_group(name, &self.config);
        self.send_msg_kb(
            &format!(
                "<b>组合已创建</b>\n\n{}",
                format_group_detail_v2(&group, true)
            ),
            group_detail_keyboard(&group),
        )
        .await;
    }

    async fn cmd_groupdel(&self, args: &[&str]) {
        let Some(group_id) = args.first() else {
            self.send_msg_kb(
                "请选择要删除的组合。",
                group_picker_keyboard(&self.groups, "del"),
            )
            .await;
            return;
        };

        match self.groups.delete_group(group_id) {
            Ok(()) => self.send_msg("组合已删除。").await,
            Err(err) => self.send_msg(&err).await,
        }
    }

    async fn cmd_usegroup(&self, args: &[&str]) {
        let Some(group_id) = args.first() else {
            self.send_msg_kb(
                "请选择要切换的组合。",
                group_picker_keyboard(&self.groups, "use"),
            )
            .await;
            return;
        };

        match self.groups.set_selected_group(group_id) {
            Ok(()) => self.send_msg("当前组合已切换。").await,
            Err(err) => self.send_msg(&err).await,
        }
    }

    async fn cmd_group_enabled(&self, args: &[&str], enabled: bool) {
        let Some(group_id) = args.first() else {
            let action = if enabled { "on" } else { "off" };
            let text = if enabled {
                "请选择要启用的组合。"
            } else {
                "请选择要停用的组合。"
            };
            self.send_msg_kb(text, group_picker_keyboard(&self.groups, action))
                .await;
            return;
        };

        match self.groups.set_group_enabled(group_id, enabled) {
            Ok(()) => {
                self.send_msg(if enabled {
                    "组合已启用。"
                } else {
                    "组合已停用。"
                })
                .await;
            }
            Err(err) => self.send_msg(&err).await,
        }
    }

    async fn cmd_set(&self, args: &[&str]) {
        let Some(mut group) = self.groups.selected_group() else {
            self.send_msg("当前没有选中的组合。").await;
            return;
        };

        if args.is_empty() {
            self.send_msg_kb(
                &format!(
                    "<b>当前组合参数</b>\n\n{}",
                    format_group_detail_v2(&group, true)
                ),
                group_setting_menu_keyboard_v2(&group),
            )
            .await;
            return;
        }

        if args.len() < 2 {
            self.send_msg("用法: /set <key> <value>").await;
            return;
        }

        let key = args[0].to_ascii_lowercase();
        let value = if args.len() >= 3 && args[1] == "=" {
            args[2]
        } else {
            args[1]
        };

        match apply_group_setting_value(&mut group, &key, value) {
            Ok(message) => {
                self.groups.replace_group(group);
                self.send_msg(&message).await;
            }
            Err(err) => self.send_msg(&err).await,
        }
    }

    async fn cmd_wallets(&self) {
        let Some(group) = self.groups.selected_group() else {
            self.send_msg("当前没有选中的组合。").await;
            return;
        };

        let mut text = format!("<b>{}</b> ({})", group.name, group.id);
        if group.wallets.is_empty() {
            text.push_str("\n暂无钱包。");
        } else {
            for (index, wallet) in group.wallets.iter().enumerate() {
                text.push_str(&format!("\n{}. <code>{}</code>", index + 1, wallet));
            }
        }

        self.send_msg(&text).await;
    }

    async fn cmd_addwallet(&self, args: &[&str]) {
        let Some(group) = self.groups.selected_group() else {
            self.send_msg("当前没有选中的组合。").await;
            return;
        };

        let Some(raw_wallet) = args.first() else {
            self.send_msg("用法: /addwallet <pubkey>").await;
            return;
        };

        match Pubkey::from_str(raw_wallet) {
            Ok(wallet) => match self.groups.add_wallet(&group.id, wallet) {
                Ok(()) => {
                    self.send_msg("钱包已加入组合，监控订阅将在几百毫秒内自动更新。")
                        .await
                }
                Err(err) => self.send_msg(&err).await,
            },
            Err(_) => self.send_msg("钱包地址格式无效。").await,
        }
    }

    async fn cmd_rmwallet(&self, args: &[&str]) {
        let Some(group) = self.groups.selected_group() else {
            self.send_msg("当前没有选中的组合。").await;
            return;
        };

        let Some(raw_wallet) = args.first() else {
            self.send_msg("用法: /rmwallet <pubkey>").await;
            return;
        };

        match Pubkey::from_str(raw_wallet) {
            Ok(wallet) => match self.groups.remove_wallet(&group.id, &wallet) {
                Ok(()) => {
                    self.send_msg("钱包已移出组合，监控订阅将在几百毫秒内自动更新。")
                        .await
                }
                Err(err) => self.send_msg(&err).await,
            },
            Err(_) => self.send_msg("钱包地址格式无效。").await,
        }
    }

    async fn cmd_renamegroup(&self, args: &[&str]) {
        let Some(group) = self.groups.selected_group() else {
            self.send_msg("当前没有选中的组合。").await;
            return;
        };

        if args.is_empty() {
            self.send_msg("用法: /renamegroup <新名称>").await;
            return;
        }

        match self.groups.rename_group(&group.id, args.join(" ")) {
            Ok(()) => {
                if let Some(updated) = self.groups.get_group(&group.id) {
                    self.send_msg_kb(
                        &format!(
                            "<b>组合名称已更新</b>\n\n{}",
                            format_group_detail_v2(&updated, true)
                        ),
                        group_detail_keyboard(&updated),
                    )
                    .await;
                }
            }
            Err(err) => self.send_msg(&err).await,
        }
    }

    async fn cmd_buymode(&self, args: &[&str]) {
        let Some(mut group) = self.groups.selected_group() else {
            self.send_msg("当前没有选中的组合。").await;
            return;
        };

        if args.is_empty() {
            self.send_msg(&format!(
                "当前组合 <b>{}</b> 的跟单买入模式：<b>{}</b>",
                group.name,
                entry_mode_label(group.entry_mode),
            ))
            .await;
            return;
        }

        match parse_entry_mode(args[0]) {
            Ok(mode) => {
                group.entry_mode = mode;
                self.groups.replace_group(group.clone());
                self.send_msg(&format!(
                    "组合 <b>{}</b> 的跟单买入模式已更新为：<b>{}</b>",
                    group.name,
                    entry_mode_label(mode),
                ))
                .await;
            }
            Err(err) => self.send_msg(&err).await,
        }
    }

    async fn cmd_sellmode(&self, args: &[&str]) {
        let Some(mut group) = self.groups.selected_group() else {
            self.send_msg("当前没有选中的组合。").await;
            return;
        };

        if args.is_empty() {
            self.send_msg(&format!(
                "当前组合 <b>{}</b> 的卖出模式: <b>{}</b>",
                group.name,
                sell_mode_label(group.sell_mode),
            ))
            .await;
            return;
        }

        match parse_sell_mode(args[0]) {
            Ok(mode) => {
                group.sell_mode = mode;
                self.groups.replace_group(group.clone());
                self.send_msg(&format!(
                    "组合 <b>{}</b> 卖出模式已更新为 <b>{}</b>",
                    group.name,
                    sell_mode_label(mode),
                ))
                .await;
            }
            Err(err) => self.send_msg(&err).await,
        }
    }

    async fn cmd_positions(&self, args: &[&str]) {
        let positions = if let Some(group_id) = args.first() {
            self.auto_sell.get_group_positions(group_id)
        } else {
            self.auto_sell.get_active_positions()
        };

        if positions.is_empty() {
            self.send_msg("当前没有持仓。").await;
            return;
        }

        let positions = sorted_positions(positions);
        self.send_msg_kb(
            &format_positions_v2(&positions, self._sol_usd.get()),
            positions_list_keyboard(&positions),
        )
        .await;
    }

    async fn cmd_sellall(&self, args: &[&str]) {
        let positions = if let Some(group_id) = args.first() {
            self.auto_sell.get_group_positions(group_id)
        } else {
            self.auto_sell.get_active_positions()
        };

        if positions.is_empty() {
            self.send_msg("没有可卖出的持仓。").await;
            return;
        }

        for pos in positions {
            let _ = self.sell_signal_tx.send(SellSignal {
                position_key: pos.key(),
                group_name: pos.group.name.clone(),
                reason: SellReason::Manual,
                current_price: pos.current_price,
                pnl_percent: pos.pnl_percent(),
            });
        }

        self.send_msg("手动卖出信号已加入队列。").await;
    }

    async fn cmd_stats(&self) {
        let text = format!(
            "<b>运行统计</b>\n\n运行时长: {}\ngRPC 事件: {}\n买入尝试: {}\n买入成功: {}\n买入失败: {}\n持仓记录数: {}",
            fmt_time(self.stats.started_at.elapsed().as_secs()),
            self.stats.grpc_events.load(Ordering::Relaxed),
            self.stats.buy_attempts.load(Ordering::Relaxed),
            self.stats.buy_success.load(Ordering::Relaxed),
            self.stats.buy_failed.load(Ordering::Relaxed),
            self.auto_sell.position_count(),
        );
        self.send_msg(&text).await;
    }

    async fn cmd_group_stats(&self) {
        self.performance.sync_groups(&self.groups);
        self.send_msg_kb(
            &self.performance.render_overview_html(&self.groups),
            group_performance_keyboard(),
        )
        .await;
    }

    async fn handle_event(&self, event: TgEvent) {
        let text = match event {
            TgEvent::ConsensusReached {
                group_name,
                mint,
                wallets,
            } => {
                let wallets = wallets
                    .iter()
                    .map(short_pubkey)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "<b>组合共识达成</b>\n\n组合: <b>{}</b>\n代币: <code>{}</code>\n钱包: {}",
                    group_name, mint, wallets
                )
            }
            TgEvent::BuySubmitted {
                group_name,
                mint,
                sol_amount,
                latency_ms,
            } => format!(
                "<b>买入已提交</b>\n\n组合: <b>{}</b>\n代币地址: <code>{}</code>\n金额: {:.4} SOL\n耗时: {}ms",
                group_name,
                mint,
                sol_amount,
                latency_ms,
            ),
            TgEvent::BuyConfirmed {
                group_id,
                group_name,
                mint,
                token_name,
                spent_sol,
                cost_price_usd,
                mcap_usd,
            } => {
                self.stats.buy_success.fetch_add(1, Ordering::Relaxed);
                self.performance
                    .record_buy_confirmed(&self.groups, &group_id, &group_name);
                let text = format!(
                    "<b>买入确认成功</b>\n\n组合: <b>{}</b>\n代币: {}\n代币地址: <code>{}</code>\n花费: {:.4} SOL\n成本价: {}\n市值: {}",
                    group_name, token_name, mint, spent_sol, cost_price_usd, mcap_usd
                );
                if let Some(position) = self.auto_sell.get_position_by_group_mint(&group_id, &mint)
                {
                    self.send_msg_kb(&text, position_detail_keyboard(&position))
                        .await;
                    String::new()
                } else {
                    text
                }
            }
            TgEvent::BuyFailed {
                group_id,
                group_name,
                mint,
                reason,
            } => {
                self.stats.buy_failed.fetch_add(1, Ordering::Relaxed);
                self.performance
                    .record_buy_failed(&self.groups, &group_id, &group_name);
                format!(
                    "<b>买入失败</b>\n\n组合: <b>{}</b>\n代币地址: <code>{}</code>\n原因: {}",
                    group_name,
                    mint,
                    reason,
                )
            }
            TgEvent::SellSuccess {
                group_id,
                group_name,
                mint,
                token_name,
                reason,
                pnl_percent,
                tx_sig,
                buy_sig,
                hold_seconds,
                entry_sol_amount,
                fully_closed,
            } => {
                if fully_closed {
                    let record = build_closed_trade_record(
                        group_id.clone(),
                        group_name.clone(),
                        mint.to_string(),
                        token_name.clone(),
                        buy_sig,
                        tx_sig.clone(),
                        reason.clone(),
                        entry_sol_amount,
                        pnl_percent,
                        hold_seconds,
                    );
                    self.performance.record_closed_trade(&self.groups, record);
                }
                format!(
                    "<b>卖出成功</b>\n\n组合: <b>{}</b>\n代币: {}\n代币地址: <code>{}</code>\n原因: {}\nPnL: {:+.2}%\n<a href=\"https://solscan.io/tx/{}\">查看链上交易</a>",
                    group_name, token_name, mint, reason, pnl_percent, tx_sig
                )
            }
            TgEvent::SellFailed {
                group_id,
                group_name,
                mint,
                reason,
            } => {
                self.performance
                    .record_sell_failed(&self.groups, &group_id, &group_name);
                format!(
                    "<b>卖出失败</b>\n\n组合: <b>{}</b>\n代币地址: <code>{}</code>\n原因: {}",
                    group_name,
                    mint,
                    reason,
                )
            }
        };

        if !text.is_empty() {
            self.send_msg(&text).await;
        }
    }
}

fn apply_group_setting_value(
    group: &mut CopyGroup,
    key: &str,
    raw_value: &str,
) -> Result<String, String> {
    let value = raw_value.trim().trim_end_matches('%');
    match key {
        "buy" => value
            .parse::<f64>()
            .map(|v| {
                group.buy_sol_amount = v;
                format!("买入金额 = {} SOL", v)
            })
            .map_err(|err| err.to_string()),
        "min_buy" => value
            .parse::<f64>()
            .map(|v| {
                group.min_target_buy_sol = v;
                format!("最小触发买入 = {} SOL", v)
            })
            .map_err(|err| err.to_string()),
        "tp" => value
            .parse::<f64>()
            .map(|v| {
                group.take_profit_percent = v;
                format!("止盈比例 = {}%", v)
            })
            .map_err(|err| err.to_string()),
        "sl" => value
            .parse::<f64>()
            .map(|v| {
                group.stop_loss_percent = v.abs();
                format!("止损比例 = {}%", v.abs())
            })
            .map_err(|err| err.to_string()),
        "trailing" => value
            .parse::<f64>()
            .map(|v| {
                group.trailing_stop_percent = v;
                format!("移动止损 = {}%", v)
            })
            .map_err(|err| err.to_string()),
        "slippage" => value
            .parse::<u64>()
            .map(|v| {
                group.slippage_bps = v;
                format!("买入滑点 = {} bps", v)
            })
            .map_err(|err| err.to_string()),
        "sell_slippage" => value
            .parse::<u64>()
            .map(|v| {
                group.sell_slippage_bps = v;
                format!("卖出滑点 = {} bps", v)
            })
            .map_err(|err| err.to_string()),
        "consensus" => value
            .parse::<usize>()
            .map(|v| {
                group.consensus_min_wallets = v.max(1);
                format!("共识数量 = {}", group.consensus_min_wallets)
            })
            .map_err(|err| err.to_string()),
        "hold" => value
            .parse::<u64>()
            .map(|v| {
                group.max_hold_seconds = v * 60;
                format!("hold = {} 分钟", v)
            })
            .map_err(|err| err.to_string()),
        "tip_buy" => value
            .parse::<u64>()
            .map(|v| {
                group.tip_buy_lamports = v;
                format!("买入小费 = {} lamports", v)
            })
            .map_err(|err| err.to_string()),
        "tip_sell" => value
            .parse::<u64>()
            .map(|v| {
                group.tip_sell_lamports = v;
                format!("卖出小费 = {} lamports", v)
            })
            .map_err(|err| err.to_string()),
        "zero_slot_tip" | "zero_slot_tip_buy" | "tip_0slot" => value
            .parse::<u64>()
            .map(|v| {
                group.zero_slot_tip_lamports = v;
                format!("0slot 小费 = {} lamports", v)
            })
            .map_err(|err| err.to_string()),
        "buy_mode" | "entry_mode" => parse_entry_mode(value).map(|mode| {
            group.entry_mode = mode;
            format!("跟单买入模式 = {}", entry_mode_label(mode))
        }),
        "mode" | "sell_mode" => parse_sell_mode(value).map(|mode| {
            group.sell_mode = mode;
            format!("卖出模式 = {}", sell_mode_label(mode))
        }),
        "enabled" => parse_bool_flag(value).map(|enabled| {
            group.enabled = enabled;
            format!("状态 = {}", if enabled { "启用" } else { "停用" })
        }),
        _ => Err(format!("未知参数键: {}", key)),
    }
}

fn parse_entry_mode(value: &str) -> Result<u8, String> {
    match value.to_ascii_lowercase().as_str() {
        "buy" | "smart_buy" | "buy_follow" | "buy-follow" => Ok(ENTRY_MODE_SMART_BUY),
        "sell" | "smart_sell" | "sell_buy" | "sell-follow" | "sell_follow" => {
            Ok(ENTRY_MODE_SMART_SELL)
        }
        _ => Err("跟单买入模式无效，请使用 buy 或 sell。".to_string()),
    }
}

fn parse_sell_mode(value: &str) -> Result<u8, String> {
    match value.to_ascii_lowercase().as_str() {
        "follow" | "follow_sell" | "follow-sell" => Ok(SELL_MODE_FOLLOW),
        "tp" | "sl" | "tp_sl" | "tpsl" | "tp-sl" => Ok(SELL_MODE_TP_SL),
        _ => Err("卖出模式无效，请使用 follow 或 tp_sl。".to_string()),
    }
}

fn parse_bool_flag(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "on" | "true" | "yes" => Ok(true),
        "0" | "off" | "false" | "no" => Ok(false),
        _ => Err("布尔值无效，请使用 on/off。".to_string()),
    }
}

fn sell_mode_label(mode: u8) -> &'static str {
    if mode == SELL_MODE_FOLLOW {
        "跟卖模式"
    } else {
        "止盈止损模式"
    }
}

fn entry_mode_label(mode: u8) -> &'static str {
    if mode == ENTRY_MODE_SMART_SELL {
        "卖出时跟单买入"
    } else {
        "买入时跟单买入"
    }
}

fn picker_action_label(action: &str) -> &'static str {
    match action {
        "view" => "查看组合",
        "set" => "设置组合",
        "del" => "删除组合",
        "on" => "启用组合",
        "off" => "停用组合",
        "use" => "切换组合",
        _ => "组合操作",
    }
}

fn setting_label(key: &str) -> &'static str {
    match key {
        "buy" => "买入金额",
        "min_buy" => "最小触发买入额",
        "tp" => "止盈比例",
        "sl" => "止损比例",
        "trailing" => "移动止损",
        "slippage" => "买入滑点",
        "sell_slippage" => "卖出滑点",
        "consensus" => "共识数量",
        "hold" => "最大持仓时间",
        "tip_buy" => "买入小费",
        "tip_sell" => "卖出小费",
        "mode" => "卖出模式",
        _ => "参数",
    }
}

fn setting_custom_hint(group: &CopyGroup, key: &str) -> String {
    let example_value = match key {
        "buy" => "0.006",
        "min_buy" => "0.5",
        "tp" => "50",
        "sl" => "15",
        "trailing" => "5",
        "slippage" => "3000",
        "sell_slippage" => "3000",
        "consensus" => "2",
        "hold" => "10",
        "tip_buy" => "10000",
        "tip_sell" => "10000",
        "zero_slot_tip" => "1000000",
        "buy_mode" => "buy",
        "mode" => "follow",
        "enabled" => "on",
        _ => "value",
    };

    format!(
        "也支持自定义输入。\n先执行 <code>/usegroup {}</code>\n再执行 <code>/set {} {}</code>",
        group.id, key, example_value
    )
}

fn group_menu_keyboard() -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "查看组合", "callback_data": "gm:overview"},
                {"text": "新增组合", "callback_data": "gm:add"}
            ],
            [
                {"text": "删除组合", "callback_data": "gm:pick:del"},
                {"text": "启用组合", "callback_data": "gm:pick:on"}
            ],
            [
                {"text": "停用组合", "callback_data": "gm:pick:off"},
                {"text": "设置组合", "callback_data": "gm:pick:set"}
            ],
            [
                {"text": "切换当前组合", "callback_data": "gm:pick:use"}
            ],
            [
                {"text": "切换 0slot 买入", "callback_data": "gm:toggle_zero_slot_buy"}
            ]
        ]
    })
}

fn groups_overview_keyboard(groups: &GroupManager) -> serde_json::Value {
    let mut rows = Vec::new();
    for group in groups.all_groups() {
        rows.push(json!([
            {"text": format!("查看 {} ({})", group.name, group.id), "callback_data": format!("gm:view:{}", group.id)},
            {"text": "设置", "callback_data": format!("gm:set:{}", group.id)}
        ]));
    }
    rows.push(json!([
        {"text": "组合绩效", "callback_data": "gm:perf"}
    ]));
    rows.push(json!([
        {"text": "新增组合", "callback_data": "gm:add"},
        {"text": "菜单", "callback_data": "gm:main"}
    ]));
    json!({ "inline_keyboard": rows })
}

fn group_picker_keyboard(groups: &GroupManager, action: &str) -> serde_json::Value {
    let mut rows = Vec::new();
    for group in groups.all_groups() {
        rows.push(json!([
            {
                "text": format!("{} ({})", group.name, group.id),
                "callback_data": format!("gm:{}:{}", action, group.id)
            }
        ]));
    }
    rows.push(json!([
        {"text": "返回菜单", "callback_data": "gm:main"}
    ]));
    json!({ "inline_keyboard": rows })
}

fn group_detail_keyboard(group: &CopyGroup) -> serde_json::Value {
    let toggle_text = if group.enabled {
        "停用组合"
    } else {
        "启用组合"
    };
    let toggle_cb = if group.enabled {
        format!("gm:off:{}", group.id)
    } else {
        format!("gm:on:{}", group.id)
    };

    json!({
        "inline_keyboard": [
            [
                {"text": "设置参数", "callback_data": format!("gm:set:{}", group.id)},
                {"text": "设为当前组合", "callback_data": format!("gm:use:{}", group.id)}
            ],
            [
                {"text": "添加钱包", "callback_data": format!("gw:a:{}", group.id)},
                {"text": "删除钱包", "callback_data": format!("gw:r:{}", group.id)}
            ],
            [
                {"text": "修改名称", "callback_data": format!("gn:{}", group.id)},
                {"text": toggle_text, "callback_data": toggle_cb}
            ],
            [
                {"text": "删除组合", "callback_data": format!("gm:del:{}", group.id)}
            ],
            [
                {"text": "查看全部组合", "callback_data": "gm:overview"},
                {"text": "返回菜单", "callback_data": "gm:main"}
            ]
        ]
    })
}

fn group_setting_menu_keyboard(group: &CopyGroup) -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "买入金额", "callback_data": format!("gm:key:{}:buy", group.id)},
                {"text": "最小买入", "callback_data": format!("gm:key:{}:min_buy", group.id)}
            ],
            [
                {"text": "止盈", "callback_data": format!("gm:key:{}:tp", group.id)},
                {"text": "止损", "callback_data": format!("gm:key:{}:sl", group.id)}
            ],
            [
                {"text": "移动止损", "callback_data": format!("gm:key:{}:trailing", group.id)},
                {"text": "共识数量", "callback_data": format!("gm:key:{}:consensus", group.id)}
            ],
            [
                {"text": "买入滑点", "callback_data": format!("gm:key:{}:slippage", group.id)},
                {"text": "卖出滑点", "callback_data": format!("gm:key:{}:sell_slippage", group.id)}
            ],
            [
                {"text": "持仓时间", "callback_data": format!("gm:key:{}:hold", group.id)},
                {"text": "卖出模式", "callback_data": format!("gm:key:{}:mode", group.id)}
            ],
            [
                {"text": "买入小费", "callback_data": format!("gm:key:{}:tip_buy", group.id)},
                {"text": "卖出小费", "callback_data": format!("gm:key:{}:tip_sell", group.id)}
            ],
            [
                {"text": "查看组合", "callback_data": format!("gm:view:{}", group.id)},
                {"text": "返回菜单", "callback_data": "gm:main"}
            ]
        ]
    })
}

fn group_setting_value_keyboard(group_id: &str, key: &str) -> serde_json::Value {
    let rows = match key {
        "buy" => vec![
            value_row(
                group_id,
                key,
                &[("0.003 SOL", "0.003"), ("0.005 SOL", "0.005")],
            ),
            value_row(group_id, key, &[("0.01 SOL", "0.01"), ("0.02 SOL", "0.02")]),
        ],
        "min_buy" => vec![
            value_row(group_id, key, &[("0.1 SOL", "0.1"), ("0.3 SOL", "0.3")]),
            value_row(group_id, key, &[("0.5 SOL", "0.5"), ("1 SOL", "1.0")]),
        ],
        "tp" => vec![
            value_row(group_id, key, &[("30%", "30"), ("50%", "50")]),
            value_row(group_id, key, &[("100%", "100"), ("1000%", "1000")]),
        ],
        "sl" => vec![
            value_row(group_id, key, &[("10%", "10"), ("15%", "15")]),
            value_row(group_id, key, &[("20%", "20"), ("30%", "30")]),
        ],
        "trailing" => vec![
            value_row(group_id, key, &[("0%", "0"), ("5%", "5")]),
            value_row(group_id, key, &[("10%", "10"), ("15%", "15")]),
        ],
        "consensus" => vec![
            value_row(group_id, key, &[("1", "1"), ("2", "2")]),
            value_row(group_id, key, &[("3", "3"), ("4", "4")]),
        ],
        "slippage" | "sell_slippage" => vec![
            value_row(group_id, key, &[("2000 bps", "2000"), ("3000 bps", "3000")]),
            value_row(group_id, key, &[("5000 bps", "5000"), ("8000 bps", "8000")]),
        ],
        "hold" => vec![
            value_row(group_id, key, &[("5 min", "5"), ("10 min", "10")]),
            value_row(group_id, key, &[("20 min", "20"), ("30 min", "30")]),
        ],
        "tip_buy" | "tip_sell" => vec![
            value_row(group_id, key, &[("10000", "10000"), ("50000", "50000")]),
            value_row(group_id, key, &[("100000", "100000"), ("300000", "300000")]),
        ],
        "zero_slot_tip" => vec![
            value_row(group_id, key, &[("100000", "100000"), ("300000", "300000")]),
            value_row(
                group_id,
                key,
                &[("500000", "500000"), ("1000000", "1000000")],
            ),
        ],
        "buy_mode" => vec![value_row(
            group_id,
            key,
            &[("买入时跟单买入", "buy"), ("卖出时跟单买入", "sell")],
        )],
        "mode" => vec![value_row(
            group_id,
            key,
            &[("跟卖模式", "follow"), ("止盈止损", "tp_sl")],
        )],
        _ => vec![value_row(group_id, key, &[("默认", "0")])],
    };

    let mut keyboard = rows;
    keyboard.push(json!([
        {"text": "返回参数页", "callback_data": format!("gm:set:{}", group_id)},
        {"text": "返回菜单", "callback_data": "gm:main"}
    ]));
    json!({ "inline_keyboard": keyboard })
}

fn value_row(group_id: &str, key: &str, values: &[(&str, &str)]) -> serde_json::Value {
    let items: Vec<_> = values
        .iter()
        .map(|(label, value)| {
            json!({
                "text": label,
                "callback_data": format!("gm:val:{}:{}:{}", group_id, key, value),
            })
        })
        .collect();
    json!(items)
}

fn format_groups_overview(groups: &GroupManager, selected: Option<String>) -> String {
    let mut text = "<b>组合列表</b>".to_string();
    text.push_str(&format!(
        "\n0slot 买入通道: {}",
        zero_slot_buy_status_label(groups.zero_slot_buy_enabled())
    ));
    for group in groups.all_groups() {
        let selected_tag = if selected.as_deref() == Some(group.id.as_str()) {
            " [当前组合]"
        } else {
            ""
        };
        text.push_str(&format!(
            "\n\n<b>{}</b> ({}){}\n状态：{}\n钱包数：{}\n跟单买入模式：{}\n卖出模式：{}\n买入金额：{} SOL | 共识数量：{}",
            group.name,
            group.id,
            selected_tag,
            if group.enabled { "启用" } else { "停用" },
            group.wallets.len(),
            entry_mode_label(group.entry_mode),
            sell_mode_label(group.sell_mode),
            group.buy_sol_amount,
            group.consensus_min_wallets,
        ));
    }
    text
}

fn format_group_compact(group: &CopyGroup) -> String {
    format!(
        "组合: <b>{}</b> ({})\n模式: {}\n买入: {} SOL | 最小买入: {} SOL\nTP: {}% | SL: {}% | Trailing: {}%\n买入滑点: {} bps | 卖出滑点: {} bps\n共识: {} | 持仓: {} 分钟\n买入小费: {} | 卖出小费: {}",
        group.name,
        group.id,
        sell_mode_label(group.sell_mode),
        group.buy_sol_amount,
        group.min_target_buy_sol,
        group.take_profit_percent,
        group.stop_loss_percent,
        group.trailing_stop_percent,
        group.slippage_bps,
        group.sell_slippage_bps,
        group.consensus_min_wallets,
        group.max_hold_seconds / 60,
        group.tip_buy_lamports,
        group.tip_sell_lamports,
    )
}

fn format_group_detail(group: &CopyGroup, selected: bool) -> String {
    let mut text = format!(
        "<b>{}</b> ({}){}\n状态: {}\n卖出模式: {}\n钱包数: {}\n买入: {} SOL\n最小触发买入: {} SOL\nTP: {}%\nSL: {}%\nTrailing: {}%\n买入滑点: {} bps\n卖出滑点: {} bps\n共识数量: {}\n持仓时间: {} 分钟\n买入小费: {} lamports\n卖出小费: {} lamports",
        group.name,
        group.id,
        if selected { " [当前组合]" } else { "" },
        if group.enabled { "启用" } else { "停用" },
        sell_mode_label(group.sell_mode),
        group.wallets.len(),
        group.buy_sol_amount,
        group.min_target_buy_sol,
        group.take_profit_percent,
        group.stop_loss_percent,
        group.trailing_stop_percent,
        group.slippage_bps,
        group.sell_slippage_bps,
        group.consensus_min_wallets,
        group.max_hold_seconds / 60,
        group.tip_buy_lamports,
        group.tip_sell_lamports,
    );

    if group.wallets.is_empty() {
        text.push_str("\n监听钱包: 暂无");
    } else {
        text.push_str("\n监听钱包:");
        for wallet in &group.wallets {
            text.push_str(&format!("\n- <code>{}</code>", wallet));
        }
    }

    text
}

fn group_value_text(group: &CopyGroup, key: &str) -> String {
    match key {
        "buy" => format!("{} SOL", group.buy_sol_amount),
        "min_buy" => format!("{} SOL", group.min_target_buy_sol),
        "tp" => format!("{}%", group.take_profit_percent),
        "sl" => format!("{}%", group.stop_loss_percent),
        "trailing" => format!("{}%", group.trailing_stop_percent),
        "slippage" => format!("{} bps", group.slippage_bps),
        "sell_slippage" => format!("{} bps", group.sell_slippage_bps),
        "consensus" => group.consensus_min_wallets.to_string(),
        "hold" => format!("{} 分钟", group.max_hold_seconds / 60),
        "tip_buy" => format!("{} lamports", group.tip_buy_lamports),
        "tip_sell" => format!("{} lamports", group.tip_sell_lamports),
        "zero_slot_tip" => format!("{} lamports", group.zero_slot_tip_lamports),
        "buy_mode" => entry_mode_label(group.entry_mode).to_string(),
        "mode" => sell_mode_label(group.sell_mode).to_string(),
        _ => "-".to_string(),
    }
}

fn format_positions(positions: &[Position]) -> String {
    let mut text = format!("<b>持仓列表</b> ({})", positions.len());
    for (index, pos) in positions.iter().enumerate() {
        let mint = pos.token_mint.to_string();
        let token_name = if pos.token_name.is_empty() {
            format!("{}..{}", &mint[..6], &mint[mint.len() - 4..])
        } else {
            pos.token_name.clone()
        };

        text.push_str(&format!(
            "\n\n{}. <b>{}</b> [{}]\n状态: {}\nPnL: {:+.2}%\n持仓时长: {}",
            index + 1,
            token_name,
            pos.group.name,
            pos.state,
            pos.pnl_percent(),
            fmt_time(pos.held_seconds()),
        ));
    }
    text
}

fn group_menu_keyboard_v2(zero_slot_buy_enabled: bool) -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "查看组合", "callback_data": "gm:overview"},
                {"text": "新增组合", "callback_data": "gm:add"}
            ],
            [
                {"text": "删除组合", "callback_data": "gm:pick:del"},
                {"text": "启用组合", "callback_data": "gm:pick:on"}
            ],
            [
                {"text": "停用组合", "callback_data": "gm:pick:off"},
                {"text": "设置组合", "callback_data": "gm:pick:set"}
            ],
            [
                {"text": "切换当前组合", "callback_data": "gm:pick:use"},
                {"text": "查看持仓列表", "callback_data": "gm:positions"}
            ],
            [
                {"text": "组合绩效", "callback_data": "gm:perf"}
            ],
            [
                {"text": zero_slot_buy_button_label(zero_slot_buy_enabled), "callback_data": "gm:toggle_zero_slot_buy"}
            ]
        ]
    })
}

fn group_performance_keyboard() -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "刷新组合绩效", "callback_data": "gm:perf"},
                {"text": "查看组合", "callback_data": "gm:overview"}
            ],
            [
                {"text": "返回菜单", "callback_data": "gm:main"}
            ]
        ]
    })
}

fn zero_slot_buy_button_label(enabled: bool) -> &'static str {
    if enabled {
        "关闭 0slot 买入"
    } else {
        "开启 0slot 买入"
    }
}

fn zero_slot_buy_status_label(enabled: bool) -> &'static str {
    if enabled {
        "已开启"
    } else {
        "已关闭"
    }
}

fn group_setting_menu_keyboard_v2(group: &CopyGroup) -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "买入金额", "callback_data": format!("gm:key:{}:buy", group.id)},
                {"text": "最小买入", "callback_data": format!("gm:key:{}:min_buy", group.id)}
            ],
            [
                {"text": "止盈", "callback_data": format!("gm:key:{}:tp", group.id)},
                {"text": "止损", "callback_data": format!("gm:key:{}:sl", group.id)}
            ],
            [
                {"text": "移动止损", "callback_data": format!("gm:key:{}:trailing", group.id)},
                {"text": "共识数量", "callback_data": format!("gm:key:{}:consensus", group.id)}
            ],
            [
                {"text": "买入滑点", "callback_data": format!("gm:key:{}:slippage", group.id)},
                {"text": "卖出滑点", "callback_data": format!("gm:key:{}:sell_slippage", group.id)}
            ],
            [
                {"text": "持仓时间", "callback_data": format!("gm:key:{}:hold", group.id)},
                {"text": "跟单买入模式", "callback_data": format!("gm:key:{}:buy_mode", group.id)}
            ],
            [
                {"text": "买入小费", "callback_data": format!("gm:key:{}:tip_buy", group.id)},
                {"text": "卖出小费", "callback_data": format!("gm:key:{}:tip_sell", group.id)}
            ],
            [
                {"text": "0slot 小费", "callback_data": format!("gm:key:{}:zero_slot_tip", group.id)},
                {"text": "卖出模式", "callback_data": format!("gm:key:{}:mode", group.id)}
            ],
            [
                {"text": "查看组合", "callback_data": format!("gm:view:{}", group.id)},
                {"text": "返回菜单", "callback_data": "gm:main"}
            ]
        ]
    })
}

fn group_wallet_hint_keyboard(group: &CopyGroup) -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "查看钱包列表", "callback_data": format!("gm:view:{}", group.id)},
                {"text": "删除钱包", "callback_data": format!("gw:r:{}", group.id)}
            ],
            [
                {"text": "返回组合", "callback_data": format!("gm:view:{}", group.id)}
            ]
        ]
    })
}

fn group_wallet_remove_keyboard(group: &CopyGroup) -> serde_json::Value {
    let mut rows = Vec::new();
    if group.wallets.is_empty() {
        rows.push(json!([
            {"text": "暂无钱包", "callback_data": format!("gm:view:{}", group.id)}
        ]));
    } else {
        for wallet in &group.wallets {
            rows.push(json!([
                {
                    "text": short_pubkey(wallet),
                    "callback_data": format!("gw:x:{}:{}", group.id, wallet)
                }
            ]));
        }
    }
    rows.push(json!([
        {"text": "返回组合", "callback_data": format!("gm:view:{}", group.id)}
    ]));
    json!({ "inline_keyboard": rows })
}

fn group_rename_keyboard(group: &CopyGroup) -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "返回组合", "callback_data": format!("gm:view:{}", group.id)},
                {"text": "查看全部组合", "callback_data": "gm:overview"}
            ]
        ]
    })
}

fn format_group_compact_v2(group: &CopyGroup) -> String {
    format!(
        "组合：<b>{}</b> ({})\n跟单买入模式：{}\n卖出模式：{}\n买入金额：{} SOL | 最小买入：{} SOL\nTP：{}% | SL：{}% | 移动止损：{}%\n买入滑点：{} bps | 卖出滑点：{} bps\n共识数量：{} | 持仓时间：{} 分钟\n买入小费：{} | 卖出小费：{}\n0slot 小费：{}",
        group.name,
        group.id,
        entry_mode_label(group.entry_mode),
        sell_mode_label(group.sell_mode),
        group.buy_sol_amount,
        group.min_target_buy_sol,
        group.take_profit_percent,
        group.stop_loss_percent,
        group.trailing_stop_percent,
        group.slippage_bps,
        group.sell_slippage_bps,
        group.consensus_min_wallets,
        group.max_hold_seconds / 60,
        group.tip_buy_lamports,
        group.tip_sell_lamports,
        group.zero_slot_tip_lamports,
    )
}

fn format_group_detail_v2(group: &CopyGroup, selected: bool) -> String {
    let mut text = format!(
        "<b>{}</b> ({}){}\n状态：{}\n跟单买入模式：{}\n卖出模式：{}\n钱包数：{}\n买入金额：{} SOL\n最小触发买入：{} SOL\nTP：{}%\nSL：{}%\n移动止损：{}%\n买入滑点：{} bps\n卖出滑点：{} bps\n共识数量：{}\n持仓时间：{} 分钟\n买入小费：{} lamports\n卖出小费：{} lamports\n0slot 小费：{} lamports",
        group.name,
        group.id,
        if selected { " [当前组合]" } else { "" },
        if group.enabled { "启用" } else { "停用" },
        entry_mode_label(group.entry_mode),
        sell_mode_label(group.sell_mode),
        group.wallets.len(),
        group.buy_sol_amount,
        group.min_target_buy_sol,
        group.take_profit_percent,
        group.stop_loss_percent,
        group.trailing_stop_percent,
        group.slippage_bps,
        group.sell_slippage_bps,
        group.consensus_min_wallets,
        group.max_hold_seconds / 60,
        group.tip_buy_lamports,
        group.tip_sell_lamports,
        group.zero_slot_tip_lamports,
    );

    if group.wallets.is_empty() {
        text.push_str("\n监听钱包：暂无");
    } else {
        text.push_str("\n监听钱包：");
        for wallet in &group.wallets {
            text.push_str(&format!("\n- <code>{}</code>", wallet));
        }
    }

    text
}

fn setting_label_v2(key: &str) -> &'static str {
    match key {
        "buy" => "买入金额",
        "min_buy" => "最小触发买入额",
        "tp" => "止盈比例",
        "sl" => "止损比例",
        "trailing" => "移动止损",
        "slippage" => "买入滑点",
        "sell_slippage" => "卖出滑点",
        "consensus" => "共识数量",
        "hold" => "持仓时间",
        "tip_buy" => "买入小费",
        "tip_sell" => "卖出小费",
        "zero_slot_tip" => "0slot 小费",
        "buy_mode" => "跟单买入模式",
        "mode" => "卖出模式",
        _ => "参数",
    }
}

fn format_positions_v2(positions: &[Position], sol_usd_price: f64) -> String {
    let mut text = format!("<b>持仓列表</b> ({})", positions.len());
    for (index, pos) in positions.iter().enumerate() {
        let entry_mcap = format_market_cap(pos.entry_mcap_sol, sol_usd_price);
        let current_mcap = format_market_cap(current_market_cap_sol(pos), sol_usd_price);
        text.push_str(&format!(
            "\n\n{}. <b>{}</b>\n代币地址: <code>{}</code>\n组合: {} ({})\n状态: {} | PnL: {:+.2}% | 持仓: {}\n买入市值: {} | 当前市值: {}",
            index + 1,
            position_display_name(pos),
            pos.token_mint,
            pos.group.name,
            pos.group.id,
            pos.state,
            pos.pnl_percent(),
            fmt_time(pos.held_seconds()),
            entry_mcap,
            current_mcap,
        ));
    }
    text
}

fn sorted_positions(mut positions: Vec<Position>) -> Vec<Position> {
    positions.sort_by(|left, right| {
        position_display_name(left)
            .cmp(&position_display_name(right))
            .then_with(|| {
                left.token_mint
                    .to_string()
                    .cmp(&right.token_mint.to_string())
            })
            .then_with(|| left.group.id.cmp(&right.group.id))
    });
    positions
}

fn position_display_name(position: &Position) -> String {
    if position.token_name.is_empty() {
        short_pubkey(&position.token_mint)
    } else {
        position.token_name.clone()
    }
}

fn positions_list_keyboard(positions: &[Position]) -> serde_json::Value {
    let mut rows = Vec::new();
    for position in positions {
        rows.push(json!([
            {
                "text": format!("{} [{}]", position_display_name(position), position.group.id),
                "callback_data": format!("ps:view:{}:{}", position.group.id, position.token_mint)
            }
        ]));
    }
    rows.push(json!([
        {"text": "刷新", "callback_data": "ps:list"},
        {"text": "返回菜单", "callback_data": "gm:main"}
    ]));
    json!({ "inline_keyboard": rows })
}

fn position_detail_keyboard(position: &Position) -> serde_json::Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "25%", "callback_data": format!("ps:sell:{}:{}:25", position.group.id, position.token_mint)},
                {"text": "50%", "callback_data": format!("ps:sell:{}:{}:50", position.group.id, position.token_mint)}
            ],
            [
                {"text": "75%", "callback_data": format!("ps:sell:{}:{}:75", position.group.id, position.token_mint)},
                {"text": "100%", "callback_data": format!("ps:sell:{}:{}:100", position.group.id, position.token_mint)}
            ],
            [
                {"text": "刷新", "callback_data": format!("ps:view:{}:{}", position.group.id, position.token_mint)},
                {"text": "返回列表", "callback_data": "ps:list"}
            ]
        ]
    })
}

fn format_position_detail(position: &Position, sol_usd_price: f64) -> String {
    let entry_mcap = format_market_cap(position.entry_mcap_sol, sol_usd_price);
    let current_mcap = format_market_cap(current_market_cap_sol(position), sol_usd_price);
    format!(
        "<b>{}</b>\n代币地址: <code>{}</code>\n组合: {} ({})\n状态: {}\n成本: {:.4} SOL\n买入市值: {}\n当前市值: {}\n当前数量: {:.4}\nPnL: {:+.2}%\n持仓时长: {}",
        position_display_name(position),
        position.token_mint,
        position.group.name,
        position.group.id,
        position.state,
        position.entry_sol_amount as f64 / 1e9,
        entry_mcap,
        current_mcap,
        position.token_amount as f64 / 1e6,
        position.pnl_percent(),
        fmt_time(position.held_seconds()),
    )
}

fn current_market_cap_sol(position: &Position) -> f64 {
    position.current_price * 1_000_000_000.0
}

fn format_market_cap(market_cap_sol: f64, sol_usd_price: f64) -> String {
    if market_cap_sol <= 0.0 || sol_usd_price <= 0.0 {
        return "-".to_string();
    }
    format_usd_short(market_cap_sol * sol_usd_price)
}

fn format_usd_short(value: f64) -> String {
    if value >= 1_000_000_000.0 {
        format!("${:.2}B", value / 1_000_000_000.0)
    } else if value >= 1_000_000.0 {
        format!("${:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("${:.2}K", value / 1_000.0)
    } else if value >= 1.0 {
        format!("${:.2}", value)
    } else {
        format!("${:.4}", value)
    }
}

fn short_pubkey(pubkey: &Pubkey) -> String {
    let value = pubkey.to_string();
    format!("{}..{}", &value[..6], &value[value.len() - 4..])
}

fn fmt_time(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

pub async fn send_shutdown_notification(bot_token: &str, chat_id: &str) {
    let url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);
    let body = json!({
        "chat_id": chat_id,
        "text": "<b>Solana 跟单机器人已离线</b>",
        "parse_mode": "HTML",
    });

    if let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
    {
        let _ = client.post(&url).json(&body).send().await;
    }
}
