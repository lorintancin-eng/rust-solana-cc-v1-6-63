use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

use crate::config::{AppConfig, SELL_MODE_TP_SL};

const GROUPS_FILE: &str = "copy_groups.json";
pub const ENTRY_MODE_SMART_BUY: u8 = 0;
pub const ENTRY_MODE_SMART_SELL: u8 = 1;

#[derive(Debug, Clone)]
pub struct CopyGroup {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub wallets: Vec<Pubkey>,
    pub consensus_min_wallets: usize,
    pub consensus_timeout_secs: u64,
    pub buy_sol_amount: f64,
    pub min_target_buy_sol: f64,
    pub take_profit_percent: f64,
    pub stop_loss_percent: f64,
    pub trailing_stop_percent: f64,
    pub slippage_bps: u64,
    pub sell_slippage_bps: u64,
    pub max_hold_seconds: u64,
    pub tip_buy_lamports: u64,
    pub tip_sell_lamports: u64,
    pub zero_slot_tip_lamports: u64,
    pub entry_mode: u8,
    pub sell_mode: u8,
}

impl CopyGroup {
    pub fn from_app_config(config: &AppConfig) -> Self {
        Self {
            id: "g1".to_string(),
            name: "group-1".to_string(),
            enabled: true,
            wallets: config.target_wallets.clone(),
            consensus_min_wallets: config.consensus_min_wallets,
            consensus_timeout_secs: config.consensus_timeout_secs,
            buy_sol_amount: config.buy_sol_amount,
            min_target_buy_sol: config.min_target_buy_sol,
            take_profit_percent: config.take_profit_percent,
            stop_loss_percent: config.stop_loss_percent,
            trailing_stop_percent: config.trailing_stop_percent,
            slippage_bps: config.slippage_bps,
            sell_slippage_bps: config.sell_slippage_bps,
            max_hold_seconds: config.max_hold_seconds,
            tip_buy_lamports: config.jito_buy_tip_lamports,
            tip_sell_lamports: config.jito_sell_tip_lamports,
            zero_slot_tip_lamports: config.zero_slot_tip_lamports,
            entry_mode: ENTRY_MODE_SMART_BUY,
            sell_mode: SELL_MODE_TP_SL,
        }
    }

    pub fn buy_lamports(&self) -> u64 {
        (self.buy_sol_amount * 1_000_000_000.0) as u64
    }

    pub fn min_target_buy_lamports(&self) -> u64 {
        (self.min_target_buy_sol * 1_000_000_000.0) as u64
    }

    pub fn follow_sell_mode(&self) -> bool {
        self.sell_mode != SELL_MODE_TP_SL
    }

    pub fn buy_on_smart_buy(&self) -> bool {
        self.entry_mode == ENTRY_MODE_SMART_BUY
    }

    pub fn buy_on_smart_sell(&self) -> bool {
        self.entry_mode == ENTRY_MODE_SMART_SELL
    }

    pub fn to_app_config(&self, base: &AppConfig) -> AppConfig {
        let mut config = base.clone();
        config.target_wallets = self.wallets.clone();
        config.consensus_min_wallets = self.consensus_min_wallets;
        config.consensus_timeout_secs = self.consensus_timeout_secs;
        config.buy_sol_amount = self.buy_sol_amount;
        config.min_target_buy_sol = self.min_target_buy_sol;
        config.take_profit_percent = self.take_profit_percent;
        config.stop_loss_percent = self.stop_loss_percent;
        config.trailing_stop_percent = self.trailing_stop_percent;
        config.slippage_bps = self.slippage_bps;
        config.sell_slippage_bps = self.sell_slippage_bps;
        config.max_hold_seconds = self.max_hold_seconds;
        config.jito_buy_tip_lamports = self.tip_buy_lamports;
        config.jito_sell_tip_lamports = self.tip_sell_lamports;
        config.zero_slot_tip_lamports = self.zero_slot_tip_lamports;
        config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedGroup {
    id: String,
    name: String,
    enabled: bool,
    wallets: Vec<String>,
    consensus_min_wallets: usize,
    consensus_timeout_secs: u64,
    buy_sol_amount: f64,
    min_target_buy_sol: f64,
    take_profit_percent: f64,
    stop_loss_percent: f64,
    trailing_stop_percent: f64,
    slippage_bps: u64,
    sell_slippage_bps: u64,
    max_hold_seconds: u64,
    tip_buy_lamports: u64,
    tip_sell_lamports: u64,
    #[serde(default)]
    zero_slot_tip_lamports: Option<u64>,
    #[serde(default)]
    entry_mode: u8,
    sell_mode: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedGroupsState {
    groups: Vec<PersistedGroup>,
    selected_group_id: Option<String>,
    blocklist: Vec<String>,
    #[serde(default)]
    zero_slot_buy_enabled: bool,
}

impl PersistedGroup {
    fn from_group(group: &CopyGroup) -> Self {
        Self {
            id: group.id.clone(),
            name: group.name.clone(),
            enabled: group.enabled,
            wallets: group.wallets.iter().map(ToString::to_string).collect(),
            consensus_min_wallets: group.consensus_min_wallets,
            consensus_timeout_secs: group.consensus_timeout_secs,
            buy_sol_amount: group.buy_sol_amount,
            min_target_buy_sol: group.min_target_buy_sol,
            take_profit_percent: group.take_profit_percent,
            stop_loss_percent: group.stop_loss_percent,
            trailing_stop_percent: group.trailing_stop_percent,
            slippage_bps: group.slippage_bps,
            sell_slippage_bps: group.sell_slippage_bps,
            max_hold_seconds: group.max_hold_seconds,
            tip_buy_lamports: group.tip_buy_lamports,
            tip_sell_lamports: group.tip_sell_lamports,
            zero_slot_tip_lamports: Some(group.zero_slot_tip_lamports),
            entry_mode: group.entry_mode,
            sell_mode: group.sell_mode,
        }
    }

    fn into_group(self, base: &AppConfig) -> Option<CopyGroup> {
        let mut wallets = Vec::with_capacity(self.wallets.len());
        for wallet in self.wallets {
            match Pubkey::from_str(&wallet) {
                Ok(pubkey) => wallets.push(pubkey),
                Err(_) => return None,
            }
        }

        Some(CopyGroup {
            id: self.id,
            name: self.name,
            enabled: self.enabled,
            wallets,
            consensus_min_wallets: self.consensus_min_wallets,
            consensus_timeout_secs: self.consensus_timeout_secs,
            buy_sol_amount: self.buy_sol_amount,
            min_target_buy_sol: self.min_target_buy_sol,
            take_profit_percent: self.take_profit_percent,
            stop_loss_percent: self.stop_loss_percent,
            trailing_stop_percent: self.trailing_stop_percent,
            slippage_bps: self.slippage_bps,
            sell_slippage_bps: self.sell_slippage_bps,
            max_hold_seconds: self.max_hold_seconds,
            tip_buy_lamports: self.tip_buy_lamports,
            tip_sell_lamports: self.tip_sell_lamports,
            zero_slot_tip_lamports: self
                .zero_slot_tip_lamports
                .unwrap_or(base.zero_slot_tip_lamports),
            entry_mode: self.entry_mode,
            sell_mode: self.sell_mode,
        })
    }
}

pub struct GroupManager {
    groups: Arc<DashMap<String, CopyGroup>>,
    selected_group_id: RwLock<Option<String>>,
    blocklist: DashSet<Pubkey>,
    zero_slot_buy_enabled: RwLock<bool>,
}

impl GroupManager {
    pub fn load_or_default(config: &AppConfig) -> Arc<Self> {
        let mut groups = Vec::new();
        let mut selected_group_id = None;
        let mut blocklist = Vec::new();
        let mut zero_slot_buy_enabled = false;

        if Path::new(GROUPS_FILE).exists() {
            match fs::read_to_string(GROUPS_FILE) {
                Ok(raw) => match serde_json::from_str::<PersistedGroupsState>(&raw) {
                    Ok(saved) => {
                        selected_group_id = saved.selected_group_id;
                        blocklist = saved.blocklist;
                        zero_slot_buy_enabled = saved.zero_slot_buy_enabled;
                        groups = saved
                            .groups
                            .into_iter()
                            .filter_map(|group| group.into_group(config))
                            .collect();
                    }
                    Err(err) => warn!("Failed to parse {}: {}", GROUPS_FILE, err),
                },
                Err(err) => warn!("Failed to read {}: {}", GROUPS_FILE, err),
            }
        }

        if groups.is_empty() {
            groups.push(CopyGroup::from_app_config(config));
        }

        let manager = Arc::new(Self {
            groups: Arc::new(DashMap::new()),
            selected_group_id: RwLock::new(selected_group_id),
            blocklist: DashSet::new(),
            zero_slot_buy_enabled: RwLock::new(zero_slot_buy_enabled),
        });

        for group in groups {
            manager.groups.insert(group.id.clone(), group);
        }

        for mint in blocklist {
            if let Ok(pubkey) = Pubkey::from_str(&mint) {
                manager.blocklist.insert(pubkey);
            }
        }

        if manager.selected_group().is_none() {
            let first_id = manager.all_groups().first().map(|group| group.id.clone());
            *manager.selected_group_id.write().unwrap() = first_id;
        }

        info!(
            "Loaded {} copy groups | target wallets={}",
            manager.groups.len(),
            manager.all_target_wallets().len(),
        );

        manager
    }

    pub fn all_groups(&self) -> Vec<CopyGroup> {
        let mut groups: Vec<_> = self
            .groups
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        groups.sort_by(|left, right| left.id.cmp(&right.id));
        groups
    }

    pub fn all_target_wallets(&self) -> Vec<Pubkey> {
        let mut wallets = Vec::new();
        for group in self.all_groups() {
            for wallet in group.wallets {
                if !wallets.contains(&wallet) {
                    wallets.push(wallet);
                }
            }
        }
        wallets
    }

    pub fn groups_for_wallet(&self, wallet: &Pubkey) -> Vec<CopyGroup> {
        self.all_groups()
            .into_iter()
            .filter(|group| group.enabled && group.wallets.contains(wallet))
            .collect()
    }

    pub fn get_group(&self, group_id: &str) -> Option<CopyGroup> {
        self.groups.get(group_id).map(|entry| entry.value().clone())
    }

    pub fn selected_group(&self) -> Option<CopyGroup> {
        let selected = self.selected_group_id.read().unwrap().clone()?;
        self.get_group(&selected)
    }

    pub fn selected_group_id(&self) -> Option<String> {
        self.selected_group_id.read().unwrap().clone()
    }

    pub fn set_selected_group(&self, group_id: &str) -> Result<(), String> {
        if !self.groups.contains_key(group_id) {
            return Err(format!("group not found: {}", group_id));
        }
        *self.selected_group_id.write().unwrap() = Some(group_id.to_string());
        self.save();
        Ok(())
    }

    pub fn replace_group(&self, group: CopyGroup) {
        self.groups.insert(group.id.clone(), group);
        self.save();
    }

    pub fn add_group(&self, name: String, base: &AppConfig) -> CopyGroup {
        let next_id = self.next_group_id();
        let mut group = CopyGroup::from_app_config(base);
        group.id = next_id.clone();
        group.name = name;
        group.wallets.clear();
        group.consensus_min_wallets = 1;
        self.groups.insert(next_id.clone(), group.clone());
        *self.selected_group_id.write().unwrap() = Some(next_id);
        self.save();
        group
    }

    pub fn delete_group(&self, group_id: &str) -> Result<(), String> {
        if self.groups.len() <= 1 {
            return Err("at least one group must remain".to_string());
        }

        if self.groups.remove(group_id).is_none() {
            return Err(format!("group not found: {}", group_id));
        }

        if self.selected_group_id() == Some(group_id.to_string()) {
            let next = self.all_groups().first().map(|group| group.id.clone());
            *self.selected_group_id.write().unwrap() = next;
        }

        self.save();
        Ok(())
    }

    pub fn set_group_enabled(&self, group_id: &str, enabled: bool) -> Result<(), String> {
        let Some(mut entry) = self.groups.get_mut(group_id) else {
            return Err(format!("group not found: {}", group_id));
        };
        entry.enabled = enabled;
        drop(entry);
        self.save();
        Ok(())
    }

    pub fn add_wallet(&self, group_id: &str, wallet: Pubkey) -> Result<(), String> {
        let Some(mut entry) = self.groups.get_mut(group_id) else {
            return Err(format!("group not found: {}", group_id));
        };

        if !entry.wallets.contains(&wallet) {
            entry.wallets.push(wallet);
        }

        drop(entry);
        self.save();
        Ok(())
    }

    pub fn rename_group(&self, group_id: &str, name: String) -> Result<(), String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err("group name cannot be empty".to_string());
        }

        let Some(mut entry) = self.groups.get_mut(group_id) else {
            return Err(format!("group not found: {}", group_id));
        };
        entry.name = trimmed.to_string();
        drop(entry);
        self.save();
        Ok(())
    }

    pub fn remove_wallet(&self, group_id: &str, wallet: &Pubkey) -> Result<(), String> {
        let Some(mut entry) = self.groups.get_mut(group_id) else {
            return Err(format!("group not found: {}", group_id));
        };

        if let Some(index) = entry
            .wallets
            .iter()
            .position(|candidate| candidate == wallet)
        {
            entry.wallets.remove(index);
            drop(entry);
            self.save();
            Ok(())
        } else {
            Err("wallet not found in group".to_string())
        }
    }

    pub fn is_blocked(&self, mint: &Pubkey) -> bool {
        self.blocklist.contains(mint)
    }

    pub fn block_token(&self, mint: Pubkey) {
        self.blocklist.insert(mint);
        self.save();
    }

    pub fn unblock_token(&self, mint: &Pubkey) {
        self.blocklist.remove(mint);
        self.save();
    }

    pub fn blocklist(&self) -> Vec<Pubkey> {
        self.blocklist.iter().map(|entry| *entry.key()).collect()
    }

    pub fn zero_slot_buy_enabled(&self) -> bool {
        *self.zero_slot_buy_enabled.read().unwrap()
    }

    pub fn set_zero_slot_buy_enabled(&self, enabled: bool) {
        *self.zero_slot_buy_enabled.write().unwrap() = enabled;
        self.save();
    }

    pub fn toggle_zero_slot_buy_enabled(&self) -> bool {
        let mut enabled = self.zero_slot_buy_enabled.write().unwrap();
        *enabled = !*enabled;
        let current = *enabled;
        drop(enabled);
        self.save();
        current
    }

    fn next_group_id(&self) -> String {
        let max_index = self
            .groups
            .iter()
            .filter_map(|entry| entry.key().strip_prefix('g')?.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        format!("g{}", max_index + 1)
    }

    fn save(&self) {
        let state = PersistedGroupsState {
            groups: self
                .all_groups()
                .iter()
                .map(PersistedGroup::from_group)
                .collect(),
            selected_group_id: self.selected_group_id(),
            blocklist: self.blocklist().iter().map(ToString::to_string).collect(),
            zero_slot_buy_enabled: self.zero_slot_buy_enabled(),
        };

        match serde_json::to_string_pretty(&state) {
            Ok(raw) => {
                if let Err(err) = fs::write(GROUPS_FILE, raw) {
                    warn!("Failed to write {}: {}", GROUPS_FILE, err);
                }
            }
            Err(err) => warn!("Failed to serialize {}: {}", GROUPS_FILE, err),
        }
    }
}
