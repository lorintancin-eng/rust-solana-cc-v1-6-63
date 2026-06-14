use anyhow::Result;
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::{v0, VersionedMessage},
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};
use tracing::debug;

use crate::config::AppConfig;
use crate::processor::MirrorInstruction;

/// 交易构建器：将 MirrorInstruction 组装成 VersionedTransaction (V0)
/// 支持 Address Lookup Tables，减少交易体积
pub struct TxBuilder;

impl TxBuilder {
    /// 组装指令列表（ComputeBudget + pre + swap + post）
    fn assemble_instructions(mirror: &MirrorInstruction, config: &AppConfig) -> Vec<Instruction> {
        let mut all_instructions: Vec<Instruction> = Vec::with_capacity(
            2 + mirror.pre_instructions.len()
                + mirror.swap_instructions.len()
                + mirror.post_instructions.len(),
        );

        // 1. Compute budget 指令（优先费）
        if config.compute_units > 0 {
            all_instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
                config.compute_units,
            ));
        }
        if config.priority_fee_micro_lamport > 0 {
            all_instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
                config.priority_fee_micro_lamport,
            ));
        }

        // 2. Pre-instructions（ATA 创建等）
        all_instructions.extend(mirror.pre_instructions.clone());

        // 3. Swap 指令
        all_instructions.extend(mirror.swap_instructions.clone());

        // 4. Post-instructions（ATA 关闭等）
        all_instructions.extend(mirror.post_instructions.clone());

        all_instructions
    }

    /// 编译 V0 Message + 签名 → VersionedTransaction
    fn compile_and_sign(
        instructions: &[Instruction],
        keypair: &Keypair,
        recent_blockhash: Hash,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let v0_msg =
            v0::Message::try_compile(&keypair.pubkey(), instructions, alts, recent_blockhash)?;

        let tx = VersionedTransaction::try_new(VersionedMessage::V0(v0_msg), &[keypair])?;

        Ok(tx)
    }

    /// 将镜像指令组装为 VersionedTransaction (V0)
    pub fn build_transaction(
        mirror: &MirrorInstruction,
        config: &AppConfig,
        keypair: &Keypair,
        recent_blockhash: Hash,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let all_instructions = Self::assemble_instructions(mirror, config);

        debug!(
            "Built V0 transaction with {} instructions (CU={}, priority_fee={}, ALTs={})",
            all_instructions.len(),
            config.compute_units,
            config.priority_fee_micro_lamport,
            alts.len(),
        );

        Self::compile_and_sign(&all_instructions, keypair, recent_blockhash, alts)
    }

    /// 构建 0slot 专用交易：fee 指令放在最前面
    pub fn build_0slot_transaction(
        mirror: &MirrorInstruction,
        config: &AppConfig,
        keypair: &Keypair,
        recent_blockhash: Hash,
        zero_slot_tip_account: &solana_sdk::pubkey::Pubkey,
        tip_lamports: u64,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let mut all_instructions: Vec<Instruction> = Vec::with_capacity(
            1 + 2
                + mirror.pre_instructions.len()
                + mirror.swap_instructions.len()
                + mirror.post_instructions.len(),
        );

        if tip_lamports > 0 {
            all_instructions.push(solana_sdk::system_instruction::transfer(
                &keypair.pubkey(),
                zero_slot_tip_account,
                tip_lamports,
            ));
        }

        all_instructions.extend(Self::assemble_instructions(mirror, config));

        debug!(
            "Built 0slot V0 transaction with {} instructions (tip={} lamports, ALTs={})",
            all_instructions.len(),
            tip_lamports,
            alts.len(),
        );

        Self::compile_and_sign(&all_instructions, keypair, recent_blockhash, alts)
    }

    /// 构建 Jito bundle 交易（附带 tip 转账）
    pub fn build_jito_bundle_transaction(
        mirror: &MirrorInstruction,
        config: &AppConfig,
        keypair: &Keypair,
        recent_blockhash: Hash,
        jito_tip_account: &solana_sdk::pubkey::Pubkey,
        tip_lamports: u64,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let mut all_instructions = Self::assemble_instructions(mirror, config);

        // Jito tip（放在最后）
        if tip_lamports > 0 {
            all_instructions.push(solana_sdk::system_instruction::transfer(
                &keypair.pubkey(),
                jito_tip_account,
                tip_lamports,
            ));
        }

        debug!(
            "Built Jito V0 transaction with {} instructions (tip={} lamports, ALTs={})",
            all_instructions.len(),
            tip_lamports,
            alts.len(),
        );

        Self::compile_and_sign(&all_instructions, keypair, recent_blockhash, alts)
    }

    /// 构建简单的 VersionedTransaction（无 MirrorInstruction，如 close ATA）
    pub fn build_simple(
        instructions: &[Instruction],
        keypair: &Keypair,
        recent_blockhash: Hash,
    ) -> Result<VersionedTransaction> {
        Self::compile_and_sign(instructions, keypair, recent_blockhash, &[])
    }
}
