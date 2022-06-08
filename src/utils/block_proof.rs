/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use anyhow::{anyhow, Result};
use ton_block::Deserializable;
use ton_types::{Cell, HashmapType};

use super::{BlockIdExtExtension, ShardStateStuff};
use crate::utils::*;

pub type BlockProofStuffAug = WithArchiveData<BlockProofStuff>;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockProofStuff {
    proof: ton_block::BlockProof,
    root: Cell,
    is_link: bool,
    id: ton_block::BlockIdExt,
}

impl BlockProofStuff {
    pub fn deserialize(
        block_id: ton_block::BlockIdExt,
        mut data: &[u8],
        is_link: bool,
    ) -> Result<Self> {
        let root = ton_types::deserialize_tree_of_cells(&mut data)?;
        let proof = ton_block::BlockProof::construct_from(&mut root.clone().into())?;

        if proof.proof_for != block_id {
            return Err(anyhow!(
                "proof for another block (found: {}, expected: {block_id})",
                proof.proof_for,
            ));
        }

        if !block_id.is_masterchain() && !is_link {
            return Err(anyhow!("proof for non-masterchain block {block_id}"));
        }

        Ok(Self {
            proof,
            root,
            is_link,
            id: block_id,
        })
    }

    pub fn virtualize_block_root(&self) -> Result<Cell> {
        let merkle_proof =
            ton_block::MerkleProof::construct_from(&mut self.proof.root.clone().into())?;
        let block_virt_root = merkle_proof.proof.virtualize(1);

        if *self.proof.proof_for.root_hash() != block_virt_root.repr_hash() {
            return Err(anyhow!(
                "merkle proof has invalid virtual hash (found: {}, expected: {})",
                block_virt_root.repr_hash(),
                self.proof.proof_for
            ));
        }

        Ok(block_virt_root)
    }

    pub fn virtualize_block(&self) -> Result<(ton_block::Block, ton_types::UInt256)> {
        let cell = self.virtualize_block_root()?;
        let hash = cell.repr_hash();
        Ok((ton_block::Block::construct_from(&mut cell.into())?, hash))
    }

    pub fn proof(&self) -> &ton_block::BlockProof {
        &self.proof
    }

    pub fn is_link(&self) -> bool {
        self.is_link
    }

    pub fn id(&self) -> &ton_block::BlockIdExt {
        &self.id
    }

    pub fn get_cur_validators_set(
        &self,
    ) -> Result<(ton_block::ValidatorSet, ton_block::CatchainConfig)> {
        let (virt_block, virt_block_info) = self.pre_check_block_proof()?;

        if !virt_block_info.key_block() {
            return Err(anyhow!(
                "proof for key block {} contains a Merkle proof which declares non key block",
                self.id
            ));
        }

        let (validator_set, catchain_config) = virt_block.read_cur_validator_set_and_cc_conf()?;

        Ok((validator_set, catchain_config))
    }

    pub fn check_with_prev_key_block_proof(
        &self,
        prev_key_block_proof: &BlockProofStuff,
    ) -> Result<()> {
        let (virt_block, virt_block_info) = self.pre_check_block_proof()?;
        check_with_prev_key_block_proof(self, prev_key_block_proof, &virt_block, &virt_block_info)?;
        Ok(())
    }

    pub fn check_with_master_state(&self, master_state: &ShardStateStuff) -> Result<()> {
        if self.is_link {
            return Err(anyhow!(
                "Can't verify block {}: can't call `check_with_master_state` for proof link",
                self.id
            ));
        }

        let (virt_block, virt_block_info) = self.pre_check_block_proof()?;
        check_with_master_state(self, master_state, &virt_block, &virt_block_info)?;
        Ok(())
    }

    pub fn check_proof_link(&self) -> Result<()> {
        if !self.is_link {
            return Err(anyhow!(
                "Can't call `check_proof_link` not for proof link, block {}",
                self.id
            ));
        }

        self.pre_check_block_proof()?;
        Ok(())
    }

    pub fn pre_check_block_proof(&self) -> Result<(ton_block::Block, ton_block::BlockInfo)> {
        if !self.id.is_masterchain() && self.proof.signatures.is_some() {
            return Err(anyhow!(
                "proof for non-master block {} can't contain signatures",
                self.id
            ));
        }

        let (virt_block, virt_block_hash) = self.virtualize_block()?;

        if virt_block_hash != self.id.root_hash {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with incorrect root hash: expected {}, found: {} ",
                self.id,
                self.id.root_hash,
                virt_block_hash
            ));
        }

        let info = virt_block.read_info()?;
        let _value_flow = virt_block.read_value_flow()?;
        let _state_update = virt_block.read_state_update()?;

        if info.version() != 0 {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with incorrect block info's version {}",
                self.id,
                info.version()
            ));
        }

        if info.seq_no() != self.id.seq_no as u32 {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with seq_no {}, but {} is expected",
                self.id,
                info.seq_no(),
                self.id.seq_no
            ));
        }

        if info.shard() != self.id.shard() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with shard id {}, but {} is expected",
                self.id,
                info.shard(),
                self.id.shard()
            ));
        }

        if info.read_master_ref()?.is_none() != info.shard().is_masterchain() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with invalid not_master flag in block info",
                self.id,
            ));
        }

        if self.id.is_masterchain()
            && (info.after_merge() || info.before_split() || info.after_split())
        {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares split/merge for a masterchain block",
                self.id,
            ));
        }

        if info.after_merge() && info.after_split() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares both after merge and after split flags",
                self.id,
            ));
        }

        if info.after_split() && (info.shard().is_full()) {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_split flag and non zero shard prefix",
                self.id,
            ));
        }

        if info.after_merge() && !info.shard().can_split() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_merge flag and shard prefix which can't split anymore",
                self.id,
            ));
        }

        if info.key_block() && !self.id.is_masterchain() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof which declares non master chain but key block",
                self.id(),
            ));
        }

        Ok((virt_block, info))
    }

    fn check_signatures(
        &self,
        validators_list: Vec<ton_block::ValidatorDescr>,
        list_hash_short: u32,
    ) -> Result<()> {
        // Prepare
        let signatures = match self.proof.signatures.as_ref() {
            Some(signatures) => signatures,
            None => {
                return Err(anyhow!(
                    "Proof for {} doesn't have signatures to check",
                    self.id,
                ));
            }
        };

        if signatures.validator_info.validator_list_hash_short != list_hash_short {
            return Err(anyhow!(
                "Bad validator set hash in proof for block {}, calculated: {}, found: {}",
                self.id,
                list_hash_short,
                signatures.validator_info.validator_list_hash_short
            ));
        }

        let expected_count = signatures.pure_signatures.count() as usize;
        let count = signatures
            .pure_signatures
            .signatures()
            .count(expected_count)?;
        if expected_count != count {
            return Err(anyhow!(
                "Proof for {}: signature count mismatch: declared: {}, calculated: {}",
                self.id,
                expected_count,
                count,
            ));
        }

        // Check signatures
        let checked_data =
            ton_block::Block::build_data_for_sign(&self.id.root_hash, &self.id.file_hash);
        let total_weight: u64 = validators_list.iter().map(|v| v.weight).sum();
        let weight = signatures
            .pure_signatures
            .check_signatures(&validators_list, &checked_data)
            .map_err(|e| anyhow!("Proof for {}: error while check signatures: {}", self.id, e))?;

        // Check weight
        if weight != signatures.pure_signatures.weight() {
            return Err(anyhow!(
                "Proof for {}: total signature weight mismatch: declared: {}, calculated: {}",
                self.id,
                signatures.pure_signatures.weight(),
                weight
            ));
        }

        if weight * 3 <= total_weight * 2 {
            return Err(anyhow!(
                "Proof for {}: too small signatures weight",
                self.id,
            ));
        }

        Ok(())
    }

    fn pre_check_key_block_proof(&self, virt_block: &ton_block::Block) -> Result<()> {
        let extra = virt_block.read_extra()?;
        let mc_extra = extra.read_custom()?.ok_or_else(|| {
            anyhow!(
                "proof for key block {} contains a Merkle proof without masterchain block extra",
                self.id(),
            )
        })?;

        let config = mc_extra.config().ok_or_else(|| {
            anyhow!(
                "proof for key block {} contains a Merkle proof without config params",
                self.id(),
            )
        })?;

        let _cur_validator_set = config.config(34)?
            .ok_or_else(|| anyhow!(
                "proof for key block {} contains a Merkle proof without current validators config param (34)",
                self.id(),
            ))?;
        for param in 32..=38 {
            let _val_set = config.config(param)?;
        }
        let _catchain_config = config.config(28)?;

        Ok(())
    }

    fn process_given_state(
        &self,
        state: &ShardStateStuff,
        block_info: &ton_block::BlockInfo,
    ) -> Result<(Vec<ton_block::ValidatorDescr>, u32)> {
        if !state.block_id().is_masterchain() {
            return Err(anyhow!(
                "Can't check proof for {}: given state {} doesn't belong masterchain",
                self.id,
                state.block_id()
            ));
        }

        if !self.id().is_masterchain() {
            return Err(anyhow!(
                "Can't check proof for non master block {} using master state",
                self.id,
            ));
        }

        if (state.block_id().seq_no as u32) < block_info.prev_key_block_seqno() {
            return Err(anyhow!(
                "Can't check proof for block {} using master state {}, because it is older than the previous key block with seqno {}",
                self.id,
                state.block_id(),
                block_info.prev_key_block_seqno()
            ));
        }

        if state.block_id().seq_no > self.id.seq_no {
            return Err(anyhow!(
                "Can't check proof for block {} using newer master state {}",
                self.id,
                state.block_id(),
            ));
        }

        let (validator_set, catchain_config) =
            state.state().read_cur_validator_set_and_cc_conf()?;

        self.calc_validators_subset(&validator_set, &catchain_config, block_info.gen_utime().0)
    }

    fn process_prev_key_block_proof(
        &self,
        prev_key_block_proof: &BlockProofStuff,
        gen_utime: u32,
    ) -> Result<(Vec<ton_block::ValidatorDescr>, u32)> {
        let (virt_key_block, prev_key_block_info) = prev_key_block_proof.pre_check_block_proof()?;

        if !prev_key_block_info.key_block() {
            return Err(anyhow!(
                "proof for key block {} contains a Merkle proof which declares non key block",
                prev_key_block_proof.id,
            ));
        }

        let (validator_set, catchain_config) = virt_key_block.read_cur_validator_set_and_cc_conf()
            .map_err(|e| {
                anyhow!(
                    "While checking proof for {}: can't extract config params from key block's proof {}: {}",
                    self.id,
                    prev_key_block_proof.id(),
                    e
                )
            })?;

        self.calc_validators_subset(&validator_set, &catchain_config, gen_utime)
    }

    fn calc_validators_subset(
        &self,
        validator_set: &ton_block::ValidatorSet,
        catchain_config: &ton_block::CatchainConfig,
        gen_utime: u32,
    ) -> Result<(Vec<ton_block::ValidatorDescr>, u32)> {
        validator_set.calc_subset(
            catchain_config,
            self.id.shard().shard_prefix_with_tag(),
            self.id.shard().workchain_id(),
            self.proof
                .signatures
                .as_ref()
                .map(|s| s.validator_info.catchain_seqno)
                .unwrap_or_default(),
            ton_block::UnixTime32(gen_utime),
        )
    }
}

pub fn check_with_prev_key_block_proof(
    proof: &BlockProofStuff,
    prev_key_block_proof: &BlockProofStuff,
    virt_block: &ton_block::Block,
    virt_block_info: &ton_block::BlockInfo,
) -> Result<()> {
    if !proof.id.is_masterchain() {
        return Err(anyhow!(
            "Can't verify non masterchain block {} using previous key masterchain block",
            proof.id
        ));
    }

    if !prev_key_block_proof.id.is_masterchain() {
        return Err(anyhow!(
            "Invalid previous key block: it's id {} doesn't belong to the masterchain",
            prev_key_block_proof.id
        ));
    }

    let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();
    if prev_key_block_proof.id.seq_no as u32 != prev_key_block_seqno {
        return Err(anyhow!(
            "Can't verify block {} using key block {} because the block declares different previous key block seqno {}",
            proof.id,
            prev_key_block_proof.id,
            prev_key_block_seqno
        ));
    }

    if prev_key_block_proof.id.seq_no >= proof.id.seq_no {
        return Err(anyhow!(
            "Can't verify block {} using key block {} with larger or equal seqno",
            proof.id,
            prev_key_block_proof.id
        ));
    }

    let (validators, validators_hash_short) =
        proof.process_prev_key_block_proof(prev_key_block_proof, virt_block_info.gen_utime().0)?;

    if virt_block_info.key_block() {
        proof.pre_check_key_block_proof(virt_block)?;
    }

    proof.check_signatures(validators, validators_hash_short)
}

pub fn check_with_master_state(
    proof: &BlockProofStuff,
    master_state: &ShardStateStuff,
    virt_block: &ton_block::Block,
    virt_block_info: &ton_block::BlockInfo,
) -> Result<()> {
    if virt_block_info.key_block() {
        proof.pre_check_key_block_proof(virt_block)?;
    }

    let (validators, validators_hash_short) =
        proof.process_given_state(master_state, virt_block_info)?;

    proof.check_signatures(validators, validators_hash_short)
}
