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

#[derive(Clone)]
pub struct BlockProofStuff {
    proof: ton_block::BlockProof,
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
        let proof = ton_block::BlockProof::construct_from_cell(root)?;

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
            is_link,
            id: block_id,
        })
    }

    pub fn virtualize_block_root(&self) -> Result<Cell> {
        let merkle_proof = ton_block::MerkleProof::construct_from_cell(self.proof.root.clone())?;
        let block_virt_root = merkle_proof.proof.virtualize(1);

        if self.proof.proof_for.root_hash() != block_virt_root.repr_hash() {
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
        Ok((ton_block::Block::construct_from_cell(cell)?, hash))
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

        if info.seq_no() != self.id.seq_no {
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

    fn check_signatures(&self, subset: &ValidatorSubsetInfo) -> Result<()> {
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

        if signatures.validator_info.validator_list_hash_short != subset.short_hash {
            return Err(anyhow!(
                "Bad validator set hash in proof for block {}, calculated: {}, found: {}",
                self.id,
                subset.short_hash,
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
        let total_weight: u64 = subset.validators.iter().map(|v| v.weight).sum();
        let weight = signatures
            .pure_signatures
            .check_signatures(&subset.validators, &checked_data)
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
    ) -> Result<ValidatorSubsetInfo> {
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

        if state.block_id().seq_no < block_info.prev_key_block_seqno() {
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

        self.calc_validators_subset_standard(&validator_set, &catchain_config)
    }

    fn process_prev_key_block_proof(
        &self,
        prev_key_block_proof: &BlockProofStuff,
    ) -> Result<ValidatorSubsetInfo> {
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

        self.calc_validators_subset_standard(&validator_set, &catchain_config)
    }

    fn calc_validators_subset_standard(
        &self,
        validator_set: &ton_block::ValidatorSet,
        catchain_config: &ton_block::CatchainConfig,
    ) -> Result<ValidatorSubsetInfo> {
        let cc_seqno = self
            .proof
            .signatures
            .as_ref()
            .map(|s| s.validator_info.catchain_seqno)
            .unwrap_or_default();

        ValidatorSubsetInfo::compute_standard(validator_set, &self.id, catchain_config, cc_seqno)
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
    if prev_key_block_proof.id.seq_no != prev_key_block_seqno {
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

    let subset = proof.process_prev_key_block_proof(prev_key_block_proof)?;

    if virt_block_info.key_block() {
        proof.pre_check_key_block_proof(virt_block)?;
    }

    proof.check_signatures(&subset)
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

    let subset = proof.process_given_state(master_state, virt_block_info)?;
    proof.check_signatures(&subset)
}

#[derive(Clone, Debug)]
pub struct ValidatorSubsetInfo {
    pub validators: Vec<ton_block::ValidatorDescr>,
    pub short_hash: u32,
    #[cfg(feature = "venom")]
    pub collator_range: Option<ton_block::CollatorRange>,
}

impl ValidatorSubsetInfo {
    pub fn compute_standard(
        validator_set: &ton_block::ValidatorSet,
        block_id: &ton_block::BlockIdExt,
        catchain_config: &ton_block::CatchainConfig,
        cc_seqno: u32,
    ) -> Result<Self> {
        let (validators, short_hash) = validator_set.calc_subset(
            catchain_config,
            block_id.shard().shard_prefix_with_tag(),
            block_id.shard().workchain_id(),
            cc_seqno,
            Default::default(),
        )?;

        Ok(ValidatorSubsetInfo {
            validators,
            short_hash,
            #[cfg(feature = "venom")]
            collator_range: None,
        })
    }

    #[cfg(feature = "venom")]
    pub fn compute_for_workchain_venom(
        validator_set: &ton_block::ValidatorSet,
        block_id: &ton_block::BlockIdExt,
        shard_hashes: &ton_block::ShardHashes,
        cc_seqno: u32,
    ) -> Result<ValidatorSubsetInfo> {
        let possible_validators =
            compute_possible_collator_ranges(shard_hashes, &block_id.shard_id, block_id.seq_no)?;
        get_validator_descrs_by_collator_range(validator_set, cc_seqno, &possible_validators)
    }

    pub fn compute_validator_set(&self, cc_seqno: u32) -> Result<ton_block::ValidatorSet> {
        ton_block::ValidatorSet::with_cc_seqno(0, 0, 0, cc_seqno, self.validators.clone())
    }
}

#[cfg(feature = "venom")]
pub fn compute_possible_collator_ranges(
    shards: &ton_block::ShardHashes,
    shard_id: &ton_block::ShardIdent,
    block_seqno: u32,
) -> Result<Vec<ton_block::CollatorRange>> {
    let mut possible_validators: Vec<ton_block::CollatorRange> = Vec::new();

    shards.iterate_shards_for_workchain(shard_id.workchain_id(), |ident, descr| {
        let Some(collators) = descr.collators else {
            anyhow::bail!("{} has no collator info", ident);
        };

        if let Some(rng) = compute_collator_range(&collators, &ident, shard_id, block_seqno)? {
            if !possible_validators.contains(&rng) {
                possible_validators.push(rng);
            }
        }
        Ok(true)
    })?;

    Ok(possible_validators)
}

#[cfg(feature = "venom")]
fn get_validator_descrs_by_collator_range(
    vset: &ton_block::ValidatorSet,
    cc_seqno: u32,
    possible_validators: &[ton_block::CollatorRange],
) -> Result<ValidatorSubsetInfo> {
    let Some(range) = possible_validators.first() else {
        anyhow::bail!("No possible validators found");
    };

    anyhow::ensure!(
        possible_validators.len() == 1,
        "Too many validators for collating block",
    );

    let Some(validator) = vset.list().get(range.collator as usize) else {
        anyhow::bail!("No validator no {} in validator set {vset:?}", range.collator)
    };

    let mut subset = Vec::new();
    subset.push(validator.clone());

    let short_hash = ton_block::ValidatorSet::calc_subset_hash_short(subset.as_slice(), cc_seqno)?;
    Ok(ValidatorSubsetInfo {
        validators: subset,
        short_hash,
        collator_range: Some(range.clone()),
    })
}

#[cfg(feature = "venom")]
fn compute_collator_range(
    collators: &ton_block::ShardCollators,
    current_shard: &ton_block::ShardIdent,
    block_shard: &ton_block::ShardIdent,
    block_seqno: u32,
) -> Result<Option<ton_block::CollatorRange>> {
    // Current shard
    let mut shards_to_check = Vec::from([(*current_shard, collators.current.clone())]);
    // The shard was same or will be same
    if collators.prev2.is_none() {
        shards_to_check.push((*current_shard, collators.prev.clone()));
    }
    if collators.next2.is_none() {
        shards_to_check.push((*current_shard, collators.next.clone()));
    }
    // The shard was split or will be merged
    if !current_shard.is_full() {
        if collators.prev2.is_none() {
            shards_to_check.push((current_shard.merge()?, collators.prev.clone()));
        }
        if collators.next2.is_none() {
            shards_to_check.push((current_shard.merge()?, collators.next.clone()));
        }
    }
    // The shard was merged or will be split
    if current_shard.can_split() {
        let (l, r) = current_shard.split()?;
        if let Some(prev2) = &collators.prev2 {
            shards_to_check.push((l, collators.prev.clone()));
            shards_to_check.push((r, prev2.clone()));
        }
        if let Some(next2) = &collators.next2 {
            shards_to_check.push((l, collators.next.clone()));
            shards_to_check.push((r, next2.clone()));
        }
    }

    shards_to_check.retain(|(id, range)| {
        id == block_shard && range.start <= block_seqno && block_seqno <= range.finish
    });

    if let Some((id, rng)) = shards_to_check.first() {
        anyhow::ensure!(
            shards_to_check.len() == 1,
            "Impossilbe state: shard {}, block {} corresponds to two different ranges: ({},{:?}) and {:?}",
            current_shard, block_seqno, id, rng, shards_to_check.get(1)
        );

        Ok(Some((*rng).clone()))
    } else {
        Ok(None)
    }
}
