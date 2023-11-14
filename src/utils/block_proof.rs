/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use anyhow::{anyhow, Context, Result};
use everscale_types::merkle::*;
use everscale_types::models::*;
use everscale_types::prelude::*;

use super::ShardStateStuff;
use crate::utils::*;

pub type BlockProofStuffAug = WithArchiveData<BlockProofStuff>;

#[derive(Clone)]
pub struct BlockProofStuff {
    proof: BlockProof,
    is_link: bool,
    id: BlockId,
}

impl BlockProofStuff {
    pub fn deserialize(block_id: BlockId, data: &[u8], is_link: bool) -> Result<Self> {
        let proof = BocRepr::decode::<BlockProof, _>(data)?;

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

    pub fn virtualize_block_root(&self) -> Result<&DynCell> {
        let merkle_proof = self.proof.root.parse::<MerkleProofRef>()?;
        let block_virt_root = merkle_proof.cell.virtualize();

        if &self.proof.proof_for.root_hash != block_virt_root.repr_hash() {
            return Err(anyhow!(
                "merkle proof has invalid virtual hash (found: {}, expected: {})",
                block_virt_root.repr_hash(),
                self.proof.proof_for
            ));
        }

        Ok(block_virt_root)
    }

    pub fn virtualize_block(&self) -> Result<(Block, HashBytes)> {
        let cell = self.virtualize_block_root()?;
        let hash = cell.repr_hash();
        Ok((cell.parse::<Block>()?, *hash))
    }

    pub fn proof(&self) -> &BlockProof {
        &self.proof
    }

    pub fn is_link(&self) -> bool {
        self.is_link
    }

    pub fn id(&self) -> &BlockId {
        &self.id
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

    pub fn pre_check_block_proof(&self) -> Result<(Block, BlockInfo)> {
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
                virt_block_hash,
            ));
        }

        let info = virt_block.load_info()?;
        let _value_flow = virt_block.load_value_flow()?;
        let _state_update = virt_block.load_state_update()?;

        if info.version != 0 {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with incorrect block info's version {}",
                self.id,
                info.version
            ));
        }

        if info.seqno != self.id.seqno {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with seq_no {}, but {} is expected",
                self.id,
                info.seqno,
                self.id.seqno
            ));
        }

        if info.shard != self.id.shard {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with shard id {}, but {} is expected",
                self.id,
                info.shard,
                self.id.shard
            ));
        }

        if info.load_master_ref()?.is_none() != info.shard.is_masterchain() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with invalid not_master flag in block info",
                self.id,
            ));
        }

        if self.id.is_masterchain() && (info.after_merge || info.before_split || info.after_split) {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares split/merge for a masterchain block",
                self.id,
            ));
        }

        if info.after_merge && info.after_split {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares both after merge and after split flags",
                self.id,
            ));
        }

        if info.after_split && (info.shard.is_full()) {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_split flag and non zero shard prefix",
                self.id,
            ));
        }

        if info.after_merge && !info.shard.can_split() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof with a block info which declares both after_merge flag and shard prefix which can't split anymore",
                self.id,
            ));
        }

        if info.key_block && !self.id.is_masterchain() {
            return Err(anyhow!(
                "proof for block {} contains a Merkle proof which declares non master chain but key block",
                self.id(),
            ));
        }

        Ok((virt_block, info))
    }

    fn check_signatures(&self, subset: &ValidatorSubsetInfo) -> Result<()> {
        // Prepare
        let Some(signatures) = self.proof.signatures.as_ref() else {
            anyhow::bail!(
                "Proof for {} doesn't have signatures to check",
                self.id,
            );
        };

        anyhow::ensure!(
            signatures.validator_info.validator_list_hash_short == subset.short_hash,
            "Bad validator set hash in proof for block {}, calculated: {}, found: {}",
            self.id,
            subset.short_hash,
            signatures.validator_info.validator_list_hash_short
        );

        let expected_count = signatures.signature_count as usize;

        {
            let mut count = 0usize;
            for value in signatures.signatures.raw_values() {
                value?;
                count += 1;
                if count > expected_count {
                    break;
                }
            }

            if expected_count != count {
                return Err(anyhow!(
                    "Proof for {}: signature count mismatch: declared: {}, calculated: {}",
                    self.id,
                    expected_count,
                    count,
                ));
            }
        }

        // Check signatures
        let checked_data = Block::build_data_for_sign(&self.id);
        let total_weight: u64 = subset.validators.iter().map(|v| v.weight).sum();
        let weight = signatures
            .signatures
            .check_signatures(&subset.validators, &checked_data)
            .map_err(|e| anyhow!("Proof for {}: error while check signatures: {}", self.id, e))?;

        // Check weight
        if weight != signatures.total_weight {
            return Err(anyhow!(
                "Proof for {}: total signature weight mismatch: declared: {}, calculated: {}",
                self.id,
                signatures.total_weight,
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

    fn pre_check_key_block_proof(&self, virt_block: &Block) -> Result<()> {
        let extra = virt_block.load_extra()?;
        let Some(mc_extra) = extra.load_custom()? else {
            anyhow::bail!(
                "proof for key block {} contains a Merkle proof without masterchain block extra",
                self.id(),
            )
        };

        let Some(config) = mc_extra.config.as_ref() else {
            anyhow::bail!(
                "proof for key block {} contains a Merkle proof without config params",
                self.id(),
            )
        };

        if config.get::<ConfigParam34>()?.is_none() {
            anyhow::bail!(
                "proof for key block {} contains a Merkle proof without current validators config param",
                self.id(),
            );
        }

        for param in 32..=38 {
            if let Some(mut vset) = config.get_raw(param)? {
                ValidatorSet::load_from(&mut vset)?;
            }
        }
        let _catchain_config = config.get::<ConfigParam28>()?;

        Ok(())
    }

    fn process_given_state(
        &self,
        master_state: &ShardStateStuff,
        block_info: &BlockInfo,
    ) -> Result<ValidatorSubsetInfo> {
        anyhow::ensure!(
            master_state.block_id().is_masterchain(),
            "Can't check proof for {}: given state {} doesn't belong masterchain",
            self.id,
            master_state.block_id()
        );

        anyhow::ensure!(
            self.id().is_masterchain(),
            "Can't check proof for non master block {} using master state",
            self.id,
        );

        anyhow::ensure!(
            block_info.prev_key_block_seqno <= master_state.block_id().seqno,
            "Can't check proof for block {} using master state {}, because it is older than the previous key block with seqno {}",
            self.id,
            master_state.block_id(),
            block_info.prev_key_block_seqno,
        );

        anyhow::ensure!(
            master_state.block_id().seqno < self.id.seqno,
            "Can't check proof for block {} using newer master state {}",
            self.id,
            master_state.block_id(),
        );

        let (validator_set, catchain_config) = {
            let custom = master_state
                .state()
                .load_custom()?
                .context("No custom found")?;
            let validator_set = custom.config.get_current_validator_set()?;
            let catchain_config = custom.config.get_catchain_config()?;
            (validator_set, catchain_config)
        };

        self.calc_validators_subset_standard(&validator_set, &catchain_config)
    }

    fn process_prev_key_block_proof(
        &self,
        prev_key_block_proof: &BlockProofStuff,
    ) -> Result<ValidatorSubsetInfo> {
        let (virt_key_block, prev_key_block_info) = prev_key_block_proof.pre_check_block_proof()?;

        anyhow::ensure!(
            prev_key_block_info.key_block,
            "Proof for key block {} contains a Merkle proof which declares non key block",
            prev_key_block_proof.id,
        );

        let (validator_set, catchain_config) = {
            let extra = virt_key_block.load_extra()?;
            let custom = extra.load_custom()?.context("No custom found")?;
            let config = custom.config.as_ref().context("No config found")?;

            let validator_set = config.get_current_validator_set()?;
            let catchain_config = config.get_catchain_config()?;
            (validator_set, catchain_config)
        };

        self.calc_validators_subset_standard(&validator_set, &catchain_config)
    }

    fn calc_validators_subset_standard(
        &self,
        validator_set: &ValidatorSet,
        catchain_config: &CatchainConfig,
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
    virt_block: &Block,
    virt_block_info: &BlockInfo,
) -> Result<()> {
    anyhow::ensure!(
        proof.id.is_masterchain(),
        "Can't verify non masterchain block {} using previous key masterchain block",
        proof.id,
    );

    anyhow::ensure!(
        prev_key_block_proof.id.is_masterchain(),
        "Invalid previous key block: it's id {} doesn't belong to the masterchain",
        prev_key_block_proof.id
    );

    let prev_key_block_seqno = virt_block_info.prev_key_block_seqno;
    anyhow::ensure!(
        prev_key_block_proof.id.seqno == prev_key_block_seqno,
        "Can't verify block {} using key block {} because the block declares different previous key block seqno {}",
        proof.id,
        prev_key_block_proof.id,
        prev_key_block_seqno,
    );

    anyhow::ensure!(
        prev_key_block_proof.id.seqno < proof.id.seqno,
        "Can't verify block {} using key block {} with larger or equal seqno",
        proof.id,
        prev_key_block_proof.id
    );

    let subset = proof.process_prev_key_block_proof(prev_key_block_proof)?;

    if virt_block_info.key_block {
        proof.pre_check_key_block_proof(virt_block)?;
    }

    proof.check_signatures(&subset)
}

pub fn check_with_master_state(
    proof: &BlockProofStuff,
    master_state: &ShardStateStuff,
    virt_block: &Block,
    virt_block_info: &BlockInfo,
) -> Result<()> {
    if virt_block_info.key_block {
        proof.pre_check_key_block_proof(virt_block)?;
    }

    let subset = proof.process_given_state(master_state, virt_block_info)?;
    proof.check_signatures(&subset)
}

#[derive(Clone, Debug)]
pub struct ValidatorSubsetInfo {
    pub validators: Vec<ValidatorDescription>,
    pub short_hash: u32,
    #[cfg(feature = "venom")]
    pub collator_range: Option<ton_block::CollatorRange>,
}

impl ValidatorSubsetInfo {
    pub fn compute_standard(
        validator_set: &ValidatorSet,
        block_id: &BlockId,
        catchain_config: &CatchainConfig,
        cc_seqno: u32,
    ) -> Result<Self> {
        let (validators, short_hash) = validator_set
            .compute_subset(block_id.shard, catchain_config, cc_seqno)
            .context("Failed to compute validator subset")?;

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

    // pub fn compute_validator_set(&self, cc_seqno: u32) -> Result<ton_block::ValidatorSet> {
    //     ton_block::ValidatorSet::with_cc_seqno(0, 0, 0, cc_seqno, self.validators.clone())
    // }
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
