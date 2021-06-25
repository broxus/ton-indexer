use anyhow::{anyhow, Result};
use ton_api::ton;
use ton_block::{Deserializable, Serializable};
use ton_types::Cell;

use super::{BlockIdExtExtension, NoFailure};

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockProofStuff {
    proof: ton_block::BlockProof,
    root: Cell,
    is_link: bool,
    id: ton_block::BlockIdExt,
    data: Vec<u8>,
}

impl BlockProofStuff {
    pub fn deserialize(
        block_id: ton_block::BlockIdExt,
        data: Vec<u8>,
        is_link: bool,
    ) -> Result<Self> {
        let root =
            ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(&data)).convert()?;
        let proof = ton_block::BlockProof::construct_from(&mut root.clone().into()).convert()?;

        if proof.proof_for != block_id {
            return Err(anyhow!(
                "proof for another block (found: {}, expected: {})",
                proof.proof_for,
                block_id
            ));
        }

        if !block_id.is_masterchain() && !is_link {
            return Err(anyhow!("proof for non-masterchain block {}", block_id));
        }

        Ok(Self {
            proof,
            root,
            is_link,
            id: block_id,
            data,
        })
    }

    pub fn new(proof: ton_block::BlockProof, is_link: bool) -> Result<Self> {
        let id = proof.proof_for.clone();
        if !id.is_masterchain() && !is_link {
            return Err(anyhow!("proof for non-masterchain block {}", id));
        }

        let cell = proof.write_to_new_cell().convert()?.into();
        let mut data = Vec::new();
        ton_types::serialize_tree_of_cells(&cell, &mut data).convert()?;

        Ok(Self {
            root: proof.write_to_new_cell().convert()?.into(),
            proof,
            is_link,
            id,
            data,
        })
    }

    pub fn virtualize_block_root(&self) -> Result<Cell> {
        let merkle_proof =
            ton_block::MerkleProof::construct_from(&mut self.proof.root.clone().into())
                .convert()?;
        let block_virt_root = merkle_proof.proof.clone().virtualize(1);

        if *self.proof.proof_for.root_hash() != block_virt_root.repr_hash() {
            return Err(anyhow!(
                "merkle proof has invalid virtual hash (found: {}, expected: {})",
                block_virt_root.repr_hash(),
                self.proof.proof_for
            ));
        }

        Ok(block_virt_root)
    }

    pub fn virtualize_block(&self) -> Result<(ton_block::Block, ton_api::ton::int256)> {
        let cell = self.virtualize_block_root()?;
        let hash = ton_api::ton::int256(cell.repr_hash().as_slice().to_owned());
        Ok((
            ton_block::Block::construct_from(&mut cell.into()).convert()?,
            hash,
        ))
    }

    pub fn is_link(&self) -> bool {
        self.is_link
    }

    pub fn id(&self) -> &ton_block::BlockIdExt {
        &self.id
    }

    pub fn proof(&self) -> &ton_block::BlockProof {
        &self.proof
    }

    pub fn proof_root(&self) -> &Cell {
        &self.proof.root
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    fn pre_check_key_block_proof(&self, virt_block: &ton_block::Block) -> Result<()> {
        let extra = virt_block.read_extra().convert()?;
        let mc_extra = extra.read_custom().convert()?.ok_or_else(|| {
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

        let _cur_validator_set = config.config(34).convert()?
            .ok_or_else(|| anyhow!(
                "proof for key block {} contains a Merkle proof without current validators config param (34)",
                self.id(),
            ))?;
        for param in 32..=38 {
            let _val_set = config.config(param).convert()?;
        }
        let _catchain_config = config.config(28).convert()?;

        Ok(())
    }
}
