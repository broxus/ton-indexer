/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - removed unused package entry ids
/// - optimized serialization/deserialization
///
use std::borrow::Borrow;
use std::hash::Hash;
use std::str::FromStr;

use anyhow::Result;
use smallvec::SmallVec;

use super::{BlockIdShort, StoredValue};

/// Package entry id
#[derive(Debug, Hash, Eq, PartialEq)]
pub enum PackageEntryId<I> {
    /// Block data entry
    Block(I),
    /// Block proof entry
    Proof(I),
    /// Block proof link entry
    ProofLink(I),
}

impl PackageEntryId<ton_block::BlockIdExt> {
    /// Parses package entry id from string
    pub fn from_filename(filename: &str) -> Result<Self> {
        let block_id_pos = match filename.find('(') {
            Some(pos) => pos,
            None => return Err(PackageEntryIdError::InvalidFileName.into()),
        };

        let (prefix, block_id) = filename.split_at(block_id_pos);

        Ok(match prefix {
            PACKAGE_ENTRY_BLOCK => Self::Block(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF => Self::Proof(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF_LINK => Self::ProofLink(parse_block_id(block_id)?),
            _ => return Err(PackageEntryIdError::InvalidFileName.into()),
        })
    }
}

impl<I> PackageEntryId<I>
where
    I: Borrow<ton_block::BlockIdExt>,
{
    /// Returns package entry prefix
    fn filename_prefix(&self) -> &'static str {
        match self {
            Self::Block(_) => PACKAGE_ENTRY_BLOCK,
            Self::Proof(_) => PACKAGE_ENTRY_PROOF,
            Self::ProofLink(_) => PACKAGE_ENTRY_PROOF_LINK,
        }
    }

    /// Constructs on-stack buffer with the serialized object
    pub fn to_vec(&self) -> SmallVec<[u8; BlockIdShort::SIZE_HINT + 32 + 1]> {
        let mut result = SmallVec::with_capacity(BlockIdShort::SIZE_HINT + 32 + 1);
        let (block_id, ty) = match self {
            Self::Block(id) => (id, 0),
            Self::Proof(id) => (id, 1),
            Self::ProofLink(id) => (id, 2),
        };
        let block_id = block_id.borrow();

        result.extend_from_slice(&block_id.shard_id.workchain_id().to_be_bytes());
        result.extend_from_slice(&block_id.shard_id.shard_prefix_with_tag().to_be_bytes());
        result.extend_from_slice(&block_id.seq_no.to_be_bytes());
        result.extend_from_slice(block_id.root_hash.as_slice());
        result.push(ty);

        result
    }
}

pub trait GetFileName {
    /// Returns string representation of the package entry id
    fn filename(&self) -> String;
}

impl GetFileName for ton_block::BlockIdExt {
    fn filename(&self) -> String {
        format!(
            "({},{:016x},{}):{}:{}",
            self.shard_id.workchain_id(),
            self.shard_id.shard_prefix_with_tag(),
            self.seq_no,
            hex::encode_upper(self.root_hash.as_slice()),
            hex::encode_upper(self.file_hash.as_slice())
        )
    }
}

impl<I> GetFileName for PackageEntryId<I>
where
    I: Borrow<ton_block::BlockIdExt> + Hash,
{
    fn filename(&self) -> String {
        match self {
            Self::Block(block_id) | Self::Proof(block_id) | Self::ProofLink(block_id) => {
                format!("{}{}", self.filename_prefix(), block_id.borrow().filename())
            }
        }
    }
}

fn parse_block_id(filename: &str) -> Result<ton_block::BlockIdExt> {
    let mut parts = filename.split(':');

    let shard_id = match parts.next() {
        Some(part) => part,
        None => return Err(PackageEntryIdError::ShardIdNotFound.into()),
    };

    let mut shard_id_parts = shard_id.split(',');
    let workchain_id = match shard_id_parts
        .next()
        .and_then(|part| part.strip_prefix('('))
    {
        Some(part) => i32::from_str(part)?,
        None => return Err(PackageEntryIdError::WorkchainIdNotFound.into()),
    };

    let shard_prefix_tagged = match shard_id_parts.next() {
        Some(part) => u64::from_str_radix(part, 16)?,
        None => return Err(PackageEntryIdError::ShardPrefixNotFound.into()),
    };

    let seq_no = match shard_id_parts
        .next()
        .and_then(|part| part.strip_suffix(')'))
    {
        Some(part) => u32::from_str(part)?,
        None => return Err(PackageEntryIdError::SeqnoNotFound.into()),
    };

    let shard_id = ton_block::ShardIdent::with_tagged_prefix(workchain_id, shard_prefix_tagged)?;

    let root_hash = match parts.next() {
        Some(part) => ton_types::UInt256::from_str(part)?,
        None => return Err(PackageEntryIdError::RootHashNotFound.into()),
    };

    let file_hash = match parts.next() {
        Some(part) => ton_types::UInt256::from_str(part)?,
        None => return Err(PackageEntryIdError::FileHashNotFound.into()),
    };

    Ok(ton_block::BlockIdExt {
        shard_id,
        seq_no,
        root_hash,
        file_hash,
    })
}

const PACKAGE_ENTRY_BLOCK: &str = "block_";
const PACKAGE_ENTRY_PROOF: &str = "proof_";
const PACKAGE_ENTRY_PROOF_LINK: &str = "prooflink_";

#[derive(thiserror::Error, Debug)]
enum PackageEntryIdError {
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Shard id not found")]
    ShardIdNotFound,
    #[error("Workchain id not found")]
    WorkchainIdNotFound,
    #[error("Shard prefix not found")]
    ShardPrefixNotFound,
    #[error("Seqno not found")]
    SeqnoNotFound,
    #[error("Root hash not found")]
    RootHashNotFound,
    #[error("File hash not found")]
    FileHashNotFound,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_load() {
        fn check_package_id(package_id: PackageEntryId<ton_block::BlockIdExt>) {
            assert_eq!(
                PackageEntryId::from_filename(&package_id.filename()).unwrap(),
                package_id
            );
        }

        let block_id = ton_block::BlockIdExt {
            shard_id: ton_block::ShardIdent::masterchain(),
            seq_no: rand::random(),
            root_hash: ton_types::UInt256::rand(),
            file_hash: ton_types::UInt256::rand(),
        };

        check_package_id(PackageEntryId::Block(block_id.clone()));
        check_package_id(PackageEntryId::Proof(block_id.clone()));
        check_package_id(PackageEntryId::ProofLink(block_id));
    }
}
