use std::io::Write;
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::NoFailure;
use parking_lot::RwLock;
use ton_api::ton;

use super::block_handle::*;
use super::StoredValue;
use crate::utils::*;

pub struct BlockIndexDb {
    lt_desc_db: RwLock<LtDescDb>,
    lt_db: LtDb,
}

impl BlockIndexDb {
    pub fn with_db(lt_desc_db: Tree, lt_db: Tree) -> Self {
        Self {
            lt_desc_db: RwLock::new(LtDescDb { db: lt_desc_db }),
            lt_db: LtDb { db: lt_db },
        }
    }

    pub fn get_block_by_seq_no(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        seq_no: u32,
    ) -> Result<ton_block::BlockIdExt> {
        self.get_block(
            account_prefix,
            |lt_desc| seq_no.cmp(&lt_desc.last_seq_no),
            |entry| seq_no.cmp(&(entry.block_id_ext.seqno as u32)),
            true,
        )
    }

    pub fn get_block_by_utime(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        utime: u32,
    ) -> Result<ton_block::BlockIdExt> {
        self.get_block(
            account_prefix,
            |lt_desc| utime.cmp(&lt_desc.last_utime),
            |entry| utime.cmp(&entry.gen_utime),
            false,
        )
    }

    pub fn get_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<ton_block::BlockIdExt> {
        self.get_block(
            account_prefix,
            |lt_desc| lt.cmp(&lt_desc.last_lt),
            |entry| lt.cmp(&entry.gen_lt),
            false,
        )
    }

    fn get_block<FCmpDesc, FCmpEntry>(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        compare_lt_desc: FCmpDesc,
        compare_lt_entry: FCmpEntry,
        exact: bool,
    ) -> Result<ton_block::BlockIdExt>
    where
        FCmpDesc: Fn(&LtDesc) -> std::cmp::Ordering,
        FCmpEntry: Fn(&LtDbEntry) -> std::cmp::Ordering,
    {
        let mut found = false;
        let mut result: Option<ton_block::BlockIdExt> = None;
        let mut index_range_begin = 0;

        for prefix_len in 0..=ton_block::MAX_SPLIT_DEPTH {
            let shard = ton_block::ShardIdent::with_prefix_len(
                prefix_len,
                account_prefix.workchain_id,
                account_prefix.prefix,
            )
            .convert()?;

            let lt_desc_key = shard.to_vec()?;
            let lt_desc = match self.lt_desc_db.read().try_load_lt_desc(&lt_desc_key)? {
                Some(lt_desc) => lt_desc,
                None if found => break,
                None if shard.workchain_id() == ton_block::MASTERCHAIN_ID => {
                    return Err(BlockIndexDbError::BlockNotFound.into())
                }
                None => continue,
            };

            found = true;

            if compare_lt_desc(&lt_desc) == std::cmp::Ordering::Greater {
                continue;
            }

            let mut first_index = lt_desc.first_index;
            let mut first_block_id = None;
            let mut last_index = lt_desc.last_index + 1;
            let mut last_block_id = None;

            let mut previous_index = u32::MAX;
            while last_index > first_index {
                let index = first_index + (last_index - first_index) / 2;
                if index == previous_index {
                    break;
                }
                previous_index = index;

                let entry = self.lt_db.load(LtDbKey {
                    shard_ident: &shard,
                    index,
                })?;
                let block_id = convert_block_id_ext_api2blk(&entry.block_id_ext)?;
                match compare_lt_entry(&entry) {
                    std::cmp::Ordering::Equal => return Ok(block_id),
                    std::cmp::Ordering::Less => {
                        last_block_id = Some(block_id);
                        last_index = index;
                    }
                    std::cmp::Ordering::Greater => {
                        first_block_id = Some(block_id);
                        first_index = index;
                    }
                }
            }

            if let Some(last_block_id) = last_block_id {
                if let Some(result) = &mut result {
                    if result.seq_no > last_block_id.seq_no as u32 {
                        *result = last_block_id;
                    }
                } else {
                    result = Some(last_block_id);
                }
            }

            if let Some(first_block_id) = first_block_id {
                if index_range_begin < first_block_id.seq_no {
                    index_range_begin = first_block_id.seq_no;
                }
            }

            if let Some(result) = &mut result {
                if result.seq_no == index_range_begin + 1 {
                    if exact {
                        return Err(BlockIndexDbError::BlockNotFound.into());
                    }

                    return Ok(result.clone());
                }
            }
        }

        if let Some(result) = result {
            if !exact {
                return Ok(result);
            }
        }

        Err(BlockIndexDbError::BlockNotFound.into())
    }

    pub fn add_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        let lt_desc_key = handle.id().shard_id.to_vec()?;

        let lt_desc_db = self.lt_desc_db.write();

        let index = match lt_desc_db.try_load_lt_desc(&lt_desc_key)? {
            Some(desc) => match handle.id().seq_no.cmp(&desc.last_seq_no) {
                std::cmp::Ordering::Equal => return Ok(()),
                std::cmp::Ordering::Greater => desc.last_index + 1,
                std::cmp::Ordering::Less => {
                    return Err(BlockIndexDbError::AscendingOrderRequired.into())
                }
            },
            None => 1,
        };

        self.lt_db.store(
            LtDbKey {
                shard_ident: handle.id().shard(),
                index,
            },
            &LtDbEntry {
                block_id_ext: convert_block_id_ext_blk2api(handle.id()),
                gen_lt: handle.meta().gen_lt(),
                gen_utime: handle.meta().gen_utime(),
            },
        )?;

        lt_desc_db.store_lt_desc(
            &lt_desc_key,
            &LtDesc {
                first_index: 1,
                last_index: index,
                last_seq_no: handle.id().seq_no,
                last_lt: handle.meta().gen_lt(),
                last_utime: handle.meta().gen_utime(),
            },
        )?;

        Ok(())
    }
}

struct LtDb {
    db: Tree,
}

impl LtDb {
    fn load(&self, key: LtDbKey<'_>) -> Result<LtDbEntry> {
        match self.db.get(&key.to_vec()?)? {
            Some(value) => Ok(bincode::deserialize(&value)?),
            None => Err(BlockIndexDbError::LtDbEntryNotFound.into()),
        }
    }

    fn store(&self, key: LtDbKey<'_>, value: &LtDbEntry) -> Result<()> {
        self.db.insert(key.to_vec()?, bincode::serialize(&value)?)?;
        Ok(())
    }
}

struct LtDbKey<'a> {
    shard_ident: &'a ton_block::ShardIdent,
    index: u32,
}

impl<'a> LtDbKey<'a> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.shard_ident.serialize(writer)?;
        writer.write_all(&self.index.to_le_bytes())?;
        Ok(())
    }

    fn to_vec(&self) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(4 + 8 + 4);
        self.serialize(&mut result)?;
        Ok(result)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LtDbEntry {
    block_id_ext: ton::ton_node::blockidext::BlockIdExt,
    gen_lt: u64,
    gen_utime: u32,
}

struct LtDescDb {
    db: Tree,
}

impl LtDescDb {
    fn try_load_lt_desc(&self, key: &[u8]) -> Result<Option<LtDesc>> {
        Ok(match self.db.get(key)? {
            Some(value) => Some(bincode::deserialize(&value)?),
            None => None,
        })
    }

    fn store_lt_desc(&self, key: &[u8], lt_desc: &LtDesc) -> Result<()> {
        let value = bincode::serialize(lt_desc)?;
        self.db.insert(key, value)?;
        Ok(())
    }
}

#[derive(PartialEq, serde::Serialize, serde::Deserialize)]
struct LtDesc {
    first_index: u32,
    last_index: u32,
    last_seq_no: u32,
    last_lt: u64,
    last_utime: u32,
}

#[derive(thiserror::Error, Debug)]
enum BlockIndexDbError {
    #[error("Ascending order required")]
    AscendingOrderRequired,
    #[error("Lt db entry not found")]
    LtDbEntryNotFound,
    #[error("Block not found")]
    BlockNotFound,
}
