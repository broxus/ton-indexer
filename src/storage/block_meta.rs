use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use nekoton_utils::NoFailure;
use ton_types::ByteOrderRead;

use super::StoredValue;

#[derive(Debug, Default)]
pub struct BlockMeta {
    flags: AtomicU64,
    gen_utime: u32,
    gen_lt: u64,
}

impl BlockMeta {
    pub fn from_block(block: &ton_block::Block) -> Result<Self> {
        let info = block.read_info().convert()?;
        let flags = if info.key_block() {
            BLOCK_META_FLAG_IS_KEY_BLOCK
        } else {
            0
        };
        Ok(Self::with_data(
            flags,
            info.gen_utime().0,
            info.start_lt(),
            0,
        ))
    }

    pub fn with_data(flags: u64, gen_utime: u32, gen_lt: u64, masterchain_ref_seqno: u32) -> Self {
        Self {
            flags: AtomicU64::new(flags | masterchain_ref_seqno as u64),
            gen_utime,
            gen_lt,
        }
    }

    pub fn masterchain_ref_seqno(&self) -> u32 {
        self.flags.load(Ordering::Acquire) as u32
    }

    pub fn set_masterchain_ref_seqno(&self, seqno: u32) -> u32 {
        self.flags.fetch_or(seqno as u64, Ordering::Release) as u32
    }

    pub fn gen_utime(&self) -> u32 {
        self.gen_utime
    }

    pub fn gen_lt(&self) -> u64 {
        self.gen_lt
    }

    pub fn set_has_data(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_DATA)
    }

    pub fn has_data(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_DATA)
    }

    pub fn set_has_proof(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PROOF)
    }

    pub fn has_proof(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PROOF)
    }

    pub fn set_has_proof_link(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PROOF_LINK)
    }

    pub fn has_proof_link(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PROOF_LINK)
    }

    pub fn set_has_state(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_STATE)
    }

    pub fn has_state(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_STATE)
    }

    #[allow(unused)]
    pub fn set_has_persistent_state(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PERSISTENT_STATE)
    }

    #[allow(unused)]
    pub fn has_persistent_state(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PERSISTENT_STATE)
    }

    pub fn set_has_next1(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_NEXT_1)
    }

    pub fn has_next1(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_NEXT_1)
    }

    pub fn set_has_next2(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_NEXT_2)
    }

    pub fn has_next2(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_NEXT_2)
    }

    pub fn set_has_prev1(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PREV_1)
    }

    pub fn has_prev1(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PREV_1)
    }

    pub fn set_has_prev2(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_HAS_PREV_2)
    }

    pub fn has_prev2(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_HAS_PREV_2)
    }

    pub fn set_is_applied(&self) -> bool {
        self.set_flag(BLOCK_META_FLAG_IS_APPLIED)
    }

    pub fn is_applied(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_IS_APPLIED)
    }

    pub fn is_key_block(&self) -> bool {
        self.test_flag(BLOCK_META_FLAG_IS_KEY_BLOCK)
    }

    fn test_flag(&self, flag: u64) -> bool {
        self.flags.load(Ordering::Acquire) & flag == flag
    }

    fn set_flag(&self, flag: u64) -> bool {
        self.flags.fetch_or(flag, Ordering::Release) & flag != flag
    }
}

impl StoredValue for BlockMeta {
    fn size_hint(&self) -> Option<usize> {
        Some(8 + 4 + 8)
    }

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        const FLAGS_MASK: u64 = 0x0000_ffff_ffff_ffff;
        let flags = self.flags.load(Ordering::Acquire) & FLAGS_MASK;

        writer.write_all(&flags.to_le_bytes())?;
        writer.write_all(&self.gen_utime.to_le_bytes())?;
        writer.write_all(&self.gen_lt.to_le_bytes())?;

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let flags = reader.read_le_u64()?;
        let gen_utime = reader.read_le_u32()?;
        let gen_lt = reader.read_le_u64()?;

        Ok(Self {
            flags: AtomicU64::new(flags),
            gen_utime,
            gen_lt,
        })
    }
}

const BLOCK_META_FLAG_HAS_DATA: u64 = 1 << 32;
const BLOCK_META_FLAG_HAS_PROOF: u64 = 1 << (32 + 1);
const BLOCK_META_FLAG_HAS_PROOF_LINK: u64 = 1 << (32 + 2);
// skip flag 3 (processed by external listener)
const BLOCK_META_FLAG_HAS_STATE: u64 = 1 << (32 + 4);
const BLOCK_META_FLAG_HAS_PERSISTENT_STATE: u64 = 1 << (32 + 5);
const BLOCK_META_FLAG_HAS_NEXT_1: u64 = 1 << (32 + 6);
const BLOCK_META_FLAG_HAS_NEXT_2: u64 = 1 << (32 + 7);
const BLOCK_META_FLAG_HAS_PREV_1: u64 = 1 << (32 + 8);
const BLOCK_META_FLAG_HAS_PREV_2: u64 = 1 << (32 + 9);
const BLOCK_META_FLAG_IS_APPLIED: u64 = 1 << (32 + 10);
const BLOCK_META_FLAG_IS_KEY_BLOCK: u64 = 1 << (32 + 11);
// skip flag 12 (?)
// const BLOCK_META_FLAG_MOVED_TO_ARCHIVE: u64 = 1 << (32 + 13);
// const BLOCK_META_FLAG_INDEXED: u64 = 1 << (32 + 14);
// skip flag 15 (?)
