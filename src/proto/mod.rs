use bytes::Bytes;
use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

#[derive(Copy, Clone, TlWrite)]
#[tl(boxed, id = 0x3d1b1867)]
pub struct ExternalMessageBroadcast<'tl> {
    pub data: &'tl [u8],
}

#[derive(Clone, TlRead)]
#[tl(boxed, id = 0xae2e1105)]
pub struct BlockBroadcast {
    #[tl(with = "tl_block_id")]
    pub id: ton_block::BlockIdExt,
    pub catchain_seqno: u32,
    pub validator_set_hash: u32,
    #[tl(with = "tl_signature_pair_vec")]
    pub signatures: Vec<ton_block::CryptoSignaturePair>,
    pub proof: Bytes,
    pub data: Bytes,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x1455b0f3)]
pub struct RpcGetNextBlockDescription {
    #[tl(with = "tl_block_id")]
    pub prev_block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed)]
pub enum BlockDescription {
    #[tl(id = 0x46a1d088)]
    Found {
        #[tl(with = "tl_block_id")]
        id: ton_block::BlockIdExt,
    },
    #[tl(id = 0x8384ae95)]
    Empty,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x875c3308)]
pub struct RpcPrepareBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
    pub allow_partial: bool,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x77364c38)]
pub struct RpcPrepareKeyBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
    pub allow_partial: bool,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x75a37f4e)]
pub struct RpcPrepareBlock {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0xfeea269e)]
pub struct RpcPreparePersistentState {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
    #[tl(with = "tl_block_id")]
    pub masterchain_block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0xf5e9e6e3)]
pub struct RpcDownloadPersistentStateSlice {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
    #[tl(with = "tl_block_id")]
    pub masterchain_block: ton_block::BlockIdExt,
    pub offset: u64,
    pub max_size: u64,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x41ce0825)]
pub struct RpcPrepareZeroState {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0xadcc1e5a)]
pub struct RpcDownloadZeroState {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0xf2e7cfbb)]
pub struct RpcGetNextKeyBlockIds {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
    pub max_size: u32,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x6ea0374a)]
pub struct RpcDownloadNextBlockFull {
    #[tl(with = "tl_block_id")]
    pub prev_block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x6a27c49d)]
pub struct RpcDownloadBlockFull {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0xe27279c3)]
pub struct RpcDownloadBlock {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x4bd6478a)]
pub struct RpcDownloadBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0xec23483a)]
pub struct RpcDownloadKeyBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x25b300c6)]
pub struct RpcDownloadBlockProofLink {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x12e42ad2)]
pub struct RpcDownloadKeyBlockProofLink {
    #[tl(with = "tl_block_id")]
    pub block: ton_block::BlockIdExt,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x7b2dd941)]
pub struct RpcGetArchiveInfo {
    pub masterchain_seqno: u32,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x203b5168, size_hint = 20)]
pub struct RpcGetArchiveSlice {
    pub archive_id: u64,
    pub offset: u64,
    pub max_size: u32,
}

#[derive(TlWrite, TlRead)]
#[tl(boxed, id = 0xdee618f8)]
pub struct RpcGetCapabilities;

#[derive(Copy, Clone, Eq, PartialEq, TlRead, TlWrite)]
#[tl(boxed)]
pub enum PreparedProof {
    #[tl(id = 0x899f9a4b)]
    Found,
    #[tl(id = 0xc769c17a)]
    Empty,
    #[tl(id = 0x3dff328d)]
    Link,
}

#[derive(Copy, Clone, Eq, PartialEq, TlRead, TlWrite)]
#[tl(boxed)]
pub enum Prepared {
    #[tl(id = 0xe2c33da6)]
    NotFound,
    #[tl(id = 0xeac4bbcd)]
    Found,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, TlRead, TlWrite)]
#[tl(boxed)]
pub enum PreparedState {
    #[tl(id = 0x32390a51)]
    NotFound,
    #[tl(id = 0x375bcb6d)]
    Found,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed)]
pub enum DataFull {
    #[tl(id = 0xbe589f93)]
    Found {
        #[tl(with = "tl_block_id")]
        block_id: ton_block::BlockIdExt,
        proof: Bytes,
        block: Bytes,
        is_link: bool,
    },
    #[tl(id = 0x576e85ca)]
    Empty,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = 0x07664d59)]
pub struct KeyBlocks {
    #[tl(with = "tl_block_id_vec")]
    pub blocks: Vec<ton_block::BlockIdExt>,
    pub incomplete: bool,
    pub error: bool,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed)]
pub enum ArchiveInfo {
    #[tl(id = 0x19efff8c, size_hint = 8)]
    Found { id: u64 },
    #[tl(id = 0x99291683)]
    NotFound,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, TlWrite, TlRead)]
#[tl(boxed, id = 0xf5bf60c0)]
pub struct Capabilities {
    pub version: u32,
    pub capabilities: u64,
}

mod tl_signature_pair_vec {
    use super::*;

    pub fn read(
        packet: &[u8],
        offset: &mut usize,
    ) -> TlResult<Vec<ton_block::CryptoSignaturePair>> {
        let len = u32::read_from(packet, offset)?;
        if *offset + len as usize * tl_signature_pair::SIZE_HINT > packet.len() {
            return Err(TlError::UnexpectedEof);
        }
        let mut pairs = Vec::with_capacity(len as usize);
        for _ in 0..len {
            pairs.push(tl_signature_pair::read(packet, offset)?);
        }
        Ok(pairs)
    }
}

mod tl_signature_pair {
    use super::*;

    pub const SIZE_HINT: usize = 32 + 68;

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<ton_block::CryptoSignaturePair> {
        let node_id_short = <[u8; 32]>::read_from(packet, offset)?;
        let sign = ton_block::CryptoSignature::from_bytes(<&[u8]>::read_from(packet, offset)?)
            .map_err(|_| TlError::InvalidData)?;

        Ok(ton_block::CryptoSignaturePair {
            node_id_short: node_id_short.into(),
            sign,
        })
    }
}

mod tl_block_id_vec {
    use super::*;

    pub fn size_hint(ids: &[ton_block::BlockIdExt]) -> usize {
        4 + ids.len() * tl_block_id::SIZE_HINT
    }

    pub fn write<P: TlPacket>(blocks: &[ton_block::BlockIdExt], packet: &mut P) {
        packet.write_u32(blocks.len() as u32);
        for block in blocks {
            tl_block_id::write(block, packet);
        }
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Vec<ton_block::BlockIdExt>> {
        let len = u32::read_from(packet, offset)?;
        if *offset + len as usize * tl_block_id::SIZE_HINT > packet.len() {
            return Err(TlError::UnexpectedEof);
        }

        let mut ids = Vec::with_capacity(len as usize);
        for _ in 0..len {
            ids.push(tl_block_id::read(packet, offset)?);
        }
        Ok(ids)
    }
}

mod tl_block_id {
    use super::*;

    pub const SIZE_HINT: usize = 80;

    pub const fn size_hint(_: &ton_block::BlockIdExt) -> usize {
        SIZE_HINT
    }

    pub fn write<P: TlPacket>(block: &ton_block::BlockIdExt, packet: &mut P) {
        packet.write_i32(block.shard_id.workchain_id());
        packet.write_u64(block.shard_id.shard_prefix_with_tag());
        packet.write_u32(block.seq_no);
        packet.write_raw_slice(block.root_hash.as_slice());
        packet.write_raw_slice(block.file_hash.as_slice());
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<ton_block::BlockIdExt> {
        let shard_id = ton_block::ShardIdent::with_tagged_prefix(
            i32::read_from(packet, offset)?,
            u64::read_from(packet, offset)?,
        )
        .map_err(|_| TlError::InvalidData)?;
        let seq_no = u32::read_from(packet, offset)?;
        let root_hash = <[u8; 32]>::read_from(packet, offset)?;
        let file_hash = <[u8; 32]>::read_from(packet, offset)?;

        Ok(ton_block::BlockIdExt {
            shard_id,
            seq_no,
            root_hash: root_hash.into(),
            file_hash: file_hash.into(),
        })
    }
}
