use bytes::Bytes;
use everscale_types::models::*;
use tl_proto::{TlError, TlPacket, TlRead, TlResult, TlWrite};

#[derive(Copy, Clone, TlWrite, TlRead)]
#[tl(boxed, id = "tonNode.externalMessageBroadcast", scheme = "scheme.tl")]
pub struct ExternalMessageBroadcast<'tl> {
    pub data: &'tl [u8],
}

#[derive(Clone, TlRead)]
#[tl(boxed, id = "tonNode.blockBroadcast", scheme = "scheme.tl")]
pub struct BlockBroadcast {
    #[tl(with = "tl_block_id")]
    pub id: BlockId,
    pub catchain_seqno: u32,
    pub validator_set_hash: u32,
    #[tl(with = "tl_block_signature_vec")]
    pub signatures: Vec<BlockSignature>,
    pub proof: Bytes,
    pub data: Bytes,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.getNextBlockDescription", scheme = "scheme.tl")]
pub struct RpcGetNextBlockDescription {
    #[tl(with = "tl_block_id")]
    pub prev_block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, scheme = "scheme.tl")]
pub enum BlockDescription {
    #[tl(id = "tonNode.blockDescription")]
    Found {
        #[tl(with = "tl_block_id")]
        id: BlockId,
    },
    #[tl(id = "tonNode.blockDescriptionEmpty")]
    Empty,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.prepareBlockProof", scheme = "scheme.tl")]
pub struct RpcPrepareBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
    pub allow_partial: bool,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.prepareKeyBlockProof", scheme = "scheme.tl")]
pub struct RpcPrepareKeyBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
    pub allow_partial: bool,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.prepareBlock", scheme = "scheme.tl")]
pub struct RpcPrepareBlock {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.preparePersistentState", scheme = "scheme.tl")]
pub struct RpcPreparePersistentState {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
    #[tl(with = "tl_block_id")]
    pub masterchain_block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(
    boxed,
    id = "tonNode.downloadPersistentStateSlice",
    scheme = "scheme.tl"
)]
pub struct RpcDownloadPersistentStateSlice {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
    #[tl(with = "tl_block_id")]
    pub masterchain_block: BlockId,
    pub offset: u64,
    pub max_size: u64,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.prepareZeroState", scheme = "scheme.tl")]
pub struct RpcPrepareZeroState {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadZeroState", scheme = "scheme.tl")]
pub struct RpcDownloadZeroState {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.getNextKeyBlockIds", scheme = "scheme.tl")]
pub struct RpcGetNextKeyBlockIds {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
    pub max_size: u32,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadNextBlockFull", scheme = "scheme.tl")]
pub struct RpcDownloadNextBlockFull {
    #[tl(with = "tl_block_id")]
    pub prev_block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadBlockFull", scheme = "scheme.tl")]
pub struct RpcDownloadBlockFull {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadBlock", scheme = "scheme.tl")]
pub struct RpcDownloadBlock {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadBlockProof", scheme = "scheme.tl")]
pub struct RpcDownloadBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadKeyBlockProof", scheme = "scheme.tl")]
pub struct RpcDownloadKeyBlockProof {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadBlockProofLink", scheme = "scheme.tl")]
pub struct RpcDownloadBlockProofLink {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.downloadKeyBlockProofLink", scheme = "scheme.tl")]
pub struct RpcDownloadKeyBlockProofLink {
    #[tl(with = "tl_block_id")]
    pub block: BlockId,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(
    boxed,
    id = "tonNode.getArchiveInfo",
    size_hint = 4,
    scheme = "scheme.tl"
)]
pub struct RpcGetArchiveInfo {
    pub masterchain_seqno: u32,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(
    boxed,
    id = "tonNode.getArchiveSlice",
    size_hint = 20,
    scheme = "scheme.tl"
)]
pub struct RpcGetArchiveSlice {
    pub archive_id: u64,
    pub offset: u64,
    pub max_size: u32,
}

#[derive(TlWrite, TlRead)]
#[tl(boxed, id = "tonNode.getCapabilities", scheme = "scheme.tl")]
pub struct RpcGetCapabilities;

#[derive(Copy, Clone, Eq, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "scheme.tl")]
pub enum PreparedProof {
    #[tl(id = "tonNode.preparedProof")]
    Found,
    #[tl(id = "tonNode.preparedProofEmpty")]
    Empty,
    #[tl(id = "tonNode.preparedProofLink")]
    Link,
}

#[derive(Copy, Clone, Eq, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "scheme.tl")]
pub enum Prepared {
    #[tl(id = "tonNode.notFound")]
    NotFound,
    #[tl(id = "tonNode.prepared")]
    Found,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, TlRead, TlWrite)]
#[tl(boxed, scheme = "scheme.tl")]
pub enum PreparedState {
    #[tl(id = "tonNode.notFoundState")]
    NotFound,
    #[tl(id = "tonNode.preparedState")]
    Found,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, scheme = "scheme.tl")]
pub enum DataFull {
    #[tl(id = "tonNode.dataFull")]
    Found {
        #[tl(with = "tl_block_id")]
        block_id: BlockId,
        proof: Bytes,
        block: Bytes,
        is_link: bool,
    },
    #[tl(id = "tonNode.dataFullEmpty")]
    Empty,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, id = "tonNode.keyBlocks", scheme = "scheme.tl")]
pub struct KeyBlocks {
    #[tl(with = "tl_block_id_vec")]
    pub blocks: Vec<BlockId>,
    pub incomplete: bool,
    pub error: bool,
}

#[derive(Clone, TlRead, TlWrite)]
#[tl(boxed, scheme = "scheme.tl")]
pub enum ArchiveInfo {
    #[tl(id = "tonNode.archiveInfo", size_hint = 8)]
    Found { id: u64 },
    #[tl(id = "tonNode.archiveNotFound")]
    NotFound,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, TlWrite, TlRead)]
#[tl(
    boxed,
    id = "tonNode.capabilities",
    size_hint = 12,
    scheme = "scheme.tl"
)]
pub struct Capabilities {
    pub version: u32,
    pub capabilities: u64,
}

mod tl_block_signature_vec {
    use super::*;

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Vec<BlockSignature>> {
        let len = u32::read_from(packet, offset)?;
        if *offset + len as usize * tl_block_signature::SIZE_HINT > packet.len() {
            return Err(TlError::UnexpectedEof);
        }
        let mut pairs = Vec::with_capacity(len as usize);
        for _ in 0..len {
            pairs.push(tl_block_signature::read(packet, offset)?);
        }
        Ok(pairs)
    }
}

mod tl_block_signature {
    use everscale_types::prelude::HashBytes;

    use super::*;

    pub const SIZE_HINT: usize = 32 + 68;

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<BlockSignature> {
        let node_id_short = HashBytes(<[u8; 32]>::read_from(packet, offset)?);
        let signature = <&[u8]>::read_from(packet, offset)?
            .try_into()
            .map(Signature)
            .map_err(|_| TlError::InvalidData)?;

        Ok(BlockSignature {
            node_id_short,
            signature,
        })
    }
}

mod tl_block_id_vec {
    use super::*;

    pub fn size_hint(ids: &[BlockId]) -> usize {
        4 + ids.len() * tl_block_id::SIZE_HINT
    }

    pub fn write<P: TlPacket>(blocks: &[BlockId], packet: &mut P) {
        packet.write_u32(blocks.len() as u32);
        for block in blocks {
            tl_block_id::write(block, packet);
        }
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<Vec<BlockId>> {
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

    pub const fn size_hint(_: &BlockId) -> usize {
        SIZE_HINT
    }

    pub fn write<P: TlPacket>(block: &BlockId, packet: &mut P) {
        packet.write_i32(block.shard.workchain());
        packet.write_u64(block.shard.prefix());
        packet.write_u32(block.seqno);
        packet.write_raw_slice(block.root_hash.as_slice());
        packet.write_raw_slice(block.file_hash.as_slice());
    }

    pub fn read(packet: &[u8], offset: &mut usize) -> TlResult<BlockId> {
        let shard = ShardIdent::new(
            i32::read_from(packet, offset)?,
            u64::read_from(packet, offset)?,
        )
        .ok_or(TlError::InvalidData)?;
        let seqno = u32::read_from(packet, offset)?;
        let root_hash = <[u8; 32]>::read_from(packet, offset)?;
        let file_hash = <[u8; 32]>::read_from(packet, offset)?;

        Ok(BlockId {
            shard,
            seqno,
            root_hash: root_hash.into(),
            file_hash: file_hash.into(),
        })
    }
}
