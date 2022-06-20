use anyhow::Result;
use smallvec::SmallVec;
use ton_types::ByteOrderRead;

/// A trait for writing or reading data from a stack-allocated buffer
pub trait StoredValue {
    /// On-stack buffer size hint
    const SIZE_HINT: usize;

    /// On-stack buffer type (see [`smallvec::SmallVec`])
    type OnStackSlice: smallvec::Array<Item = u8>;

    /// Serializes the data to the buffer
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T);

    /// Deserializes the data from the buffer.
    ///
    /// In case of successful deserialization it is guaranteed that `reader` will be
    /// moved to the end of the deserialized data.
    ///
    /// NOTE: `reader` should not be used after this call in case of an error
    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized;

    /// Deserializes the data from the buffer.
    ///
    /// [`StoredValue::deserialize`]
    #[inline(always)]
    fn from_slice(mut data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        Self::deserialize(&mut data)
    }

    /// Constructs on-stack buffer with the serialized object
    fn to_vec(&self) -> SmallVec<Self::OnStackSlice> {
        let mut result = SmallVec::with_capacity(Self::SIZE_HINT);
        self.serialize(&mut result);
        result
    }
}

/// A trait for simple buffer-based serialization
pub trait StoredValueBuffer {
    fn write_byte(&mut self, byte: u8);
    fn write_raw_slice(&mut self, data: &[u8]);
}

impl StoredValueBuffer for Vec<u8> {
    #[inline(always)]
    fn write_byte(&mut self, byte: u8) {
        self.push(byte);
    }

    #[inline(always)]
    fn write_raw_slice(&mut self, data: &[u8]) {
        self.extend_from_slice(data);
    }
}

impl<T> StoredValueBuffer for SmallVec<T>
where
    T: smallvec::Array<Item = u8>,
{
    #[inline(always)]
    fn write_byte(&mut self, byte: u8) {
        self.push(byte);
    }

    #[inline(always)]
    fn write_raw_slice(&mut self, data: &[u8]) {
        self.extend_from_slice(data);
    }
}

impl StoredValue for ton_block::BlockIdExt {
    /// 4 bytes workchain id,
    /// 8 bytes shard id,
    /// 4 bytes seqno,
    /// 32 bytes root hash,
    /// 32 bytes file hash
    const SIZE_HINT: usize = ton_block::ShardIdent::SIZE_HINT + 4 + 32 + 32;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.shard_id.serialize(buffer);
        buffer.write_raw_slice(&self.seq_no.to_be_bytes());
        buffer.write_raw_slice(self.root_hash.as_slice());
        buffer.write_raw_slice(self.file_hash.as_slice());
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let shard_id = ton_block::ShardIdent::deserialize(reader)?;
        let seq_no = reader.read_be_u32()?;
        let root_hash = ton_types::UInt256::from(reader.read_u256()?);
        let file_hash = ton_types::UInt256::from(reader.read_u256()?);
        Ok(Self::with_params(shard_id, seq_no, root_hash, file_hash))
    }
}

impl StoredValue for ton_block::ShardIdent {
    /// 4 bytes workchain id
    /// 8 bytes shard id
    const SIZE_HINT: usize = 4 + 8;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    #[inline(always)]
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(&self.workchain_id().to_be_bytes());
        buffer.write_raw_slice(&self.shard_prefix_with_tag().to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let workchain_id = reader.read_be_u32()? as i32;
        let shard_prefix_tagged = reader.read_be_u64()?;
        Ok(unsafe { Self::with_tagged_prefix_unchecked(workchain_id, shard_prefix_tagged) })
    }
}

impl StoredValue for (ton_block::ShardIdent, u32) {
    /// 12 bytes shard ident
    /// 4 bytes seqno
    const SIZE_HINT: usize = ton_block::ShardIdent::SIZE_HINT + 4;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    #[inline(always)]
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.0.serialize(buffer);
        buffer.write_raw_slice(&self.1.to_be_bytes());
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let shard_id = ton_block::ShardIdent::deserialize(reader)?;
        let seq_no = reader.read_be_u32()?;
        Ok((shard_id, seq_no))
    }
}

/// Writes BlockIdExt in little-endian format
pub fn write_block_id_le(block_id: &ton_block::BlockIdExt) -> [u8; 80] {
    let mut bytes = [0u8; 80];
    bytes[..4].copy_from_slice(&block_id.shard_id.workchain_id().to_le_bytes());
    bytes[4..12].copy_from_slice(&block_id.shard_id.shard_prefix_with_tag().to_le_bytes());
    bytes[12..16].copy_from_slice(&block_id.seq_no.to_le_bytes());
    bytes[16..48].copy_from_slice(block_id.root_hash.as_slice());
    bytes[48..80].copy_from_slice(block_id.file_hash.as_slice());
    bytes
}

/// Reads BlockIdExt in little-endian format
pub fn read_block_id_le(data: &[u8]) -> Option<ton_block::BlockIdExt> {
    if data.len() < 80 {
        return None;
    }

    let mut workchain_id = [0; 4];
    workchain_id.copy_from_slice(&data[0..4]);
    let workchain_id = i32::from_le_bytes(workchain_id);

    let mut shard_id = [0; 8];
    shard_id.copy_from_slice(&data[4..12]);
    let shard_id = u64::from_le_bytes(shard_id);

    let mut seq_no = [0; 4];
    seq_no.copy_from_slice(&data[12..16]);
    let seq_no = u32::from_le_bytes(seq_no);

    let mut root_hash = [0; 32];
    root_hash.copy_from_slice(&data[16..48]);

    let mut file_hash = [0; 32];
    file_hash.copy_from_slice(&data[48..80]);

    let shard_id =
        unsafe { ton_block::ShardIdent::with_tagged_prefix_unchecked(workchain_id, shard_id) };

    Some(ton_block::BlockIdExt {
        shard_id,
        seq_no,
        root_hash: root_hash.into(),
        file_hash: file_hash.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fully_on_stack() {
        assert!(!ton_block::BlockIdExt::default().to_vec().spilled());
        assert!(!ton_block::ShardIdent::default().to_vec().spilled());
    }

    #[test]
    fn correct_block_id_le_serialization() {
        const SERIALIZED: [u8; 80] = [
            255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 128, 123, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        ];

        let block_id = ton_block::BlockIdExt {
            shard_id: ton_block::ShardIdent::masterchain(),
            seq_no: 123,
            root_hash: [1u8; 32].into(),
            file_hash: [2u8; 32].into(),
        };

        let serialized = write_block_id_le(&block_id);
        assert_eq!(serialized, SERIALIZED);

        assert_eq!(read_block_id_le(&serialized).unwrap(), block_id);
    }
}
