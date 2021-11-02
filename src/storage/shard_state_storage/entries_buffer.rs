pub struct EntriesBuffer(Box<[[u8; HashesEntry::LEN]; 5]>);

impl EntriesBuffer {
    pub fn new() -> Self {
        Self(Box::new([[0; HashesEntry::LEN]; 5]))
    }

    pub fn current_entry_buffer(&mut self) -> &mut [u8; HashesEntry::LEN] {
        &mut self.0[0]
    }

    pub fn iter_child_buffers(&mut self) -> impl Iterator<Item = &mut [u8; HashesEntry::LEN]> {
        self.0.iter_mut().skip(1)
    }

    pub fn split_children<'a, 'b>(
        &'a mut self,
        references: &'b [u32],
    ) -> (HashesEntryWriter<'a>, EntriesBufferChildren<'b>)
    where
        'a: 'b,
    {
        let [first, tail @ ..] = &mut *self.0;
        (
            HashesEntryWriter(first),
            EntriesBufferChildren(references, tail),
        )
    }
}

pub struct EntriesBufferChildren<'a>(&'a [u32], &'a [[u8; HashesEntry::LEN]]);

impl EntriesBufferChildren<'_> {
    pub fn iter(&self) -> impl Iterator<Item = (&u32, HashesEntry)> {
        self.0
            .iter()
            .zip(self.1)
            .map(|(index, item)| (index, HashesEntry(item)))
    }
}

pub struct HashesEntryWriter<'a>(&'a mut [u8]);

impl HashesEntryWriter<'_> {
    pub fn as_reader(&self) -> HashesEntry {
        HashesEntry(self.0)
    }

    pub fn clear(&mut self) {
        for byte in &mut *self.0 {
            *byte = 0;
        }
    }

    pub fn set_level_mask(&mut self, level_mask: ton_types::LevelMask) {
        self.0[0] = level_mask.mask();
    }

    pub fn set_cell_type(&mut self, cell_type: ton_types::CellType) {
        self.0[1] = cell_type.into();
    }

    pub fn set_tree_bits_count(&mut self, count: u64) {
        self.get_tree_bits_count_slice()
            .copy_from_slice(&count.to_le_bytes());
    }

    pub fn get_tree_bits_count_slice(&mut self) -> &mut [u8] {
        &mut self.0[4..12]
    }

    pub fn set_tree_cell_count(&mut self, count: u64) {
        self.get_tree_cell_count_slice()
            .copy_from_slice(&count.to_le_bytes());
    }

    pub fn get_tree_cell_count_slice(&mut self) -> &mut [u8] {
        &mut self.0[12..20]
    }

    pub fn set_hash(&mut self, i: u8, hash: &[u8]) {
        self.get_hash_slice(i).copy_from_slice(hash);
    }

    pub fn get_hash_slice(&mut self, i: u8) -> &mut [u8] {
        let offset = HashesEntry::HASHES_OFFSET + 32 * i as usize;
        &mut self.0[offset..offset + 32]
    }

    pub fn set_depth(&mut self, i: u8, depth: u16) {
        self.get_depth_slice(i)
            .copy_from_slice(&depth.to_le_bytes());
    }

    pub fn get_depth_slice(&mut self, i: u8) -> &mut [u8] {
        let offset = HashesEntry::DEPTHS_OFFSET + 2 * i as usize;
        &mut self.0[offset..offset + 2]
    }
}

pub struct HashesEntry<'a>(&'a [u8]);

impl<'a> HashesEntry<'a> {
    // 4 bytes - info (1 byte level mask, 1 byte cell type, 2 bytes padding)
    // 8 bytes - tree bits count
    // 4 bytes - cell count
    // 32 * 4 bytes - hashes
    // 2 * 4 bytes - depths
    pub const LEN: usize = 4 + 8 + 8 + 32 * 4 + 2 * 4;
    pub const HASHES_OFFSET: usize = 4 + 8 + 8;
    pub const DEPTHS_OFFSET: usize = 4 + 8 + 8 + 32 * 4;

    pub fn level_mask(&self) -> ton_types::LevelMask {
        ton_types::LevelMask::with_mask(self.0[0])
    }

    pub fn cell_type(&self) -> ton_types::CellType {
        ton_types::CellType::from(self.0[1])
    }

    pub fn tree_bits_count(&self) -> u64 {
        u64::from_le_bytes(self.0[4..12].try_into().unwrap())
    }

    pub fn tree_cell_count(&self) -> u64 {
        u64::from_le_bytes(self.0[12..20].try_into().unwrap())
    }

    pub fn hash(&self, n: u8) -> &[u8] {
        let offset = Self::HASHES_OFFSET + 32 * self.level_mask().calc_hash_index(n as usize);
        &self.0[offset..offset + 32]
    }

    pub fn depth(&self, n: u8) -> u16 {
        let offset = Self::DEPTHS_OFFSET + 2 * self.level_mask().calc_hash_index(n as usize);
        u16::from_le_bytes([self.0[offset], self.0[offset + 1]])
    }

    pub fn pruned_branch_hash<'b>(&self, n: u8, data: &'b [u8]) -> &'b [u8]
    where
        'a: 'b,
    {
        let level_mask = self.level_mask();
        let index = level_mask.calc_hash_index(n as usize);
        let level = level_mask.level() as usize;

        if index == level {
            let offset = Self::HASHES_OFFSET;
            &self.0[offset..offset + 32]
        } else {
            let offset = 1 + 1 + index * 32;
            &data[offset..offset + 32]
        }
    }

    pub fn pruned_branch_depth(&self, n: u8, data: &[u8]) -> u16 {
        let level_mask = self.level_mask();
        let index = level_mask.calc_hash_index(n as usize);
        let level = level_mask.level() as usize;

        if index == level {
            let offset = Self::DEPTHS_OFFSET;
            u16::from_le_bytes([self.0[offset], self.0[offset + 1]])
        } else {
            let offset = 1 + 1 + level * 32 + index * 2;
            u16::from_be_bytes([data[offset], data[offset + 1]])
        }
    }
}
