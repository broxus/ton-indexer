use anyhow::Result;

use crate::storage::package_entry_id::package_index::{ArchiveId, PackageId};
use crate::storage::{
    columns, BlockHandle, GetFileName, PackageMetaEntry, PackageMut, PackageType, StoredValue, Tree,
};
use crate::utils::BlockStuff;

use super::package_index::PackageMetaStorage;

pub struct ArchiveStorage {
    db: Tree<columns::ArchiveStorage>,
    index: PackageMetaStorage,
    block_handles: Tree<columns::BlockHandles>,
}

impl ArchiveStorage {
    pub fn with_db(
        db: Tree<columns::ArchiveStorage>,
        index: PackageMetaStorage,
        block_handles: Tree<columns::BlockHandles>,
    ) -> Result<Self> {
        Ok(Self {
            db,
            index,
            block_handles,
        })
    }

    pub fn add_entry(&self, block: &BlockHandle, data: &BlockStuff) -> Result<()> {
        let id = block.masterchain_ref_seqno();
        let is_keyblock = block.meta().is_key_block();
        let mut batch = rocksdb::WriteBatch::default();
        let archive_cf = self.db.get_cf()?;
        let meta_cf = self.index.db.get_cf()?;
        let handle_cf = self.block_handles.get_cf()?;
        // Separate keyblock archive
        if is_keyblock {
            let package = self.index.calculate_package_id(id, true, is_keyblock);
            let archive_id = ArchiveId::Key(id);
            let initial_package_meta =
                self.index
                    .get_state(ArchiveId::Key(id))?
                    .unwrap_or_else(|| {
                        PackageMetaEntry::with_data(false, false, 0, PackageType::KeyBlocks)
                    });
            let data = self.prepare_archive_data(&package, block, data)?;
            batch.merge_cf(&archive_cf, data.id, data.archive.as_bytes());
            batch.merge_cf(
                &meta_cf,
                archive_id.as_bytes(),
                initial_package_meta.as_bytes()?,
            );

            let meta = block.meta();
            meta.set_is_archived(); // todo should check i  is archived before?
            batch.put_cf(&handle_cf, block.id().root_hash(), meta.to_vec()?);
        };
        let package = self.index.calculate_package_id(id, false, is_keyblock);

        let archive_id = ArchiveId::Key(id);
        let initial_package_meta = self
            .index
            .get_state(ArchiveId::Key(id))?
            .unwrap_or_else(|| {
                PackageMetaEntry::with_data(false, false, 0, PackageType::KeyBlocks)
            });
        let data = self.prepare_archive_data(&package, block, data)?;
        batch.merge_cf(&archive_cf, data.id, data.archive.as_bytes());
        batch.merge_cf(
            &meta_cf,
            archive_id.as_bytes(),
            initial_package_meta.as_bytes()?,
        );

        let meta = block.meta();
        meta.set_is_archived(); // todo should check is archived
        batch.put_cf(&handle_cf, block.id().root_hash(), meta.to_vec()?);
        self.db.raw_db_handle().write(batch)?;
        block.meta().set_is_archived();
        Ok(())
    }

    fn prepare_archive_data(
        &self,
        id: &PackageId,
        block: &BlockHandle,
        data: &BlockStuff,
    ) -> Result<ArchivePair> {
        let id = bincode::serialize(id)?;
        let arch = if let Some(a) = self.db.get(&id)? {
            let mut arch = PackageMut::from_bytes(a.as_ref());
            arch.add_package_segment(&block.id().filename(), data.data());
            arch
        } else {
            PackageMut::with_data(&block.id().filename(), data.data())
        };
        Ok(ArchivePair { id, archive: arch })
    }

    pub fn get_archive_slice(
        &self,
        id: ArchiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Option<Vec<u8>>> {
        let slice = self.db.get(id.as_bytes())?;
        match slice {
            None => Ok(None),
            Some(slice) => {
                anyhow::ensure!(offset >= slice.len(), "Offset > total slice len");
                let real_offset = std::cmp::min(limit + offset, slice.len());
                Ok(Some(slice.as_ref()[limit..real_offset].to_vec()))
            }
        }
    }
}

struct ArchivePair {
    id: Vec<u8>,
    archive: PackageMut,
}
