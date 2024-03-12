use std::sync::Arc;

use anyhow::{Context, Result};
use num_traits::ToPrimitive;
use ton_types::UInt256;

use crate::db::*;
use crate::utils::*;

use super::cell_storage::*;
use super::entries_buffer::*;
use super::files_context::*;
use super::shard_state_reader::*;

pub struct ShardStateReplaceTransaction<'a> {
    db: &'a Db,
    cell_storage: &'a Arc<CellStorage>,
    min_ref_mc_state: &'a Arc<MinRefMcState>,
    reader: ShardStatePacketReader,
    header: Option<BocHeader>,
    cells_read: u64,
}

impl<'a> ShardStateReplaceTransaction<'a> {
    pub fn new(
        db: &'a Db,
        cell_storage: &'a Arc<CellStorage>,
        min_ref_mc_state: &'a Arc<MinRefMcState>,
    ) -> Self {
        Self {
            db,
            cell_storage,
            min_ref_mc_state,
            reader: ShardStatePacketReader::new(),
            header: None,
            cells_read: 0,
        }
    }

    pub fn header(&self) -> &Option<BocHeader> {
        &self.header
    }

    pub async fn process_packet(
        &mut self,
        ctx: &mut FilesContext,
        packet: Vec<u8>,
        progress_bar: &mut ProgressBar,
    ) -> Result<bool> {
        use tokio::io::AsyncWriteExt;

        let cells_file = ctx.cells_file()?;

        self.reader.set_next_packet(packet);

        let header = loop {
            if let Some(header) = &self.header {
                break header;
            }

            let header = match self.reader.read_header()? {
                Some(header) => header,
                None => return Ok(false),
            };

            tracing::debug!(?header);
            progress_bar.set_total(header.cell_count);

            self.header = Some(header);
        };

        let mut chunk_size = 0u32;
        let mut buffer = [0; 256]; // At most 2 + 128 + 4 * 4

        while self.cells_read < header.cell_count {
            let cell_size = match self.reader.read_cell(header.ref_size, &mut buffer)? {
                Some(cell_size) => cell_size,
                None => break,
            };

            buffer[cell_size] = cell_size as u8;
            cells_file.write_all(&buffer[..cell_size + 1]).await?;

            chunk_size += cell_size as u32 + 1;
            self.cells_read += 1;
        }

        progress_bar.set_progress(self.cells_read);

        if chunk_size > 0 {
            tracing::debug!(chunk_size, "creating chunk");
            cells_file.write_u32_le(chunk_size).await?;
        }

        if self.cells_read < header.cell_count {
            return Ok(false);
        }

        if header.has_crc && self.reader.read_crc()?.is_none() {
            return Ok(false);
        }

        progress_bar.complete();
        Ok(true)
    }

    pub async fn finalize(
        self,
        ctx: &mut FilesContext,
        block_id: ton_block::BlockIdExt,
        progress_bar: &mut ProgressBar,
    ) -> Result<Arc<ShardStateStuff>> {
        // 2^7 bits + 1 bytes
        const MAX_DATA_SIZE: usize = 128;
        const CELLS_PER_BATCH: u64 = 1_000_000;

        let header = match &self.header {
            Some(header) => header,
            None => {
                return Err(ReplaceTransactionError::InvalidShardStatePacket)
                    .context("BOC header not found")
            }
        };

        let hashes_file =
            ctx.create_mapped_hashes_file(header.cell_count as usize * HashesEntry::LEN)?;
        let cells_file = ctx.create_mapped_cells_file().await?;

        let raw = self.db.raw().as_ref();
        let write_options = self.db.cells.new_write_config();

        let mut tail = [0; 4];
        let mut ctx = FinalizationContext::new(self.db);

        // Allocate on heap to prevent big future size
        let mut chunk_buffer = Vec::with_capacity(1 << 20);
        let mut data_buffer = vec![0u8; MAX_DATA_SIZE];

        let total_size = cells_file.length();
        progress_bar.set_total(total_size as u64);

        let mut file_pos = total_size;
        let mut cell_index = header.cell_count;
        let mut batch_len = 0;
        while file_pos >= 4 {
            file_pos -= 4;
            unsafe { cells_file.read_exact_at(file_pos, &mut tail) };

            let mut chunk_size = u32::from_le_bytes(tail) as usize;
            chunk_buffer.resize(chunk_size, 0);

            file_pos -= chunk_size;
            unsafe { cells_file.read_exact_at(file_pos, &mut chunk_buffer) };

            tracing::debug!(chunk_size, "processing chunk");

            while chunk_size > 0 {
                cell_index -= 1;
                batch_len += 1;
                let cell_size = chunk_buffer[chunk_size - 1] as usize;
                chunk_size -= cell_size + 1;

                let cell = RawCell::from_stored_data(
                    &mut &chunk_buffer[chunk_size..chunk_size + cell_size],
                    header.ref_size,
                    header.cell_count as usize,
                    cell_index as usize,
                    &mut data_buffer,
                )?;

                for (&index, buffer) in cell
                    .reference_indices
                    .iter()
                    .zip(ctx.entries_buffer.iter_child_buffers())
                {
                    // SAFETY: `buffer` is guaranteed to be in separate memory area
                    unsafe { hashes_file.read_exact_at(index as usize * HashesEntry::LEN, buffer) }
                }

                self.finalize_cell(&mut ctx, cell_index as u32, cell)?;

                // SAFETY: `entries_buffer` is guaranteed to be in separate memory area
                unsafe {
                    hashes_file.write_all_at(
                        cell_index as usize * HashesEntry::LEN,
                        ctx.entries_buffer.current_entry_buffer(),
                    )
                };

                chunk_buffer.truncate(chunk_size);
            }

            if batch_len > CELLS_PER_BATCH {
                ctx.finalize_cell_usages();
                raw.write_opt(std::mem::take(&mut ctx.write_batch), &write_options)?;
                batch_len = 0;
            }

            progress_bar.set_progress((total_size - file_pos) as u64);
            tokio::task::yield_now().await;
        }

        if batch_len > 0 {
            ctx.finalize_cell_usages();
            raw.write_opt(std::mem::take(&mut ctx.write_batch), &write_options)?;
        }

        // Current entry contains root cell
        let root_hash = ctx.entries_buffer.repr_hash();
        ctx.final_check(root_hash)?;

        let shard_state_key = (block_id.shard_id, block_id.seq_no).to_vec();
        self.db.shard_states.insert(&shard_state_key, root_hash)?;

        progress_bar.complete();

        // Load stored shard state
        match self.db.shard_states.get(shard_state_key)? {
            Some(root) => {
                let cell_id = UInt256::from_be_bytes(&root);

                let cell = self.cell_storage.load_cell(cell_id)?;
                Ok(Arc::new(ShardStateStuff::new(
                    block_id,
                    ton_types::Cell::with_cell_impl_arc(cell),
                    self.min_ref_mc_state,
                )?))
            }
            None => Err(ReplaceTransactionError::NotFound.into()),
        }
    }

    fn finalize_cell(
        &self,
        ctx: &mut FinalizationContext,
        cell_index: u32,
        cell: RawCell<'_>,
    ) -> Result<()> {
        use sha2::{Digest, Sha256};

        let (mut current_entry, children) =
            ctx.entries_buffer.split_children(&cell.reference_indices);

        current_entry.clear();

        // Prepare mask and counters
        let data_size = (cell.bit_len / 8) + usize::from(cell.bit_len % 8 != 0);

        let mut children_mask = ton_types::LevelMask::with_mask(0);
        let mut tree_bits_count = cell.bit_len as u64;
        let mut tree_cell_count = 1;

        for (_, child) in children.iter() {
            children_mask |= child.level_mask();
            tree_bits_count += child.tree_bits_count();
            tree_cell_count += child.tree_cell_count();
        }

        let mut is_merkle_cell = false;
        let mut is_pruned_cell = false;
        let level_mask = match cell.cell_type {
            ton_types::CellType::Ordinary => children_mask,
            ton_types::CellType::PrunedBranch => {
                is_pruned_cell = true;
                ton_types::LevelMask::with_mask(cell.level_mask)
            }
            ton_types::CellType::LibraryReference => ton_types::LevelMask::with_mask(0),
            ton_types::CellType::MerkleProof => {
                is_merkle_cell = true;
                ton_types::LevelMask::for_merkle_cell(children_mask)
            }
            ton_types::CellType::MerkleUpdate => {
                is_merkle_cell = true;
                ton_types::LevelMask::for_merkle_cell(children_mask)
            }
            ton_types::CellType::Unknown => {
                return Err(ReplaceTransactionError::InvalidCell).context("Unknown cell type")
            }
        };

        if cell.level_mask != level_mask.mask() {
            return Err(ReplaceTransactionError::InvalidCell).context("Level mask mismatch");
        }

        // Save mask and counters
        current_entry.set_level_mask(level_mask);
        current_entry.set_cell_type(cell.cell_type);
        current_entry.set_tree_bits_count(tree_bits_count);
        current_entry.set_tree_cell_count(tree_cell_count);

        // Calculate hashes
        let hash_count = if is_pruned_cell {
            1
        } else {
            level_mask.level() + 1
        };

        let mut max_depths = [0u16; 4];
        for i in 0..hash_count {
            let mut hasher = Sha256::new();

            let level_mask = if is_pruned_cell {
                level_mask
            } else {
                ton_types::LevelMask::with_level(i)
            };

            let d1 = ton_types::cell::calc_d1(
                level_mask,
                false,
                cell.cell_type,
                cell.reference_indices.len(),
            );
            let d2 = ton_types::cell::calc_d2(cell.bit_len);

            hasher.update([d1, d2]);

            if i == 0 {
                hasher.update(&cell.data[..data_size]);
            } else {
                hasher.update(current_entry.get_hash_slice(i - 1));
            }

            for (index, child) in children.iter() {
                let child_depth = if child.cell_type() == ton_types::CellType::PrunedBranch {
                    let child_data = ctx
                        .pruned_branches
                        .get(index)
                        .ok_or(ReplaceTransactionError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    child.pruned_branch_depth(i, child_data)
                } else {
                    child.depth(if is_merkle_cell { i + 1 } else { i })
                };
                hasher.update(child_depth.to_be_bytes());

                let depth = &mut max_depths[i as usize];
                *depth = std::cmp::max(*depth, child_depth + 1);

                if *depth > ton_types::MAX_DEPTH {
                    return Err(ReplaceTransactionError::InvalidCell)
                        .context("Max tree depth exceeded");
                }

                current_entry.set_depth(i, *depth);
            }

            for (index, child) in children.iter() {
                let child_hash = if child.cell_type() == ton_types::CellType::PrunedBranch {
                    let child_data = ctx
                        .pruned_branches
                        .get(index)
                        .ok_or(ReplaceTransactionError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    child
                        .pruned_branch_hash(i, child_data)
                        .context("Invalid pruned branch")?
                } else {
                    child.hash(if is_merkle_cell { i + 1 } else { i })
                };
                hasher.update(child_hash);
            }

            current_entry.set_hash(i, hasher.finalize().as_slice());
        }

        // Update pruned branches
        if is_pruned_cell {
            ctx.pruned_branches
                .insert(cell_index, cell.data[..data_size].to_vec());
        }

        // Write cell data
        let output_buffer = &mut ctx.output_buffer;
        output_buffer.clear();

        output_buffer.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0, cell.cell_type.to_u8().unwrap()]);
        output_buffer.extend_from_slice(&(cell.bit_len as u16).to_le_bytes());
        output_buffer.extend_from_slice(&cell.data[0..(cell.bit_len + 8) / 8]);
        output_buffer.extend_from_slice(&[cell.level_mask, 0, 1, hash_count]); // level_mask, store_hashes, has_hashes, hash_count
        for i in 0..hash_count {
            output_buffer.extend_from_slice(current_entry.get_hash_slice(i));
        }
        output_buffer.extend_from_slice(&[1, hash_count]); // has_depths, depth_count(same as hash_count)
        for i in 0..hash_count {
            output_buffer.extend_from_slice(current_entry.get_depth_slice(i));
        }

        // Write cell references
        output_buffer.extend_from_slice(&[cell.reference_indices.len() as u8]);
        for (index, child) in children.iter() {
            let child_hash = if child.cell_type() == ton_types::CellType::PrunedBranch {
                let child_data = ctx
                    .pruned_branches
                    .get(index)
                    .ok_or(ReplaceTransactionError::InvalidCell)
                    .context("Pruned branch data not found")?;
                child
                    .pruned_branch_hash(MAX_LEVEL, child_data)
                    .context("Invalid pruned branch")?
            } else {
                child.hash(MAX_LEVEL)
            };

            *ctx.cell_usages.entry(*child_hash).or_default() += 1;
            output_buffer.extend_from_slice(child_hash);
        }

        // Write counters
        output_buffer.extend_from_slice(current_entry.get_tree_counters());

        // Save serialized data
        let repr_hash = if is_pruned_cell {
            current_entry
                .as_reader()
                .pruned_branch_hash(3, &cell.data[..data_size])
                .context("Invalid pruned branch")?
        } else {
            current_entry.as_reader().hash(MAX_LEVEL)
        };

        ctx.write_batch
            .merge_cf(&ctx.cells_cf, repr_hash, output_buffer.as_slice());
        ctx.cell_usages.insert(*repr_hash, -1);

        // Done
        Ok(())
    }
}

struct FinalizationContext<'a> {
    pruned_branches: FastHashMap<u32, Vec<u8>>,
    cell_usages: FastHashMap<[u8; 32], i32>,
    entries_buffer: EntriesBuffer,
    output_buffer: Vec<u8>,
    cells_cf: BoundedCfHandle<'a>,
    write_batch: rocksdb::WriteBatch,
}

impl<'a> FinalizationContext<'a> {
    fn new(db: &'a Db) -> Self {
        Self {
            pruned_branches: Default::default(),
            cell_usages: FastHashMap::with_capacity_and_hasher(128, Default::default()),
            entries_buffer: EntriesBuffer::new(),
            output_buffer: Vec::with_capacity(1 << 10),
            cells_cf: db.cells.cf(),
            write_batch: rocksdb::WriteBatch::default(),
        }
    }

    fn finalize_cell_usages(&mut self) {
        self.cell_usages.retain(|key, &mut rc| {
            if rc > 0 {
                self.write_batch.merge_cf(
                    &self.cells_cf,
                    key,
                    refcount::encode_positive_refcount(rc as u32),
                );
            }

            rc < 0
        });
    }

    fn final_check(&self, root_hash: &[u8; 32]) -> Result<()> {
        anyhow::ensure!(
            self.cell_usages.len() == 1 && self.cell_usages.contains_key(root_hash),
            "Invalid shard state cell"
        );
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
enum ReplaceTransactionError {
    #[error("Not found")]
    NotFound,
    #[error("Invalid shard state packet")]
    InvalidShardStatePacket,
    #[error("Invalid cell")]
    InvalidCell,
}

const MAX_LEVEL: u8 = 3;

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::{self, BufReader, Read};
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use ahash::HashSet;
    use anyhow::{Context, Result};
    use bumpalo::Bump;
    use ton_block::{BlockIdExt, ShardIdent};
    use ton_types::UInt256;
    use weedb::rocksdb;
    use weedb::rocksdb::WriteBatch;

    use crate::db::rocksdb::IteratorMode;
    use crate::storage::shard_state_storage::cell_storage::StorageCell;
    use crate::storage::shard_state_storage::files_context::FilesContext;
    use crate::storage::shard_state_storage::replace_transaction::ShardStateReplaceTransaction;
    use crate::utils::{ProgressBar, StoredValue};

    fn deserialize(path: &Path) -> io::Result<Vec<(u32, Vec<u8>)>> {
        let reader = File::open(path)?;
        let mut reader = BufReader::new(reader);

        let mut data = Vec::new();

        loop {
            let mut seq_no_bytes = [0u8; 4];
            let seq_no = match reader.read_exact(&mut seq_no_bytes) {
                Ok(_) => u32::from_le_bytes(seq_no_bytes),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // End of file reached.
                Err(e) => return Err(e),
            };
            let mut len_bytes = [0u8; 8];

            // Read the length of the next Vec<u8>.
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u64::from_le_bytes(len_bytes) as usize;
                    let mut buf = vec![0u8; len];
                    // Read the actual bytes.
                    reader.read_exact(&mut buf)?;
                    data.push((seq_no, buf));
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // End of file reached.
                Err(e) => return Err(e),
            }
        }

        Ok(data)
    }

    #[tokio::test]
    async fn test_store_plus_gc_empty_db() -> Result<()> {
        //setup
        let dir = tempfile::tempdir()?;
        let db = super::Db::open(dir.path().to_path_buf(), Default::default())?;
        let cell_storage = super::CellStorage::new(db.clone(), 1 << 20);
        let min_ref_mc_state = super::MinRefMcState::new();
        let transaction = ShardStateReplaceTransaction::new(&db, &cell_storage, &min_ref_mc_state);
        let mut progress_bar = ProgressBar::builder("test").build();

        let id = BlockIdExt {
            shard_id: ton_block::ShardIdent::masterchain(),
            ..Default::default()
        };
        let mut files_ctx = FilesContext::new(dir.path(), &id).await?;

        let boc = include_bytes!("../../../tests/everscale_zerostate.boc");
        let state_hash = insert_root(
            boc.to_vec(),
            transaction,
            &mut progress_bar,
            &mut files_ctx,
            id,
        )
        .await?;

        // Assertions and cleanup
        let len = db
            .shard_states
            .iterator(rocksdb::IteratorMode::Start)
            .count();
        assert_eq!(len, 1);

        let cells_len = db.cells.iterator(rocksdb::IteratorMode::Start).count();
        println!("cells_len: {}", cells_len);

        let mut batch = WriteBatch::default();
        let bump = Bump::new();
        let batch_size = cell_storage.remove_cell(&mut batch, &bump, state_hash)?;
        println!("batch_size: {}", batch_size);
        println!("Real batch size: {}", batch.len());

        batch.delete_cf(&db.shard_states.cf(), state_hash);
        db.raw().write_opt(batch, db.cells.write_config())?;

        for cell in db.cells.iterator(rocksdb::IteratorMode::Start) {
            println!("Cell: {:?}", cell.unwrap().1);
        }

        println!("Temp dir: {:?}", dir);
        db.trigger_compaction().await;

        for table in db.get_disk_usage()? {
            println!("Table: {:?}", table);
        }

        assert_eq!(cells_len, 0);

        let len = db
            .shard_states
            .iterator(rocksdb::IteratorMode::Start)
            .count();
        assert_eq!(len, 0);

        Ok(())
    }

    async fn insert_root(
        boc: Vec<u8>,
        mut transaction: ShardStateReplaceTransaction<'_>,
        progress_bar: &mut ProgressBar,
        files_ctx: &mut FilesContext,
        id: BlockIdExt,
    ) -> Result<UInt256> {
        transaction
            .process_packet(files_ctx, boc, progress_bar)
            .await?;

        let state = transaction.finalize(files_ctx, id, progress_bar).await?;
        let state_hash = state.root_cell().repr_hash();
        Ok(state_hash)
    }

    #[tokio::test]
    async fn test_first_n_states() -> anyhow::Result<()> {
        use ahash::HashSetExt;

        let test_data = include_bytes!("../../../tests/test_data.tar.xz");
        let dir = tempfile::tempdir()?;
        let tar_path = dir.path().join("test_data.tar.xz");
        std::fs::write(&tar_path, test_data)?;
        println!("Extracting test data");
        std::process::Command::new("tar")
            .arg("-xf")
            .arg(&tar_path)
            .arg("-C")
            .arg(dir.path())
            .status()?;
        println!("Test data extracted");

        let test_data = dir.path().join("test_data");

        let db = super::Db::open(dir.path().to_path_buf(), Default::default())?;
        let cell_storage = super::CellStorage::new(db.clone(), 1 << 20);
        let mut cell_hashes = Vec::new();

        let files_list = std::fs::read_dir(test_data)?;

        let mut total = 0;
        for file in files_list {
            let file = file?;
            let states = deserialize(&file.path())?;
            let file_name = file
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            println!("Processing file: {}", file_name);
            let (wc, shard) = file_name.split_once(':').unwrap();
            let wc = wc.parse::<i32>().unwrap();
            let shard = u64::from_str_radix(shard, 16).unwrap();
            let shard_id = ShardIdent::with_tagged_prefix(wc, shard).unwrap();

            for (seqno, boc) in states.into_iter() {
                let cell = ton_types::deserialize_tree_of_cells(&mut &boc[..])?;
                let root_hash = cell.repr_hash();

                let shard_state_key = (shard_id, seqno).to_vec();
                db.shard_states.insert(&shard_state_key, root_hash)?;
                cell_hashes.push(root_hash);

                let mut batch = WriteBatch::default();
                cell_storage.store_cell(&mut batch, cell)?;
                db.raw().write_opt(batch, db.cells.write_config())?;
                if total % 2000 == 0 {
                    println!("Stored {total} states");
                }
                total += 1;
            }
        }

        println!("Stored states");
        db.cells
            .iterator(IteratorMode::Start)
            .filter_map(Result::ok)
            .count();

        let len = cell_hashes.len();
        let mut total = 0;
        let mut deleted = HashSet::new();
        let num_cpus = std::thread::available_parallelism()?.get();

        let last_states = cell_hashes.split_off(len - num_cpus);
        let mut handles = Vec::new();
        let should_exit = Arc::new(AtomicBool::new(false));
        for root in last_states.iter().copied() {
            for _ in 0..3 {
                let cell_storage = cell_storage.clone();
                let exit_now = should_exit.clone();
                let cell = cell_storage.load_cell(root)?;
                let handle = std::thread::spawn(move || {
                    traverse_state(&cell, &exit_now);
                });
                handles.push(handle);
            }
        }

        for (num, root) in cell_hashes.into_iter().enumerate() {
            if !deleted.insert(root) {
                continue;
            }

            let cell = cell_storage
                .load_cell(root)
                .with_context(|| format!("Cell not found for num {num} of {len}",))?; // check that cell was stored

            let mut batch = WriteBatch::default();
            cell_storage
                .remove_cell(&mut batch, &Bump::new(), cell.repr_hash())
                .with_context(|| format!("Error removing cell for num {num} of {len}",))?; // check that cell was removed;
            db.raw().write_opt(batch, db.cells.write_config())?;
            total += 1;
            if total % 2000 == 0 {
                println!("Deleted {total} states");
            }
        }
        should_exit.store(true, std::sync::atomic::Ordering::Relaxed);
        for handle in handles {
            handle.join().unwrap();
        }
        for root in last_states {
            let cell = cell_storage.load_cell(root)?;
            let mut batch = WriteBatch::default();
            cell_storage
                .remove_cell(&mut batch, &Bump::new(), cell.repr_hash())
                .context("Error removing cell")?;
            db.raw().write_opt(batch, db.cells.write_config())?;
        }

        db.trigger_compaction().await;

        let num_not_deleted = db
            .cells
            .iterator(IteratorMode::Start)
            .filter_map(Result::ok)
            .filter(|x| !x.1.is_empty())
            .count();
        println!("Not deleted: {num_not_deleted}. Total: {total}");
        assert_eq!(num_not_deleted, 0);
        println!("{:?}", cell_storage.raw_cells_cache.hit_ratio());

        Ok(())
    }

    fn traverse_state(cell: &Arc<StorageCell>, exit_now: &AtomicBool) {
        use ton_types::CellImpl;
        let mut total_visited = 0usize;
        loop {
            let mut stack = vec![cell.clone()];
            while let Some(cell) = stack.pop() {
                total_visited += 1;
                if exit_now.load(std::sync::atomic::Ordering::Relaxed) {
                    println!("Total visited: {total_visited}");
                    return;
                }
                for i in 0..cell.references_count() {
                    let child = cell.reference(i).unwrap();
                    stack.push(child);
                }
            }
        }
    }
}
