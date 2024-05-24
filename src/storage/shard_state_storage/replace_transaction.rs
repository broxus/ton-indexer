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

        ctx.clear_temp_cells(self.db)?;

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

        self.cell_storage
            .apply_temp_cell(&ton_types::UInt256::from(root_hash))?;
        ctx.clear_temp_cells(self.db)?;

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
        let mut i = 0;
        for level in 0..4 {
            if level != 0 && (is_pruned_cell || ((1 << (level - 1)) & level_mask.mask()) == 0) {
                continue;
            }

            let mut hasher = Sha256::new();

            let level_mask = if is_pruned_cell {
                level_mask
            } else {
                ton_types::LevelMask::with_level(level)
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
                    child.pruned_branch_depth(if is_merkle_cell { i + 1 } else { i }, child_data)
                } else {
                    child.depth(if is_merkle_cell { i + 1 } else { i })
                };
                hasher.update(child_depth.to_be_bytes());

                let depth = &mut max_depths[i as usize];
                *depth = child_depth
                    .checked_add(1)
                    .map(|next_depth| next_depth.max(*depth))
                    .filter(|&depth| depth <= ton_types::MAX_DEPTH)
                    .ok_or(ReplaceTransactionError::InvalidCell)
                    .context("Max tree depth exceeded")?;

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
                        .pruned_branch_hash(if is_merkle_cell { i + 1 } else { i }, child_data)
                        .context("Invalid pruned branch")?
                } else {
                    child.hash(if is_merkle_cell { i + 1 } else { i })
                };
                hasher.update(child_hash);
            }

            current_entry.set_hash(i, hasher.finalize().as_slice());

            i += 1;
        }

        // Update pruned branches
        if is_pruned_cell {
            ctx.pruned_branches
                .insert(cell_index, cell.data[..data_size].to_vec());
        }

        // Write cell data
        let output_buffer = &mut ctx.output_buffer;
        output_buffer.clear();

        output_buffer.extend_from_slice(&[cell.cell_type.to_u8().unwrap()]);
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
            .put_cf(&ctx.temp_cells_cf, repr_hash, output_buffer.as_slice());
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
    temp_cells_cf: BoundedCfHandle<'a>,
    write_batch: rocksdb::WriteBatch,
}

impl<'a> FinalizationContext<'a> {
    fn new(db: &'a Db) -> Self {
        Self {
            pruned_branches: Default::default(),
            cell_usages: FastHashMap::with_capacity_and_hasher(128, Default::default()),
            entries_buffer: EntriesBuffer::new(),
            output_buffer: Vec::with_capacity(1 << 10),
            temp_cells_cf: db.temp_cells.cf(),
            write_batch: rocksdb::WriteBatch::default(),
        }
    }

    fn clear_temp_cells(&self, db: &Db) -> Result<(), rocksdb::Error> {
        let from = &[0x00; 32];
        let to = &[0xff; 32];
        db.raw().delete_range_cf(&self.temp_cells_cf, from, to)
    }

    fn finalize_cell_usages(&mut self) {
        self.cell_usages.retain(|_, &mut rc| rc < 0);
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
    use std::io;
    use std::io::{BufReader, Read};
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::db::Db;
    use crate::DbOptions;
    use weedb::rocksdb::{IteratorMode, WriteBatch};

    use crate::storage::shard_state_storage::cell_storage::{CellStorage, StorageCell};
    use crate::storage::shard_state_storage::files_context::FilesContext;
    use crate::storage::shard_state_storage::replace_transaction::ShardStateReplaceTransaction;
    use crate::utils::{MinRefMcState, ProgressBar};
    use anyhow::{Context, Result};
    use ton_block::{BlockIdExt, ShardIdent};
    use ton_types::{CellImpl, UInt256};
    use tracing_subscriber::EnvFilter;
    use weedb::rocksdb;

    #[tokio::test]
    #[ignore]
    async fn insert_and_delete_of_several_shards() -> Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::new("info"))
            .init();
        let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".scratch");
        let integration_test_path = project_root.join("integration_tests");
        let current_test_path = integration_test_path.join("insert_and_delete_of_several_shards");
        std::fs::remove_dir_all(&current_test_path).ok();
        std::fs::create_dir_all(&current_test_path)?;

        // decompressing the archive
        let archive_path = integration_test_path.join("states.tar.zst");
        download_and_verify(
            "https://tycho-test.broxus.cc",
            &integration_test_path.to_string_lossy(),
        )?;
        let res = std::process::Command::new("tar")
            .arg("-I")
            .arg("zstd")
            .arg("-xf")
            .arg(&archive_path)
            .arg("-C")
            .arg(&current_test_path)
            .status()?;
        if !res.success() {
            return Err(anyhow::anyhow!("Failed to decompress the archive"));
        }
        tracing::info!("Decompressed the archive");

        let db = Db::open(current_test_path.join("rocksdb"), DbOptions::default())?;

        let cells_storage = CellStorage::new(db.clone(), 100_000_000);
        let tracker = MinRefMcState::new();

        for file in std::fs::read_dir(current_test_path.join("states"))? {
            let file = file?;
            let filename = file.file_name().to_string_lossy().to_string();

            let block_id = parse_filename(filename.as_ref());

            let mut store_state = ShardStateReplaceTransaction::new(&db, &cells_storage, &tracker);

            let file = File::open(file.path())?;
            let mut file = BufReader::new(file);
            let chunk_size = 10_000_000; // size of each chunk in bytes
            let mut buffer = vec![0u8; chunk_size];
            let mut pg = ProgressBar::builder("downloading state")
                .exact_unit("cells")
                .build();

            let downloads_dir = current_test_path.join("downloads");
            std::fs::create_dir_all(&downloads_dir)?;
            let mut files_context = FilesContext::new(downloads_dir, &block_id)
                .await
                .context("failed to open files context")?;

            loop {
                let bytes_read = file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break; // End of file
                }

                let packet = buffer[..bytes_read].to_vec();
                store_state
                    .process_packet(&mut files_context, packet, &mut pg)
                    .await?;
            }

            let mut pg = ProgressBar::builder("processing state")
                .with_mapper(|x| bytesize::to_string(x, false))
                .build();
            store_state
                .finalize(&mut files_context, block_id, &mut pg)
                .await?;
        }
        tracing::info!("Finished processing all states");
        tracing::info!("Starting gc");
        states_gc(&cells_storage, &db).await?;

        drop(db);
        drop(cells_storage);
        rocksdb::DB::destroy(
            &rocksdb::Options::default(),
            current_test_path.join("rocksdb"),
        )?;

        Ok(())
    }

    async fn states_gc(cell_storage: &Arc<CellStorage>, db: &Db) -> Result<()> {
        let states_iterator = db.shard_states.iterator(IteratorMode::Start);
        let bump = bumpalo::Bump::new();

        let total_states = db.shard_states.iterator(IteratorMode::Start).count();

        for (deleted, state) in states_iterator.enumerate() {
            let (_, value) = state?;

            // check that state actually exists
            let cell = cell_storage.load_cell(UInt256::from_slice(&value))?;
            traverse_cell(cell.clone());

            let mut batch = WriteBatch::default();
            cell_storage.remove_cell(&mut batch, &bump, cell.repr_hash())?;

            // execute batch
            db.raw().write_opt(batch, db.cells.write_config())?;
            tracing::info!("State deleted. Progress: {}/{total_states}", deleted + 1);
            assert!(cell_storage.load_cell(UInt256::from_slice(&value)).is_err());
        }

        // two compactions in row. First one run merge operators, second one will remove all tombstones
        db.trigger_compaction().await;
        db.trigger_compaction().await;

        let cells_left = db.cells.iterator(IteratorMode::Start).count();
        tracing::info!("States GC finished. Cells left: {cells_left}");
        assert_eq!(cells_left, 0, "Gc is broken. Press F to pay respect");

        Ok(())
    }

    fn parse_filename(name: &str) -> BlockIdExt {
        // Split the remaining string by commas into components
        let parts: Vec<&str> = name.split(',').collect();

        // Parse each part
        let workchain: i32 = parts[0].parse().unwrap();
        let prefix = u64::from_str_radix(parts[1], 16).unwrap();
        let seqno: u32 = parts[2].parse().unwrap();

        BlockIdExt {
            shard_id: ShardIdent::with_tagged_prefix(workchain, prefix).unwrap(),
            seq_no: seqno,
            root_hash: UInt256::default(),
            file_hash: UInt256::default(),
        }
    }

    fn traverse_cell(cell: Arc<StorageCell>) {
        let mut stack = vec![cell];
        while let Some(cell) = stack.pop() {
            for i in 0..cell.references_count() {
                stack.push(cell.reference(i).unwrap());
            }
        }
    }

    fn download_and_verify(base_url: &str, dir: &str) -> io::Result<()> {
        use std::io::{self};
        use std::path::PathBuf;
        use std::process::Command;

        fn download_file(url: &str, output: &str) -> io::Result<()> {
            let status = Command::new("curl")
                .arg("--request")
                .arg("GET")
                .arg("-L")
                .arg("--url")
                .arg(url)
                .arg("--output")
                .arg(output)
                .status()?;

            if !status.success() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to download file",
                ));
            }
            Ok(())
        }

        fn verify_checksum(checksum_file: &str, archive_dir: &str) -> io::Result<bool> {
            let output = Command::new("sha256sum")
                .arg("-c")
                .arg(checksum_file)
                .current_dir(archive_dir)
                .output()?;

            Ok(output.status.success())
        }

        // Download the checksum file
        println!("Downloading checksum file...");
        let checksum_url = format!("{}/states.tar.zst.sha256", base_url);
        let checksum_output = format!("{}/states.tar.zst.sha256", dir);
        download_file(&checksum_url, &checksum_output)?;

        // Check if the archive file exists
        let archive_path = format!("{}/states.tar.zst", dir);
        let archive_dir = PathBuf::from(&archive_path)
            .parent()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        if PathBuf::from(&archive_path).exists() {
            // Verify the archive against the checksum
            println!("Verifying existing archive against checksum...");
            if verify_checksum(&checksum_output, &archive_dir)? {
                println!("Checksum matches. No need to download the archive.");
                return Ok(());
            } else {
                println!("Checksum does not match. Downloading the archive...");
            }
        } else {
            println!("Archive file does not exist. Downloading the archive...");
        }

        // Download the archive file
        let archive_url = format!("{}/states.tar.zst", base_url);
        download_file(&archive_url, &archive_path)?;

        Ok(())
    }
}
