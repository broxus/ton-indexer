use std::sync::Arc;

use anyhow::{Context, Result};
use ton_block::{AccountIdPrefixFull, BlockIdExt};

use crate::engine::complex_operations::{parse_archive, save_block};
use crate::storage::BlockHandle;
use crate::utils::ActivePeers;
use crate::Engine;

pub async fn sync(engine: Arc<Engine>, boot_data: BlockIdExt) -> Result<()> {
    log::info!("Starting background sync from {}", boot_data.seq_no);
    let store = engine.get_db().background_sync_store();

    // checking if we have already started sync process
    let (mut low, mut high, account_id) = {
        let handle = engine
            .load_block_handle(&boot_data)?
            .context("No handle for loaded block")?;
        let data = engine.load_block_data(&handle).await?.block().read_info()?;
        let prefix = data.shard().shard_prefix_with_tag();
        let account_id = AccountIdPrefixFull {
            workchain_id: -1,
            prefix,
        };

        let (low, high) = match store.get_committed_blocks() {
            Ok((low, high)) => (low, high),
            Err(e) => {
                log::warn!("No committed blocks: {:?}", e);
                let high = engine
                    .load_block_handle(&boot_data)?
                    .context("No handle for already downloaded block")?
                    .id()
                    .clone();
                let low = get_key_block_from_regular(&engine, &high, &account_id)
                    .await
                    .context("Failed getting keyblock from regular")?
                    .id()
                    .clone();
                (low, high)
            }
        };

        (low, high, account_id)
    };

    let active_peers = Arc::new(ActivePeers::default());
    let mut low_id = store.get_seqno().unwrap_or(low.seq_no);
    let mut high_id = high.seq_no;
    loop {
        if low_id == high_id {
            high_id = low_id;
            high = low.clone();
            low = get_key_block_from_regular(&engine, &low, &account_id)
                .await
                .context("Failed getting keyblock from regular")?
                .id()
                .clone();
            store.commit_low_key_block(&low)?;
            store.commit_high_key_block(&high)?;
            low_id = low.seq_no;
        }
        let data = download_archive(&engine, &active_peers, low_id).await?;
        //todo recover errors
        log::info!("Background sync: Parsing archive for block {}", low_id);

        let maps = parse_archive(data)?;
        let last_id = maps
            .mc_block_ids
            .iter()
            .map(|x| x.1.seq_no)
            .last()
            .context("Empty package")?;
        let first_id = maps
            .mc_block_ids
            .iter()
            .map(|x| x.1.seq_no)
            .next()
            .context("Empty package")?;
        log::warn!("First: {}. Last: {}", first_id, last_id);
        for (id, entry) in &maps.blocks {
            let (block, proof) = entry.get_data()?;
            save_block(&engine, id, block, proof)
                .await
                .context("Failed saving block")?;
        }
        log::info!(
            "Background sync:: Parsed {} masterchain blocks, {} blocks total",
            maps.mc_block_ids.len(),
            maps.blocks.len()
        );
        store.commit_seq_no(last_id)?;
        low_id = last_id;
        log::info!("Background sync: committed {:?}", low_id);
    }
}

async fn download_archive(engine: &Engine, peers: &Arc<ActivePeers>, id: u32) -> Result<Vec<u8>> {
    log::info!("Downloading archive for {}", id);
    let arch = engine
        .download_archive(id, peers)
        .await
        .context("Failed downloading archive")?
        .context("Archive not found")?;
    Ok(arch)
}

async fn get_key_block_from_regular(
    engine: &Engine,
    block: &BlockIdExt,
    id: &AccountIdPrefixFull,
) -> Result<Arc<BlockHandle>> {
    let handle = engine
        .load_block_handle(block)?
        .context("No handle for loaded block")?;
    let data = engine.load_block_data(&handle).await?.block().read_info()?;
    let pre_seqno = data.prev_key_block_seqno();

    let key_block = engine.find_block_by_seq_no(id, pre_seqno)?;
    Ok(key_block)
}
