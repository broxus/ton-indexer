use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use tiny_adnl::utils::*;
use ton_block::BinTreeType;

use super::Engine;
use crate::storage::StatesGcResolver;

#[derive(Default)]
pub struct DefaultStateGcResolver {
    last_processed_seqno: AtomicU32,
    min_ref_mc_seqno: AtomicU32,
    top_shard_seqno: FxDashMap<ton_block::ShardIdent, AtomicU32>,
}

impl DefaultStateGcResolver {
    pub async fn advance(
        &self,
        engine: &Engine,
        mc_block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        // Advance last processed block
        let seqno = mc_block_id.seq_no;
        let last_processed_seqno = self.last_processed_seqno.fetch_max(seqno, Ordering::AcqRel);
        if seqno <= last_processed_seqno {
            return Ok(());
        }

        // Load current state
        let mc_state = engine.load_state(mc_block_id).await?;
        let new_min_ref_mc_seqno = mc_state.state().min_ref_mc_seqno();

        // Pre-check min ref mc seqno
        let old_min_ref_mc_seqno = self.min_ref_mc_seqno.load(Ordering::Acquire);
        if new_min_ref_mc_seqno <= old_min_ref_mc_seqno {
            return Ok(());
        }

        // Find min ref mc block
        let mc_prefix = ton_block::AccountIdPrefixFull::any_masterchain();
        let handle = match engine.find_block_by_seq_no(&mc_prefix, new_min_ref_mc_seqno) {
            Ok(handle) => handle,
            Err(e) => {
                log::warn!("Cancelling GC advance: {:?}", e);
                return Ok(());
            }
        };

        // Update min ref mc and ensure that we have something to do
        let old_min_ref_mc_seqno = self
            .min_ref_mc_seqno
            .fetch_max(new_min_ref_mc_seqno, Ordering::AcqRel);
        if new_min_ref_mc_seqno <= old_min_ref_mc_seqno {
            return Ok(());
        }

        // Load top blocks from shards
        let min_mc_state = engine.load_state(handle.id()).await?;
        let shards = min_mc_state.shards()?;

        let mut current_shards = FxHashSet::with_capacity_and_hasher(16, Default::default());
        if let Some(ton_block::InRefValue(tree)) = shards.get(&ton_block::BASE_WORKCHAIN_ID)? {
            tree.iterate(|prefix, shard_descr| {
                let shard_id =
                    ton_block::ShardIdent::with_prefix_slice(ton_block::BASE_WORKCHAIN_ID, prefix)?;
                let seq_no = shard_descr.seq_no;

                match self.top_shard_seqno.entry(shard_id) {
                    Entry::Occupied(entry) => {
                        entry.get().fetch_max(seq_no, Ordering::Release);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(AtomicU32::new(seq_no));
                    }
                }

                current_shards.insert(shard_id);
                Ok(true)
            })?;
        }

        // Retain only existing shards
        self.top_shard_seqno
            .retain(|item, _| current_shards.contains(item));

        Ok(())
    }
}

impl StatesGcResolver for DefaultStateGcResolver {
    fn state_expired(&self, block_id: &ton_block::BlockIdExt) -> Result<bool> {
        Ok(if block_id.shard_id.is_masterchain() {
            if block_id.seq_no == 0 {
                false
            } else {
                block_id.seq_no < self.min_ref_mc_seqno.load(Ordering::Acquire)
            }
        } else {
            match self.top_shard_seqno.get(&block_id.shard_id) {
                Some(entry) => block_id.seq_no < entry.load(Ordering::Acquire),
                None => self
                    .top_shard_seqno
                    .iter()
                    .find(|entry| block_id.shard_id.intersect_with(entry.key()))
                    .map(|entry| block_id.seq_no < entry.load(Ordering::Acquire))
                    .unwrap_or_default(),
            }
        })
    }
}
