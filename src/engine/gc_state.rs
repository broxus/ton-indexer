use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tiny_adnl::utils::*;
use ton_block::BinTreeType;

use super::Engine;

#[derive(Default)]
pub struct GcState {
    last_processed_block: AtomicU32,
    min_ref_mc_seqno: AtomicU32,
    top_shard_seqno: FxDashMap<ton_block::ShardIdent, AtomicU32>,
}

impl GcState {
    pub async fn advance(
        &self,
        mc_block_id: &ton_block::BlockIdExt,
        engine: &Arc<Engine>,
    ) -> Result<()> {
        use dashmap::mapref::entry::Entry;

        let seqno = mc_block_id.seq_no;
        if seqno <= self.last_processed_block.fetch_max(seqno, Ordering::AcqRel) {
            return Ok(());
        }

        let mc_state = engine.load_state(mc_block_id).await?;
        let new_min_ref_mc_seqno = mc_state.state().min_ref_mc_seqno();
        let old_min_ref_mc_seqno = self
            .min_ref_mc_seqno
            .fetch_max(new_min_ref_mc_seqno, Ordering::AcqRel);

        if new_min_ref_mc_seqno <= old_min_ref_mc_seqno {
            return Ok(());
        }

        let mc_prefix = ton_block::AccountIdPrefixFull::any_masterchain();
        let handle = engine.find_block_by_seq_no(&mc_prefix, new_min_ref_mc_seqno)?;
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

        self.top_shard_seqno
            .retain(|item, _| current_shards.contains(item));

        Ok(())
    }

    pub fn state_expired(&self, block_id: &ton_block::BlockIdExt) -> Result<bool> {
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
