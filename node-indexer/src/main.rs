use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::TimeZone;
use futures::StreamExt;
use ton_block::{Block, BlockIdExt, GetRepresentationHash, Serializable};

use node_indexer::{Config, NodeClient};

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("my-pool-{}", id)
        })
        .build()
        .unwrap();

    env_logger::init();
    log::info!("Started");
    rt.block_on(async move {
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        let mut config = Config::default();
        config.adnl.server_address = ":3031".parse().unwrap();
        let node = Arc::new(NodeClient::new(config, 10).await.unwrap());
        log::info!("here");
        node.spawn_indexer(
            // BlockId {
            //     workchain: -1,
            //     shard: u64::from_str_radix("8000000000000000", 16).unwrap() as i64,
            //     seqno: 9012000,
            // },
            tx,
        )
        .await
        .unwrap();
        let mut rx = rx.enumerate();
        let mut map = HashMap::new();
        while let Some((n, a)) = rx.next().await {
            // println!("{}", n);
            let data: Block = a;

            let data = data.info.read_struct().unwrap();
            let bl_time = data.gen_utime().0;
            // time = time.max(bl_time);
            // if time == bl_time {
            let now = chrono::Utc::now();
            let hash = data.serialize().unwrap().repr_hash();
            let block = chrono::Utc.timestamp(bl_time as i64, 0);
            log::info!("Diff: {}", (now - block).num_seconds());
            // }

            *map.entry(hex::encode(hash.as_slice())).or_insert(0) += 1;
            // let info = data.read_info().unwrap();
            // let hash = info.hash().unwrap();
            // let seq = info.seq_no();
            // let wc = info.shard().workchain_id();
            // let shard = info.shard().shard_prefix_with_tag();
            // println!(
            //     "Hash: {:?} Seq: {} Wc: {}, shard: {:016x}",
            //     hash, seq, wc, shard
            // );
            // return;
            if n == 1200 {
                break;
            }
        }
        dbg!(map
            .into_iter()
            .filter(|x| x.1 != 1)
            .collect::<HashMap<_, _>>())
    });
}
