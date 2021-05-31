use std::sync::Arc;

use futures::StreamExt;
use node_indexer::{Config, NodeClient};
use ton_block::{Block, GetRepresentationHash};

#[tokio::main]
async fn main() {
    env_logger::init();
    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let config = Config::default();
    let node = Arc::new(NodeClient::new(config, 100).await.unwrap());
    log::info!("here");
    node.spawn_indexer(tx).await.unwrap();
    let mut rx = rx.enumerate();
    while let Some((n, ref a)) = rx.next().await {
        println!("{}", n);
        // let data: Block = a;
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
    }
}
