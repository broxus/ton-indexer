## TON Indexer

This project is a simplified, refactored and optimized version of
[ton-labs-node](https://github.com/tonlabs/ton-labs-node) and would not have
been implemented without their gigantic work of reverse engineering the original
С++ node.

---

**NOTE**

It's worth noting that the name "ton-indexer" can be a bit misleading, as it
implies that the repository is focused on indexing the TVM network. However, the
repository is primarily focused on providing a simplified implementation of a
TVM light node.

---

### Run examples

1. Create config `config.yaml`

```yaml
---
indexer:
  adnl_keys:
    dht_key: "0092a775abde7539df5fab97ac32e644eb3bc3711c9136ef3a96be8e290df111"
    overlay_key: "f0db73bee16d51b5372540a50a7ed61c11e1527e5f6194579ec9fff3f89be222"
  rocks_db_path: "./db/rocksdb"
  file_db_path: "./db/file"
```

2. Download network global config

```bash
wget https://raw.githubusercontent.com/tonlabs/net.ton.dev/master/configs/net.ton.dev/ton-global.config.json
```

3. Run simple node

```bash
cargo run --release --example simple_node -- --config config.yaml --global-config ton-global.config.json
```
