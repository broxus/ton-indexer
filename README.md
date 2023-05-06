<p align="center">
  <a href="https://github.com/venom-blockchain/developer-program">
    <img src="https://raw.githubusercontent.com/venom-blockchain/developer-program/main/vf-dev-program.png" alt="Logo" width="366.8" height="146.4">
  </a>
</p>

# TON Indexer

## About

This project is a simplified, refactored and optimized version of [ever-node](https://github.com/tonlabs/ever-node)
and would not have been implemented without their gigantic work of reverse engineering the original С++ node.

## Usage

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
wget https://raw.githubusercontent.com/tonlabs/net.ton.dev/master/configs/ton-global.config.json
```

3. Run simple node

```bash
cargo run --release --example simple_node -- --config config.yaml --global-config ton-global.config.json
```

## Contributing

We welcome contributions to the project! If you notice any issues or errors, feel free to open an issue or submit a pull request.

## License

This project is licensed under the [License Apache].
