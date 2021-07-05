use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use chrono::TimeZone;
use futures::StreamExt;
use indexer_lib::ExtractInput;
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use node_indexer::{Config, NodeClient};
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use ton_abi::Event;
use ton_block::{MsgAddress, MsgAddressInt, ShardIdent};

const ROOT_ABI: &str = r#"{
	"ABI version": 2,
	"header": ["pubkey", "time", "expire"],
	"functions": [
		{
			"name": "constructor",
			"inputs": [
				{"name":"initial_owner","type":"address"},
				{"name":"initial_vault","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "installPlatformOnce",
			"inputs": [
				{"name":"code","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "installOrUpdateAccountCode",
			"inputs": [
				{"name":"code","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "installOrUpdatePairCode",
			"inputs": [
				{"name":"code","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "getAccountVersion",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"uint32"}
			]
		},
		{
			"name": "getPairVersion",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"uint32"}
			]
		},
		{
			"name": "setVaultOnce",
			"inputs": [
				{"name":"new_vault","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "getVault",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"address"}
			]
		},
		{
			"name": "setActive",
			"inputs": [
				{"name":"new_active","type":"bool"}
			],
			"outputs": [
			]
		},
		{
			"name": "isActive",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"bool"}
			]
		},
		{
			"name": "upgrade",
			"inputs": [
				{"name":"code","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "requestUpgradeAccount",
			"inputs": [
				{"name":"current_version","type":"uint32"},
				{"name":"send_gas_to","type":"address"},
				{"name":"account_owner","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "forceUpgradeAccount",
			"inputs": [
				{"name":"account_owner","type":"address"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "upgradePair",
			"inputs": [
				{"name":"left_root","type":"address"},
				{"name":"right_root","type":"address"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "resetGas",
			"inputs": [
				{"name":"receiver","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "resetTargetGas",
			"inputs": [
				{"name":"target","type":"address"},
				{"name":"receiver","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "getOwner",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"dex_owner","type":"address"}
			]
		},
		{
			"name": "getPendingOwner",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"dex_pending_owner","type":"address"}
			]
		},
		{
			"name": "transferOwner",
			"inputs": [
				{"name":"new_owner","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "acceptOwner",
			"inputs": [
			],
			"outputs": [
			]
		},
		{
			"name": "getExpectedAccountAddress",
			"inputs": [
				{"name":"_answer_id","type":"uint32"},
				{"name":"account_owner","type":"address"}
			],
			"outputs": [
				{"name":"value0","type":"address"}
			]
		},
		{
			"name": "getExpectedPairAddress",
			"inputs": [
				{"name":"_answer_id","type":"uint32"},
				{"name":"left_root","type":"address"},
				{"name":"right_root","type":"address"}
			],
			"outputs": [
				{"name":"value0","type":"address"}
			]
		},
		{
			"name": "deployAccount",
			"inputs": [
				{"name":"account_owner","type":"address"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "deployPair",
			"inputs": [
				{"name":"left_root","type":"address"},
				{"name":"right_root","type":"address"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "onPairCreated",
			"inputs": [
				{"name":"left_root","type":"address"},
				{"name":"right_root","type":"address"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "platform_code",
			"inputs": [
			],
			"outputs": [
				{"name":"platform_code","type":"cell"}
			]
		},
		{
			"name": "account_code",
			"inputs": [
			],
			"outputs": [
				{"name":"account_code","type":"cell"}
			]
		},
		{
			"name": "pair_code",
			"inputs": [
			],
			"outputs": [
				{"name":"pair_code","type":"cell"}
			]
		}
	],
	"data": [
		{"key":1,"name":"_nonce","type":"uint32"}
	],
	"events": [
		{
			"name": "AccountCodeUpgraded",
			"inputs": [
				{"name":"version","type":"uint32"}
			],
			"outputs": [
			]
		},
		{
			"name": "PairCodeUpgraded",
			"inputs": [
				{"name":"version","type":"uint32"}
			],
			"outputs": [
			]
		},
		{
			"name": "RootCodeUpgraded",
			"inputs": [
			],
			"outputs": [
			]
		},
		{
			"name": "ActiveUpdated",
			"inputs": [
				{"name":"new_active","type":"bool"}
			],
			"outputs": [
			]
		},
		{
			"name": "RequestedPairUpgrade",
			"inputs": [
				{"name":"left_root","type":"address"},
				{"name":"right_root","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "RequestedForceAccountUpgrade",
			"inputs": [
				{"name":"account_owner","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "RequestedOwnerTransfer",
			"inputs": [
				{"name":"old_owner","type":"address"},
				{"name":"new_owner","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "OwnerTransferAccepted",
			"inputs": [
				{"name":"old_owner","type":"address"},
				{"name":"new_owner","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "NewPairCreated",
			"inputs": [
				{"name":"left_root","type":"address"},
				{"name":"right_root","type":"address"}
			],
			"outputs": [
			]
		}
	]
}"#;

const DEX_ABI: &str = r#"
    {
	"ABI version": 2,
	"header": ["pubkey", "time", "expire"],
	"functions": [
		{
			"name": "constructor",
			"inputs": [
			],
			"outputs": [
			]
		},
		{
			"name": "resetGas",
			"inputs": [
				{"name":"receiver","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "getRoot",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"dex_root","type":"address"}
			]
		},
		{
			"name": "getTokenRoots",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"left","type":"address"},
				{"name":"right","type":"address"},
				{"name":"lp","type":"address"}
			]
		},
		{
			"name": "getTokenWallets",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"left","type":"address"},
				{"name":"right","type":"address"},
				{"name":"lp","type":"address"}
			]
		},
		{
			"name": "getVersion",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"version","type":"uint32"}
			]
		},
		{
			"name": "getVault",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"dex_vault","type":"address"}
			]
		},
		{
			"name": "getVaultWallets",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"left","type":"address"},
				{"name":"right","type":"address"}
			]
		},
		{
			"name": "setFeeParams",
			"inputs": [
				{"name":"numerator","type":"uint16"},
				{"name":"denominator","type":"uint16"}
			],
			"outputs": [
			]
		},
		{
			"name": "getFeeParams",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"numerator","type":"uint16"},
				{"name":"denominator","type":"uint16"}
			]
		},
		{
			"name": "isActive",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"bool"}
			]
		},
		{
			"name": "getBalances",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"components":[{"name":"lp_supply","type":"uint128"},{"name":"left_balance","type":"uint128"},{"name":"right_balance","type":"uint128"}],"name":"value0","type":"tuple"}
			]
		},
		{
			"name": "buildExchangePayload",
			"inputs": [
				{"name":"id","type":"uint64"},
				{"name":"deploy_wallet_grams","type":"uint128"},
				{"name":"expected_amount","type":"uint128"}
			],
			"outputs": [
				{"name":"value0","type":"cell"}
			]
		},
		{
			"name": "buildDepositLiquidityPayload",
			"inputs": [
				{"name":"id","type":"uint64"},
				{"name":"deploy_wallet_grams","type":"uint128"}
			],
			"outputs": [
				{"name":"value0","type":"cell"}
			]
		},
		{
			"name": "buildWithdrawLiquidityPayload",
			"inputs": [
				{"name":"id","type":"uint64"},
				{"name":"deploy_wallet_grams","type":"uint128"}
			],
			"outputs": [
				{"name":"value0","type":"cell"}
			]
		},
		{
			"name": "tokensReceivedCallback",
			"inputs": [
				{"name":"token_wallet","type":"address"},
				{"name":"token_root","type":"address"},
				{"name":"tokens_amount","type":"uint128"},
				{"name":"sender_public_key","type":"uint256"},
				{"name":"sender_address","type":"address"},
				{"name":"sender_wallet","type":"address"},
				{"name":"original_gas_to","type":"address"},
				{"name":"value7","type":"uint128"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "expectedDepositLiquidity",
			"inputs": [
				{"name":"_answer_id","type":"uint32"},
				{"name":"left_amount","type":"uint128"},
				{"name":"right_amount","type":"uint128"},
				{"name":"auto_change","type":"bool"}
			],
			"outputs": [
				{"components":[{"name":"step_1_left_deposit","type":"uint128"},{"name":"step_1_right_deposit","type":"uint128"},{"name":"step_1_lp_reward","type":"uint128"},{"name":"step_2_left_to_right","type":"bool"},{"name":"step_2_right_to_left","type":"bool"},{"name":"step_2_spent","type":"uint128"},{"name":"step_2_fee","type":"uint128"},{"name":"step_2_received","type":"uint128"},{"name":"step_3_left_deposit","type":"uint128"},{"name":"step_3_right_deposit","type":"uint128"},{"name":"step_3_lp_reward","type":"uint128"}],"name":"value0","type":"tuple"}
			]
		},
		{
			"name": "depositLiquidity",
			"inputs": [
				{"name":"call_id","type":"uint64"},
				{"name":"left_amount","type":"uint128"},
				{"name":"right_amount","type":"uint128"},
				{"name":"expected_lp_root","type":"address"},
				{"name":"auto_change","type":"bool"},
				{"name":"account_owner","type":"address"},
				{"name":"value6","type":"uint32"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "expectedWithdrawLiquidity",
			"inputs": [
				{"name":"_answer_id","type":"uint32"},
				{"name":"lp_amount","type":"uint128"}
			],
			"outputs": [
				{"name":"expected_left_amount","type":"uint128"},
				{"name":"expected_right_amount","type":"uint128"}
			]
		},
		{
			"name": "withdrawLiquidity",
			"inputs": [
				{"name":"call_id","type":"uint64"},
				{"name":"lp_amount","type":"uint128"},
				{"name":"expected_lp_root","type":"address"},
				{"name":"account_owner","type":"address"},
				{"name":"value4","type":"uint32"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "expectedExchange",
			"inputs": [
				{"name":"_answer_id","type":"uint32"},
				{"name":"amount","type":"uint128"},
				{"name":"spent_token_root","type":"address"}
			],
			"outputs": [
				{"name":"expected_amount","type":"uint128"},
				{"name":"expected_fee","type":"uint128"}
			]
		},
		{
			"name": "expectedSpendAmount",
			"inputs": [
				{"name":"_answer_id","type":"uint32"},
				{"name":"receive_amount","type":"uint128"},
				{"name":"receive_token_root","type":"address"}
			],
			"outputs": [
				{"name":"expected_amount","type":"uint128"},
				{"name":"expected_fee","type":"uint128"}
			]
		},
		{
			"name": "exchange",
			"inputs": [
				{"name":"call_id","type":"uint64"},
				{"name":"spent_amount","type":"uint128"},
				{"name":"spent_token_root","type":"address"},
				{"name":"receive_token_root","type":"address"},
				{"name":"expected_amount","type":"uint128"},
				{"name":"account_owner","type":"address"},
				{"name":"value6","type":"uint32"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "checkPair",
			"inputs": [
				{"name":"call_id","type":"uint64"},
				{"name":"account_owner","type":"address"},
				{"name":"value2","type":"uint32"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "upgrade",
			"inputs": [
				{"name":"code","type":"cell"},
				{"name":"new_version","type":"uint32"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "afterInitialize",
			"inputs": [
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "liquidityTokenRootDeployed",
			"inputs": [
				{"name":"lp_root_","type":"address"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "liquidityTokenRootNotDeployed",
			"inputs": [
				{"name":"value0","type":"address"},
				{"name":"send_gas_to","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "expectedWalletAddressCallback",
			"inputs": [
				{"name":"wallet","type":"address"},
				{"name":"wallet_public_key","type":"uint256"},
				{"name":"owner_address","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "platform_code",
			"inputs": [
			],
			"outputs": [
				{"name":"platform_code","type":"cell"}
			]
		},
		{
			"name": "lp_wallet",
			"inputs": [
			],
			"outputs": [
				{"name":"lp_wallet","type":"address"}
			]
		},
		{
			"name": "left_wallet",
			"inputs": [
			],
			"outputs": [
				{"name":"left_wallet","type":"address"}
			]
		},
		{
			"name": "right_wallet",
			"inputs": [
			],
			"outputs": [
				{"name":"right_wallet","type":"address"}
			]
		},
		{
			"name": "vault_left_wallet",
			"inputs": [
			],
			"outputs": [
				{"name":"vault_left_wallet","type":"address"}
			]
		},
		{
			"name": "vault_right_wallet",
			"inputs": [
			],
			"outputs": [
				{"name":"vault_right_wallet","type":"address"}
			]
		},
		{
			"name": "lp_root",
			"inputs": [
			],
			"outputs": [
				{"name":"lp_root","type":"address"}
			]
		},
		{
			"name": "lp_supply",
			"inputs": [
			],
			"outputs": [
				{"name":"lp_supply","type":"uint128"}
			]
		},
		{
			"name": "left_balance",
			"inputs": [
			],
			"outputs": [
				{"name":"left_balance","type":"uint128"}
			]
		},
		{
			"name": "right_balance",
			"inputs": [
			],
			"outputs": [
				{"name":"right_balance","type":"uint128"}
			]
		}
	],
	"data": [
	],
	"events": [
		{
			"name": "PairCodeUpgraded",
			"inputs": [
				{"name":"version","type":"uint32"}
			],
			"outputs": [
			]
		},
		{
			"name": "FeesParamsUpdated",
			"inputs": [
				{"name":"numerator","type":"uint16"},
				{"name":"denominator","type":"uint16"}
			],
			"outputs": [
			]
		},
		{
			"name": "DepositLiquidity",
			"inputs": [
				{"name":"left","type":"uint128"},
				{"name":"right","type":"uint128"},
				{"name":"lp","type":"uint128"}
			],
			"outputs": [
			]
		},
		{
			"name": "WithdrawLiquidity",
			"inputs": [
				{"name":"lp","type":"uint128"},
				{"name":"left","type":"uint128"},
				{"name":"right","type":"uint128"}
			],
			"outputs": [
			]
		},
		{
			"name": "ExchangeLeftToRight",
			"inputs": [
				{"name":"left","type":"uint128"},
				{"name":"fee","type":"uint128"},
				{"name":"right","type":"uint128"}
			],
			"outputs": [
			]
		},
		{
			"name": "ExchangeRightToLeft",
			"inputs": [
				{"name":"right","type":"uint128"},
				{"name":"fee","type":"uint128"},
				{"name":"left","type":"uint128"}
			],
			"outputs": [
			]
		}
	]
}
    "#;

fn prep_event() -> [Event; 4] {
    let contract = ton_abi::Contract::load(std::io::Cursor::new(DEX_ABI)).unwrap();
    let mem = contract.events();
    let id1 = mem.get("DepositLiquidity").unwrap();
    let parse_ev1 = contract.event_by_id(id1.id).unwrap();
    let id2 = mem.get("WithdrawLiquidity").unwrap();
    let parse_ev2 = contract.event_by_id(id2.id).unwrap();
    let id3 = mem.get("ExchangeLeftToRight").unwrap();
    let parse_ev3 = contract.event_by_id(id3.id).unwrap();
    let id4 = mem.get("ExchangeRightToLeft").unwrap();
    let parse_ev4 = contract.event_by_id(id4.id).unwrap();
    let evs = [
        parse_ev1.clone(),
        parse_ev2.clone(),
        parse_ev3.clone(),
        parse_ev4.clone(),
    ];
    evs
}

fn main() {
    let stdout = ConsoleAppender::builder().build();

    let trace = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
        .build("trace.log")
        .unwrap();

    let config = log4rs::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("trace", Box::new(trace)))
        .logger(Logger::builder().build("app::backend::db", LevelFilter::Info))
        .logger(
            Logger::builder()
                .appender("trace")
                .additive(false)
                .build("node_indexer", LevelFilter::Trace),
        )
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();

    let handle = log4rs::init_config(config).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("my-pool-{}", id)
        })
        .build()
        .unwrap();

    log::info!("Started");
    rt.block_on(async move {
        let mut config = Config::default();
        // config.pool_size = 4;
        let node = Arc::new(NodeClient::new(config).await.unwrap());
        log::info!("here");

        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        let (tx_block, mut rx_block) = futures::channel::mpsc::unbounded();
        node.spawn_indexer(
            Some(ton_api::ton::ton_node::blockid::BlockId {
                workchain: -1,
                shard: u64::from_str_radix("8000000000000000", 16).unwrap() as i64,
                seqno: 9501034,
            }),
            tx,
            tx_block,
        )
        .await
        .unwrap();
        let abi = prep_event();
        //
        // let all = node
        //     .get_all_transactions(
        //         MsgAddressInt::from_str(
        //             "0:943bad2e74894aa28ae8ddbe673be09a0f3818fd170d12b4ea8ef1ea8051e940",
        //         )
        //         .unwrap(),
        //     )
        //     .await
        //     .unwrap();
        // let parsed: Result<Vec<_>> = all
        //     .into_iter()
        //     .map(|x| {
        //         {
        //             ExtractInput {
        //                 transaction: &x.data,
        //                 what_to_extract: &abi,
        //             }
        //         }
        //         .process()
        //     })
        //     .collect();
        // dbg!(parsed
        //     .unwrap()
        //     .into_iter()
        //     .for_each(|x| println!("{:?}", x.output)));

        let mut rx = rx.enumerate();
        while let Some((a, _)) = rx.next().await {
            if a == 500000 {
                break;
            }
        }

        // // let mut pre = &block_seqnos[0];
        // // for a in &block_seqnos {
        // //     if (pre - a) > 1 {
        // //         dbg!(block_seqnos)
        // //     }
        // // }
        // for (k, mut v) in seqnos {
        //     v.sort_unstable();
        //
        //     let start = v.first().unwrap();
        //     let end = v.last().unwrap();
        //     let gen = (*start..end + 1);
        //     let mut flag = false;
        //     for (seq, expected) in v.clone().iter().zip(gen) {
        //         if *seq != expected {
        //             flag = true;
        //         }
        //     }
        //     if flag {
        //         println!("{:016x}", k);
        //         dbg!(v);
        //     }
        // }
    })
}
