use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::TimeZone;
use futures::StreamExt;
use ton_abi::Event;
use ton_block::{MsgAddress, MsgAddressInt};

use indexer_lib::{extract_events_from_block, extract_functions_from_block};
use node_indexer::{Config, NodeClient};

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
        let (tx, rx) = futures::channel::mpsc::unbounded();
        let (tx_block, rx_block) = futures::channel::mpsc::unbounded();
        let mut config = Config::default();
        config.adnl.server_address = "44.192.25.57:3031".parse().unwrap();
        let node = Arc::new(NodeClient::new(config).await.unwrap());
        log::info!("here");

        node.spawn_indexer(
            // Some(BlockId {
            //     workchain: -1,
            //     shard: u64::from_str_radix("8000000000000000", 16).unwrap() as i64,
            //     seqno: 9149427,
            // }),
            None, tx, tx_block,
        )
        .await
        .unwrap();

        let contract = ton_abi::Contract::load(std::io::Cursor::new(ROOT_ABI)).unwrap();
        let fun = contract.function("getExpectedPairAddress").unwrap();
        let input = ton_abi::TokenValue::Address(
            MsgAddress::from_str(
                "0:943bad2e74894aa28ae8ddbe673be09a0f3818fd170d12b4ea8ef1ea8051e940",
            )
            .unwrap(),
        );
        let left = ton_abi::Token::new("left_root", input.clone());
        let right = ton_abi::Token::new("right_root", input);
        let id = ton_abi::Token::new(
            "_answer_id",
            ton_abi::TokenValue::Uint(ton_abi::Uint::new(1337, 32)),
        );
        let res = node
            .run_local(
                MsgAddressInt::from_str(
                    "0:943bad2e74894aa28ae8ddbe673be09a0f3818fd170d12b4ea8ef1ea8051e940",
                )
                .unwrap(),
                fun,
                &[id, left, right],
            )
            .await;
        dbg!(res);
        return;
        let mut rx = rx.enumerate();
        let evs = prep_event();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open("res.txt")
            .unwrap();
        while let Some((n, a)) = rx.next().await {
            // let block = a.clone();
            // let extra :BlockExtra= block.extra
            //     .read_struct().unwrap();
            //
            let res = extract_events_from_block(&evs, &a).unwrap();
            if let Some(a) = res {
                if !a.is_empty() {
                    log::info!(
                        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA: {:?}",
                        a
                    );
                    file.write_all(format!("{:?}", a).as_ref()).unwrap();
                    // return;
                }
            };
            let now = a.info.read_struct().unwrap().gen_utime().0;
            let time = chrono::Utc.timestamp(now as i64, 0);
            let diff = chrono::Utc::now() - time;
            // println!("{}", diff);
            if diff.num_seconds() < 40 {
                println!("Synced");
            }
        }
    })
}
