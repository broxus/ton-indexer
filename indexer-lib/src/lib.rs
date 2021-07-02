use std::fmt::Debug;

use anyhow::Result;
use ton_abi::Event;
use ton_block::{
    AccountBlock, CurrencyCollection, Deserializable, GetRepresentationHash, MsgAddressInt,
    Transaction,
};
use ton_types::SliceData;

pub use crate::extension::TransactionExt;

mod extension;

#[derive(Debug, Clone)]
pub struct ParsedOutput<T: Clone + Debug> {
    pub transaction: Transaction,
    pub output: Vec<T>,
}

pub struct ExtractInput<'a, W> {
    pub transaction: &'a Transaction,
    pub what_to_extract: &'a [W],
}

impl<W> ExtractInput<'_, W>
where
    W: Extractable,
{
    pub fn process(&self) -> Result<ParsedOutput<<W as Extractable>::Output>> {
        let messages = self.messages()?;
        let mut output = Vec::new();

        for parser in self.what_to_extract {
            let mut res = match parser.extract(&messages) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed parsing messages: {}", e);
                    continue;
                }
            };
            output.append(&mut res);
        }
        Ok(ParsedOutput {
            transaction: self.transaction.clone(),
            output,
        })
    }
}

pub trait Extractable {
    type Output: Clone + Debug;
    fn extract(&self, messages: &TransactionMessages) -> Result<Vec<Self::Output>>;
}

impl Extractable for Event {
    type Output = ParsedEvent;

    fn extract(&self, messages: &TransactionMessages) -> Result<Vec<Self::Output>> {
        let mut result = vec![];
        for message in &messages.out_messages {
            let message_tokens = match process_event_message(&message, &self) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed processing event messages: {:?}", e);
                    continue;
                }
            };
            match message_tokens {
                None => {}
                Some(output) => {
                    result.push(ParsedEvent {
                        function_name: self.name.clone(),
                        input: output.tokens,
                        message_hash: message
                            .msg
                            .hash()
                            .map(|x| *x.as_slice())
                            .expect("If message is parsed, than hash is ok"),
                    });
                }
            }
        }
        Ok(result)
    }
}

impl Extractable for ton_abi::Function {
    type Output = ParsedFunction;

    fn extract(&self, messages: &TransactionMessages) -> Result<Vec<Self::Output>> {
        let input = if let Some(message) = &messages.in_message {
            process_function_in_message(&message, &self)?
        } else {
            anyhow::bail!("No in messages")
        };
        let abi_out_messages_tokens = process_function_out_messages(&messages.out_messages, &self)?;

        Ok(vec![ParsedFunction {
            function_name: self.name.clone(),
            input,
            output: abi_out_messages_tokens,
        }])
    }
}

#[derive(Debug, Clone)]
pub struct ParsedFunction {
    pub function_name: String,
    pub input: Option<Vec<ton_abi::Token>>,
    pub output: Option<Vec<ton_abi::Token>>,
}

#[derive(Debug, Clone)]
pub struct ParsedEvent {
    pub function_name: String,
    pub input: Vec<ton_abi::Token>,
    pub message_hash: [u8; 32],
}

/// # Returns
/// Ok `Some` if block has account block
pub fn extract_from_block<W, O>(
    block: &ton_block::Block,
    what_to_extract: &[W],
) -> Result<Option<Vec<ParsedOutput<O>>>>
where
    W: Extractable + Extractable<Output = O>,
    O: Clone + Debug,
{
    use ton_types::HashmapType;

    // parsing account blocks from generic blocks according to `HashMapAugType::dump`
    let account_blocks = match block
        .extra
        .read_struct()
        .and_then(|extra| extra.read_account_blocks())
        .map(|x| {
            let mut account_blocks = Vec::new();
            let _ = x.iterate_slices(|_, ref mut value| {
                let _ = <CurrencyCollection>::construct_from(value);
                let res = <AccountBlock>::construct_from(value);
                match res {
                    Ok(a) => account_blocks.push(a),
                    Err(e) => {
                        log::error!("Failed parsing account block {}", e);
                    }
                };
                Ok(true)
            });
            account_blocks
        }) {
        Ok(account_blocks) => account_blocks,
        _ => return Ok(None), // no account blocks found
    };

    let mut result = vec![];
    for account_block in account_blocks {
        for item in account_block.transactions().iter() {
            let transaction = match item.and_then(|(_, value)| {
                let cell = value.into_cell().reference(0)?;
                ton_block::Transaction::construct_from_cell(cell)
            }) {
                Ok(transaction) => transaction,
                Err(e) => {
                    log::error!("Failed creating transaction from cell: {}", e);
                    continue;
                }
            };
            let input = ExtractInput {
                transaction: &transaction,
                what_to_extract,
            };
            let extracted_values = match input.process() {
                Ok(a) => a,
                Err(e) => {
                    log::error!("Failed parsing transaction: {}", e);
                    continue;
                }
            };
            result.push(extracted_values);
        }
    }
    Ok(Some(result))
}

pub fn address_from_account_id(address: SliceData, workchain_id: i8) -> Result<MsgAddressInt> {
    let address =
        match MsgAddressInt::with_standart(None, workchain_id, address.get_bytestring(0).into()) {
            Ok(a) => a,
            Err(e) => {
                anyhow::bail!("Failed creating address from account id: {}", e);
            }
        };
    Ok(address)
}

#[derive(Debug, Clone)]
struct ProcessFunctionOutput {
    tokens: Vec<ton_abi::Token>,
    time: u32,
}

fn process_function_out_messages(
    messages: &[MessageData],
    abi_function: &ton_abi::Function,
) -> Result<Option<Vec<ton_abi::Token>>, AbiError> {
    let mut output = None;
    for msg in messages {
        let MessageData { msg, .. } = msg;
        let is_internal = msg.is_internal();
        let body = msg.body().ok_or(AbiError::InvalidOutputMessage)?;

        if abi_function
            .is_my_output_message(body.clone(), is_internal)
            .map_err(|e| AbiError::DecodingError(e.to_string()))?
        {
            let tokens = abi_function
                .decode_output(body, is_internal)
                .map_err(|e| AbiError::DecodingError(e.to_string()))?;

            output = Some(tokens);
            break;
        }
    }

    match output {
        Some(a) => Ok(Some(a)),
        None if !abi_function.has_output() => Ok(None),
        _ => Err(AbiError::NoMessagesProduced),
    }
}

fn process_function_in_message(
    msg: &MessageData,
    abi_function: &ton_abi::Function,
) -> Result<Option<Vec<ton_abi::Token>>, AbiError> {
    let mut input = None;
    let MessageData { msg, .. } = msg;

    let is_internal = msg.is_internal();
    log::info!("{}", is_internal);
    dbg!(is_internal);
    let body = msg.body().ok_or(AbiError::InvalidOutputMessage)?;
    if abi_function
        .is_my_input_message(body.clone(), is_internal)
        .map_err(|e| AbiError::DecodingError(e.to_string()))?
    {
        let tokens = abi_function
            .decode_input(body, is_internal)
            .map_err(|e| AbiError::DecodingError(e.to_string()))?;

        input = Some(tokens);
    }

    match input {
        Some(a) => Ok(Some(a)),
        None if !abi_function.has_input() => Ok(None),
        _ => Err(AbiError::NoMessagesProduced),
    }
}

fn process_event_message(
    msg: &MessageData,
    abi_function: &ton_abi::Event,
) -> Result<Option<ProcessFunctionOutput>, AbiError> {
    let mut input = None;
    let MessageData { time, msg } = msg;

    if !matches!(msg.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
        return Ok(None);
    }
    let body = msg.body().ok_or(AbiError::InvalidOutputMessage)?;
    if abi_function
        .is_my_message(body.clone(), false)
        .map_err(|e| AbiError::DecodingError(e.to_string()))?
    {
        let tokens = abi_function
            .decode_input(body)
            .map_err(|e| AbiError::DecodingError(e.to_string()))?;

        input = Some(tokens);
    }

    match input {
        Some(a) => Ok(Some(ProcessFunctionOutput {
            tokens: a,
            time: *time,
        })),
        None if !abi_function.has_input() => Ok(None),
        _ => Err(AbiError::NoMessagesProduced),
    }
}

pub fn parse_transaction_messages(
    transaction: &ton_block::Transaction,
) -> Result<TransactionMessages, AbiError> {
    let mut out_messages = Vec::new();
    transaction
        .out_msgs
        .iterate_slices(|slice| {
            if let Ok(message) = slice
                .reference(0)
                .and_then(ton_block::Message::construct_from_cell)
            {
                let message = MessageData {
                    time: transaction.now(),
                    msg: message,
                };
                out_messages.push(message);
            }
            Ok(true)
        })
        .map_err(|e| AbiError::DecodingError(e.to_string()))?;

    let in_message = transaction
        .read_in_msg()
        .map_err(|e| AbiError::DecodingError(e.to_string()))?
        .map(|x| MessageData {
            time: transaction.now(),
            msg: x,
        });

    Ok(TransactionMessages {
        in_message,
        out_messages,
    })
}

#[derive(Debug, Clone)]
pub struct MessageData {
    pub time: u32,
    pub msg: ton_block::Message,
}

#[derive(Debug)]
pub struct TransactionMessages {
    pub in_message: Option<MessageData>,
    pub out_messages: Vec<MessageData>,
}

#[derive(thiserror::Error, Debug)]
pub enum AbiError {
    #[error("Invalid output message")]
    InvalidOutputMessage,
    #[error("No external output messages")]
    NoMessagesProduced,
    #[error("Failed decoding: `{0}`")]
    DecodingError(String),
}

#[cfg(test)]
mod test {
    use ton_block::{Block, Deserializable};

    use crate::extract_functions_from_block;

    const abi: &str = r#"{
	"ABI version": 2,
	"header": ["pubkey", "time", "expire"],
	"functions": [
		{
			"name": "constructor",
			"inputs": [
				{"name":"owners","type":"uint256[]"},
				{"name":"reqConfirms","type":"uint8"}
			],
			"outputs": [
			]
		},
		{
			"name": "acceptTransfer",
			"inputs": [
				{"name":"payload","type":"bytes"}
			],
			"outputs": [
			]
		},
		{
			"name": "sendTransaction",
			"inputs": [
				{"name":"dest","type":"address"},
				{"name":"value","type":"uint128"},
				{"name":"bounce","type":"bool"},
				{"name":"flags","type":"uint8"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "submitTransaction",
			"inputs": [
				{"name":"dest","type":"address"},
				{"name":"value","type":"uint128"},
				{"name":"bounce","type":"bool"},
				{"name":"allBalance","type":"bool"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
				{"name":"transId","type":"uint64"}
			]
		},
		{
			"name": "confirmTransaction",
			"inputs": [
				{"name":"transactionId","type":"uint64"}
			],
			"outputs": [
			]
		},
		{
			"name": "isConfirmed",
			"inputs": [
				{"name":"mask","type":"uint32"},
				{"name":"index","type":"uint8"}
			],
			"outputs": [
				{"name":"confirmed","type":"bool"}
			]
		},
		{
			"name": "getParameters",
			"inputs": [
			],
			"outputs": [
				{"name":"maxQueuedTransactions","type":"uint8"},
				{"name":"maxCustodianCount","type":"uint8"},
				{"name":"expirationTime","type":"uint64"},
				{"name":"minValue","type":"uint128"},
				{"name":"requiredTxnConfirms","type":"uint8"}
			]
		},
		{
			"name": "getTransaction",
			"inputs": [
				{"name":"transactionId","type":"uint64"}
			],
			"outputs": [
				{"components":[{"name":"id","type":"uint64"},{"name":"confirmationsMask","type":"uint32"},{"name":"signsRequired","type":"uint8"},{"name":"signsReceived","type":"uint8"},{"name":"creator","type":"uint256"},{"name":"index","type":"uint8"},{"name":"dest","type":"address"},{"name":"value","type":"uint128"},{"name":"sendFlags","type":"uint16"},{"name":"payload","type":"cell"},{"name":"bounce","type":"bool"}],"name":"trans","type":"tuple"}
			]
		},
		{
			"name": "getTransactions",
			"inputs": [
			],
			"outputs": [
				{"components":[{"name":"id","type":"uint64"},{"name":"confirmationsMask","type":"uint32"},{"name":"signsRequired","type":"uint8"},{"name":"signsReceived","type":"uint8"},{"name":"creator","type":"uint256"},{"name":"index","type":"uint8"},{"name":"dest","type":"address"},{"name":"value","type":"uint128"},{"name":"sendFlags","type":"uint16"},{"name":"payload","type":"cell"},{"name":"bounce","type":"bool"}],"name":"transactions","type":"tuple[]"}
			]
		},
		{
			"name": "getTransactionIds",
			"inputs": [
			],
			"outputs": [
				{"name":"ids","type":"uint64[]"}
			]
		},
		{
			"name": "getCustodians",
			"inputs": [
			],
			"outputs": [
				{"components":[{"name":"index","type":"uint8"},{"name":"pubkey","type":"uint256"}],"name":"custodians","type":"tuple[]"}
			]
		}
	],
	"data": [
	],
	"events": [
		{
			"name": "TransferAccepted",
			"inputs": [
				{"name":"payload","type":"bytes"}
			],
			"outputs": [
			]
		}
	]
}"#;
    const block_boc :&str="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";

    #[test]
    fn parse() {
        let fns = [ton_abi::Contract::load(std::io::Cursor::new(abi))
            .unwrap()
            .function("submitTransaction")
            .unwrap()
            .clone()];

        let block = Block::construct_from_base64(block_boc).unwrap();
        let res = extract_functions_from_block(&fns, &block).unwrap().unwrap();
    }

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

    #[test]
    fn parse_event() {
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
        dbg!(&evs);
        let block = Block::construct_from_base64("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").unwrap();
        let res = super::extract_events_from_block(&evs, &block).unwrap();
        dbg!(res);
    }
}
