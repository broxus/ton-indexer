use ton_block::{AccountBlock, Deserializable, MsgAddressInt};
use ton_types::BuilderData;

#[derive(Debug)]
pub struct TonExtractor {
    addresses: Vec<MsgAddressInt>,
    ton_abi_functions: Vec<ton_abi::Function>,
}

pub struct ParsedValue {
    pub address: MsgAddressInt,
    pub function_name: String,
    pub input: Vec<ton_abi::Token>,
    pub output: Vec<ton_abi::Token>,
}

trait Extractor {
    fn handle_block(&self, block: ton_block::Block) -> Vec<ParsedValue>;
}

impl TonExtractor {
    pub fn new(addresses: Vec<MsgAddressInt>, ton_abi_functions: Vec<ton_abi::Function>) -> Self {
        Self {
            addresses,
            ton_abi_functions,
        }
    }
}

impl Extractor for TonExtractor {
    fn handle_block(&self, block: ton_block::Block) -> Vec<ParsedValue> {
        match parse_block(&self.addresses, &self.ton_abi_functions, &block) {
            Ok(res) => res.unwrap_or_default(),
            Err(e) => {
                log::error!("error on parsing block - {}", e);
                vec![]
            }
        }
    }
}

pub fn parse_block(
    addresses: &[MsgAddressInt],
    ton_abi_functions: &[ton_abi::Function],
    block: &ton_block::Block,
) -> Result<Option<Vec<ParsedValue>>, anyhow::Error> {
    use ton_block::HashmapAugType;
    use ton_types::HashmapType;

    let account_blocks: Vec<(MsgAddressInt, AccountBlock)> = match block
        .extra
        .read_struct()
        .and_then(|extra| extra.read_account_blocks())
    {
        Ok(account_blocks) => account_blocks
            .iter()
            .filter_map(|account_block| {
                if let Ok((builder, mut slice)) = account_block {
                    for address in addresses {
                        let addr = BuilderData::with_bitstring(address.address().get_bytestring(0))
                            .unwrap_or_default();
                        if builder == addr {
                            return AccountBlock::construct_from(&mut slice)
                                .ok()
                                .map(|b| (address.clone(), b));
                        }
                    }
                }
                None
            })
            .collect(),
        _ => return Ok(None), // no account blocks found
    };

    let mut result = vec![];
    for (address, account_block) in account_blocks {
        for item in account_block.transactions().iter() {
            let transaction = match item.and_then(|(_, value)| {
                let cell = value.into_cell().reference(0)?;
                ton_block::Transaction::construct_from_cell(cell)
            }) {
                Ok(transaction) => transaction,
                Err(_) => continue,
            };

            let messages = parse_transaction_messages(&transaction)?;
            for ton_abi_function in ton_abi_functions {
                let abi_out_messages_tokens =
                    process_out_messages(&messages.out_messages, ton_abi_function)
                        .unwrap_or_default();
                if !abi_out_messages_tokens.is_empty() {
                    result.push(ParsedValue {
                        address: address.clone(),
                        function_name: ton_abi_function.name.clone(),
                        input: vec![],
                        output: abi_out_messages_tokens,
                    });
                }
            }

            if let Some(message) = messages.in_message {
                for ton_abi_function in ton_abi_functions {
                    let abi_in_message_tokens =
                        process_in_message(&message, ton_abi_function).unwrap_or_default();
                    if !abi_in_message_tokens.is_empty() {
                        result.push(ParsedValue {
                            address: address.clone(),
                            function_name: ton_abi_function.name.clone(),
                            input: abi_in_message_tokens,
                            output: vec![],
                        });
                        break;
                    }
                }
            }
        }
    }
    return Ok(Some(result));
}

pub fn process_out_messages(
    messages: &[ton_block::Message],
    abi_function: &ton_abi::Function,
) -> Result<Vec<ton_abi::Token>, anyhow::Error> {
    let mut output = None;

    for msg in messages {
        if !matches!(msg.header(), ton_block::CommonMsgInfo::ExtOutMsgInfo(_)) {
            continue;
        }

        let body = msg.body().ok_or(AbiError::InvalidOutputMessage)?;

        if abi_function
            .is_my_output_message(body.clone(), false)
            .map_err(|e| anyhow::anyhow!("{}", e))?
        {
            let tokens = abi_function
                .decode_output(body, false)
                .map_err(|e| anyhow::anyhow!("{}", e))?;

            output = Some(tokens);
            break;
        }
    }

    match output {
        Some(a) => Ok(a),
        None if !abi_function.has_output() => Ok(Default::default()),
        _ => Err(AbiError::NoMessagesProduced.into()),
    }
}

pub fn process_in_message(
    msg: &ton_block::Message,
    abi_function: &ton_abi::Function,
) -> Result<Vec<ton_abi::Token>, anyhow::Error> {
    let mut input = None;

    if !matches!(msg.header(), ton_block::CommonMsgInfo::ExtInMsgInfo(_)) {
        return Ok(vec![]);
    }

    let body = msg.body().ok_or(AbiError::InvalidOutputMessage)?;

    if abi_function
        .is_my_input_message(body.clone(), false)
        .map_err(|e| anyhow::anyhow!("{}", e))?
    {
        let tokens = abi_function
            .decode_input(body, false)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        input = Some(tokens);
    }

    match input {
        Some(a) => Ok(a),
        None if !abi_function.has_input() => Ok(Default::default()),
        _ => Err(AbiError::NoMessagesProduced.into()),
    }
}

fn parse_transaction_messages(
    transaction: &ton_block::Transaction,
) -> Result<TransactionMessages, anyhow::Error> {
    let mut out_messages = Vec::new();
    transaction
        .out_msgs
        .iterate_slices(|slice| {
            if let Ok(message) = slice
                .reference(0)
                .and_then(ton_block::Message::construct_from_cell)
            {
                out_messages.push(message);
            }
            Ok(true)
        })
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let in_message = transaction
        .read_in_msg()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(TransactionMessages {
        in_message,
        out_messages,
    })
}

pub struct TransactionMessages {
    pub in_message: Option<ton_block::Message>,
    pub out_messages: Vec<ton_block::Message>,
}

#[derive(thiserror::Error, Debug)]
enum AbiError {
    #[error("Invalid output message")]
    InvalidOutputMessage,
    #[error("No external output messages")]
    NoMessagesProduced,
}
