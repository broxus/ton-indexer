use std::fmt::Debug;

use anyhow::{Context, Result};
use ton_abi::Event;
use ton_block::{
    AccountBlock, CurrencyCollection, Deserializable, GetRepresentationHash, MsgAddressInt,
    Transaction,
};
use ton_types::{SliceData, UInt256};

use shared_deps::NoFailure;

pub use crate::extension::TransactionExt;

mod extension;

#[derive(Debug, Clone)]
pub struct ParsedOutput<T: Clone + Debug> {
    pub transaction: Transaction,
    pub hash: UInt256,
    pub output: Vec<T>,
}

pub struct ExtractInput<'a, W> {
    pub transaction: &'a Transaction,
    pub hash: UInt256,
    pub what_to_extract: &'a [W],
}

impl<W> ExtractInput<'_, W>
where
    W: Extractable,
    W: ShouldParseFurther,
{
    pub fn process(&self) -> Result<Option<ParsedOutput<<W as Extractable>::Output>>> {
        let messages = self
            .messages()
            .context("Failed getting messages from transaction")?;
        let mut output = Vec::new();

        for parser in self.what_to_extract {
            let mut res = match parser.extract(&messages) {
                Ok(Some(a)) => a,
                Ok(None) => continue,
                Err(e) => {
                    log::error!("Failed parsing messages: {}", e);
                    continue;
                }
            };

            output.append(&mut res);
            // if !<W as ShouldParseFurther>::should_continue() {
            //     break;
            // }
        }
        Ok((!output.is_empty()).then(|| ParsedOutput {
            transaction: self.transaction.clone(),
            hash: self.hash,
            output,
        }))
    }
}

pub trait ShouldParseFurther {
    /// Returns true, if parsed transaction can store only one element.
    /// E.G. 1 function call per transaction, but n events.
    fn should_continue() -> bool;
}

impl ShouldParseFurther for ton_abi::Function {
    fn should_continue() -> bool {
        false
    }
}

impl ShouldParseFurther for ton_abi::Event {
    fn should_continue() -> bool {
        true
    }
}

pub trait Extractable {
    type Output: Clone + Debug;
    fn extract(&self, messages: &TransactionMessages) -> Result<Option<Vec<Self::Output>>>;
}

impl Extractable for Event {
    type Output = ParsedEvent;

    fn extract(&self, messages: &TransactionMessages) -> Result<Option<Vec<Self::Output>>> {
        fn hash(message: &MessageData) -> [u8; 32] {
            message
                .msg
                .hash()
                .map(|x| *x.as_slice())
                .expect("If message is parsed, than hash is ok")
        }
        let mut result = vec![];
        for message in &messages.out_messages {
            let tokens = match process_event_message(&message, &self) {
                Ok(Some(a)) => a,
                Ok(None) => continue,
                Err(e) => {
                    log::error!("Failed processing event messages: {:?}", e);
                    continue;
                }
            };
            result.push(ParsedEvent {
                function_name: self.name.clone(),
                input: tokens.tokens,
                message_hash: hash(message),
            });
        }
        Ok((!result.is_empty()).then(|| result))
    }
}

impl Extractable for ton_abi::Function {
    type Output = ParsedFunction;

    fn extract(&self, messages: &TransactionMessages) -> Result<Option<Vec<Self::Output>>> {
        let input = if self.has_input() {
            let message = match &messages.in_message {
                None => return Ok(None),
                Some(a) => a,
            };
            process_function_in_message(&message, &self)
                .context("Failed processing function in message")?
        } else {
            None
        };

        let output = if self.has_output() {
            process_function_out_messages(&messages.out_messages, &self)
                .context("Failed processing function out messages")?
        } else {
            None
        };
        #[allow(clippy::single_match)]
        match (&input, &output) {
            (None, None) => return Ok(None),
            _ => (),
        }
        Ok(Some(vec![ParsedFunction {
            function_name: self.name.clone(),
            input,
            output,
        }]))
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
) -> Result<Vec<ParsedOutput<O>>>
where
    W: Extractable + Extractable<Output = O>,
    W: ShouldParseFurther,
    O: Clone + Debug,
{
    use ton_types::HashmapType;
    let mut result = vec![];

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
        _ => return Ok(result), // no account blocks found
    };

    for account_block in account_blocks {
        for item in account_block.transactions().iter() {
            let (_, data) = item
                .convert()
                .context("Failed getting tx data from account block:")?;

            let cell = data
                .into_cell()
                .reference(0)
                .convert()
                .context("Failed packing tx data into cell")?;
            let hash = cell.hash(0);
            let transaction = match ton_block::Transaction::construct_from_cell(cell) {
                Ok(transaction) => transaction,
                Err(e) => {
                    log::error!("Failed creating transaction from cell: {}", e);
                    continue;
                }
            };
            let input = ExtractInput {
                transaction: &transaction,
                hash,
                what_to_extract,
            };
            let extracted_values = match input.process() {
                Ok(Some(a)) => a,
                Ok(None) => continue,
                Err(e) => {
                    log::error!("Failed parsing transaction: {}", e);
                    continue;
                }
            };
            result.push(extracted_values);
        }
    }
    Ok(result)
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
        let body = match msg.body() {
            None => continue,
            Some(a) => a,
        };

        let is_my_message = abi_function
            .is_my_output_message(body.clone(), is_internal)
            .unwrap_or(false);

        if is_my_message {
            let tokens = abi_function
                .decode_output(body, is_internal)
                .map_err(|e| AbiError::DecodingError(e.to_string()))?;

            output = Some(tokens);
            break;
        }
    }
    Ok(output)
}

fn process_function_in_message(
    msg: &MessageData,
    abi_function: &ton_abi::Function,
) -> Result<Option<Vec<ton_abi::Token>>, AbiError> {
    let mut input = None;
    let MessageData { msg, .. } = msg;
    let is_internal = msg.is_internal();
    let body = match msg.body() {
        None => return Ok(None),
        Some(a) => a,
    };

    let is_my_message = abi_function
        .is_my_input_message(body.clone(), is_internal)
        .unwrap_or(false);

    if is_my_message {
        let tokens = abi_function
            .decode_input(body, is_internal)
            .map_err(|e| AbiError::DecodingError(e.to_string()))?;
        input = Some(tokens);
    }
    Ok(input)
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
    let body = match msg.body() {
        None => return Ok(None),
        Some(a) => a,
    };

    let is_internal = msg.is_internal();
    let is_my_message = abi_function
        .is_my_message(body.clone(), is_internal)
        .unwrap_or(false);

    if is_my_message {
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
        _ => Ok(None),
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
    use ton_block::{Block, Deserializable, Transaction};

    use crate::{extract_from_block, ExtractInput, TransactionExt};

    const ABI: &str = r#"{
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
    const BLOCK_BOC:&str="te6ccuECawEAD3MAABwAxADeAXACBAKgAzwDYgN0BGIFUAWkBfAGRAaQBqgHhAegB7oH1AfuCAYIHgg2CEwIYgh4CI4IpAi6CNAI5gmGCgAKTApkCnwLWAvIC+QMMQxKDGQMsQz9DRYNYw16DccN3g4rDkIOjw6kDvEPBg8cD2kPfg/LEBcQLBB5EI4Q2xDwET0RUhHyEj8SuBMFE+AUKhQ8FJgU5hTyFUAVjBWcFa8WYBZyFoIXCBfLF9QYWhh2GMMYzBm0Gh4aIhoqGjIarRtOG8IcgxyKHRAdLh3lHoYe5gQQEe9VqgAAACoBAgMEAqCbx6mHAAAAAIQBAMlBywAAAAAEAAAAABAAAAAAAAAAYLd/NgAADWxKOtpAAAANbEo62kV2Mq9UAAIMeQCJ0lUAiaRbxAAAAAUAAAAAAAAALgUGAhG45I37QE1o5SQHCAqKBCZ0qoSj77CCdylTgu+LUezjXm2NUPkIc193rYdFJ+UYxt0znYRSDWrN3antnlndV8BsDqq8zj259jEVGju+nzMASgBKCQoDiUoz9v3ersryMf3xYcYqE6buCRGqyiwLFPkEdrO1l7KzRKYEN1fy1xm+K3+7ENvuWerGbV28lgNIxXNbGAa4wvy6509IQAsMDQCYAAANbEocVcQAidJVHs2FzCOoW7aCw/+HiVegyyz6mrQZQUHN44RcISEiJ2Y0oAPSVpdXwIr/sHQc8YN3QaLiQy0vk6ERTW/e4R941wCYAAANbEormAEAyUHKZSqZUNWRo4eBxaX1Yx6OywMQBH3D7q8FQ3Hed/O1IKzbgiCGZTMKIBRJE9pJy/gEIuefm7XnyoBkixI8QVYJ+wAhcSQZ35vP1YOJIM78UA3TAAgADQAQDuaygAgzWyZ0qoSj77CCdylTgu+LUezjXm2NUPkIc193rYdFJ+UY8nQSVhqHXeV0mV32X0z+ck/xoFsRKC6d09fWf5k+5YoASgATkCOv4gAAACoEAAAAABAAAAAAAAAAAMlBygAAAABgt38zAAANbEormAEAidJVIA4PEDNbxt0znYRSDWrN3antnlndV8BsDqq8zj259jEVGju+nzOIptAe4oDdpU0nZOALzugyzVt7arHy1EAS92KB0p7ovgBKABSQI6/iAAAAKgQAAAAAEAAAAAAAAAAAyUHLAAAAAGC3fzYAAA1sSjraRQCJ0lUgIyQlEQm0qImeM4psH4IIEsP0NU42/6HbEzwQ0NrPqjg5KijNmAAImFFhgCBKEQFcK8YDJkIsL1kuRaVPopTY7UYOutUqsX11ws3J3AfblQAIgk0RCTepkvNvmtAc2aMdw38G9JD0ZedpsP8JxJB+R6Tm7WL3AAigCJWsElIoSAEBvd2YdWhC6MZyqZiWJtg0jCvsHx7ElbZw4wz7MFlYqL0AASERgcSQZ35vP1YQEQDXAAAAAAAAAAD//////////3EkGd+bz9WDgXk+RiEIVZAAANbEocVcQAidJVHs2FzCOoW7aCw/+HiVegyyz6mrQZQUHN44RcISEiJ2Y0oAPSVpdXwIr/sHQc8YN3QaLiQy0vk6ERTW/e4R9414IhN4QOJIM783n6sIKBIiEQDhPDphO4L+iBMrIhEA4JfLNbONRegsFCIRAOA9GGmNvGlILhUiDwDHqSXAyG6IMBYiDwDFm157SDdIMhciDwDAwxJxLhgINBgiDQCpPGVdbig2GSINAKMAh6qgaBo5Ig0AouoeJihoGzsiDQCgd+iH2mg8HCINAKB36IfaSD4dIg0AoHfoh9pIQB4iDQCgd0nSTWhCHyINUCgdz2NKAiBFIZq7EOE6/hejE7Val9hblkv19D0TQHjQSCUFbM4+14TUCgdyZEQKgms+sbkhqsZBq70Plht9tWHaSX/eidgsRTWZkGb4B/gAAA1kWOCFwSEiccABvz5DhOv4XoxO1WpfYW5ZL9fQ9E0B40EglBWzOPteE1KMoPqDBbI0GAAANZFjghcJQO5MiIFTQEciKEgBAYWnjHPY3VbZ9pOPMGVn60EPGwgOuva3XrRX8T9w7RmKAAEBEQAAAAAAAAAAUCYhEYHEkGd+KAbpkCcA1wAAAAAAAAAA//////////9xJBnfigG6Y4F5PkiMT36QAADWxKHFXEAInSVR7NhcwjqFu2gsP/h4lXoMss+pq0GUFBzeOEXCEhIidmNKAD0laXV8CK/7B0HPGDd0Gi4kMtL5OhEU1v3uEfeNeABrsAwAAAAAAAAAAETpKoAABrYlHW0hG2G41CBqNoRgridJQ8SQiIuP9kzHdU7/1xMCUPOksDPAIhN4QOJIM78UA3TIKCkoSAEBnwkoPMItU+euJ45u7K+eHq0W0BE8qKzta92bjXapliQARCIRAOE8OmEX5shIKisiEQDgl8s1j/EPqCwtKEgBASlj6ht7a1U75eXKsbWEF0MwQHxufI+Yt3Le9ArXpNKmAEYoSAEBSZn8JArzYGO/U4QisIgC78n57r7DwEsT4SCjBA0iu6IAKSIRAOA9GGlqIDMILi8oSAEBUPMyOw1eL6d92ZPLbsukrrU+uziyCxbldNRotKo6DqEAOyIPAMepJZ0sOEgwMShIAQFR5OMLkUdhvOOwG49DD4dt/m2JnFsIfL+tDqgHOBJDiQAmIg8AxZteV6wBCDIzKEgBAV7fnd/cvyBmrnRTrQpS6SN+oHEPcCFdhEJ063zOeeooACQiDwDAwxJNkeHINDUoSAEBgYpCJShaHZhJfpNTc79iK1aWb/UeMozZTub0ObqvVDkAISINAKk8QcE36DY3KEgBAUrob4+xL0ej/Rftc0ezZtTzlq45Br1Pmr9kqtA8XHhlABUiDQCjAGQOaig4OSINAKLp+onyKDo7KEgBAdCTfu0c1HAUPplKnHP6/ylbRrRCny84fHYa6vW7A0mmABMiDQCgd8TrpCg8PShIAQGd1tyyQjCH4wlyp3NF2hZISxhm4DqHSmHnfjfcXoXOBQATKEgBAed9xSetNFiMjAoR2f9a7KTpAZjObcOezvRE6hoyL9CUAAkiDQCgd8TrpAg+PyhIAQGXgE8BR5eNnJnZVIgo904ariWMMSto53gg6mMlKOkISgAGIg0AoHfE66QIQEEoSAEBPa1mUkDnPwlySPPu6l+BMB+p5lnxtLqDnozQyNf6JRsAHSINAKB3JjYXKEJDKEgBAXOrEvQNLOZssVVvCAddf24MYysCHj2JF7tENO/r2gEfAA4iDVAoHcZ8PHJERSGauxDhOv4XoxO1WpfYW5ZL9fQ9E0B40EglBWzOPteE1AoHcCqAprYvWy4wuRltuHju2bzS4QvtY5EreWzZUeY/dN/oW7lZAAANbEo62kRGKEgBAa8UhiZaBAsKGc6z8K/cd1fSbsUvKa/UO6FVnLWi4mtpAA4iccABvz5DhOv4XoxO1WpfYW5ZL9fQ9E0B40EglBWzOPteE1KMoPqDBbv5sAAANbEo62kVQO4FUBTTQEdIKEgBAYDWxHxKJVQ8mzl7cXFvP64eLF0kcXTFLiwZvYlkQrEFAAwB1XyzdNIdVgTtQHdx3h2kDlbg4/bYcw3X584V4l71f3F4AAABeczJG6S+WbppDqsCdqA7uO8O0gcrcHH7bDmG6/PnCvEver+4vAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAICwSQBFoA+WbppDqsCdqA7uO8O0gcrcHH7bDmG6/PnCvEver+4vABACCQwosMAQS0wCU7/bYbjUIGo2hGCuJ0lDxJCIi4/2TMd1Tv/XEwJQ86SwM5hRYYAZhRYYQFFkAkW/7Riz+THFyRGfD9ZPAVa3SN+uZ7l7PlZI2C4eQIEKzE0ABFpWAgNAQE5PA0S/tsNxqEDUbQjBXE6Sh4khERcf7JmO6p3/riYEoedJYGcCUVZQAkS/tt61trkhxYn1jlmWy4maCiWPwnlmqqfL7R10aWTVTDoAYVYCB2YUWGFRZAEMRgYDCiwwaAOnoAN+fIcJ1/C9GJ2q1L7C3LJfr6HomgPGgkEoK2Zx9rwmqAIlawRRvz5DhOv4XoxO1WpfYW5ZL9fQ9E0B40EglBWzOPteE1noAABrYlHW0hAEStYJU1RVAQtlAECaaJBWAQnRiCXwIGQAgnKuKd5OJwhMAi1NpN5CqjGKRhjGlw6rqMwA1O//kBDLomVcuWaNYAiXoBfTVIq1fQfkyaZGlxBhkIbwMlI67QFWA7dxvz5DhOv4XoxO1WpfYW5ZL9fQ9E0B40EglBWzOPteE1AAANbEo62kGCaz6xuSGqxkGrvQ+WG321YdpJf96J2CxFNZmQZvgH+AAADWRY4IXBYLd/NgAFSAIE00SFdYWQIB4FpbAIJyrineTicITAItTaTeQqoxikYYxpcOq6jMANTv/5AQy6KguaIL7Sd3FGhxh2nBB1xKCOwqJUmwAMp2i4/3e34YGwITDMBSCIYeKYPEQGJjAUWIADfnyHCdfwvRidqtS+wtyyX6+h6JoDxoJBKCtmcfa8JqDFwCAd1fYAHhnaXH3o7UDJ2vsaz7FTzjlAcSPBjaH4vuYrWFzvMg1/svcJtiGiPURmffX7xMjuhEOdA7EOU5hBrapVu8ZLzQhN8s3TSHVYE7UB3cd4dpA5W4OP22HMN1+fOFeJe9X9xeAAAAXnMyRukYLd/fhMdgs2BdAWOAA358hwnX8L0YnarUvsLcsl+voeiaA8aCQSgrZnH2vCagAAAAAAAAAAAAAAAHc1lABF4AAAEBIGgBASBhAHXgAN+fIcJ1/C9GJ2q1L7C3LJfr6HomgPGgkEoK2Zx9rwmoAAAa2JR1tIbBbv5sSY7BZoAAAAAAAAAAQACdRk9jE4gAAAAAAAAAAFgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIABvyY9CQExRYUAAAAAAAAQAAAAAAAQAoaiumwh8gRH5zGLpmC7KwR9rzGPE0yxLwUCH40HulkCQJJQDtXG/PkOE6/hejE7Val9hblkv19D0TQHjQSCUFbM4+14TUAAA1sSjraRCgqNz2QuWmkAXOzbM9E71Om/BwKGiaHy98AeHmlj0+UAAANbEo62kFgt382AAFGIJfAhlZmcBAaBoAIJyoLmiC+0ndxRocYdpwQdcSgjsKiVJsADKdouP93t+GBtlXLlmjWAIl6AX01SKtX0H5MmmRpcQYZCG8DJSOu0BVgIVDAkO5rKAGGIJfBFpagCxSAA358hwnX8L0YnarUvsLcsl+voeiaA8aCQSgrZnH2vCawAG/PkOE6/hejE7Val9hblkv19D0TQHjQSCUFbM4+14TVDuaygABhRYYAAAGtiUdbSEwW7+bEAAnkCFjD0JAAAAAAAAAAAAFwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAW8AAAAAAAAAAAAAAAAEtRS2kSeULjPfdJ4YfFGEir+G1RruLcPyCFvDGFBOfjgT+vb0Y";

    #[test]
    fn parse() {
        let fns = [ton_abi::Contract::load(std::io::Cursor::new(ABI))
            .unwrap()
            .function("submitTransaction")
            .unwrap()
            .clone()];

        let block = Block::construct_from_base64(BLOCK_BOC).unwrap();
        let res = extract_from_block(&block, &fns).unwrap();
        assert_eq!(res.is_empty(), false);
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

    const TOKEN_WALLET: &str = r#"{
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
			"name": "getVersion",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"uint32"}
			]
		},
		{
			"name": "balance",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"uint128"}
			]
		},
		{
			"name": "getDetails",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"components":[{"name":"root_address","type":"address"},{"name":"wallet_public_key","type":"uint256"},{"name":"owner_address","type":"address"},{"name":"balance","type":"uint128"},{"name":"receive_callback","type":"address"},{"name":"bounced_callback","type":"address"},{"name":"allow_non_notifiable","type":"bool"}],"name":"value0","type":"tuple"}
			]
		},
		{
			"name": "getWalletCode",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"name":"value0","type":"cell"}
			]
		},
		{
			"name": "accept",
			"inputs": [
				{"name":"tokens","type":"uint128"}
			],
			"outputs": [
			]
		},
		{
			"name": "allowance",
			"inputs": [
				{"name":"_answer_id","type":"uint32"}
			],
			"outputs": [
				{"components":[{"name":"remaining_tokens","type":"uint128"},{"name":"spender","type":"address"}],"name":"value0","type":"tuple"}
			]
		},
		{
			"name": "approve",
			"inputs": [
				{"name":"spender","type":"address"},
				{"name":"remaining_tokens","type":"uint128"},
				{"name":"tokens","type":"uint128"}
			],
			"outputs": [
			]
		},
		{
			"name": "disapprove",
			"inputs": [
			],
			"outputs": [
			]
		},
		{
			"name": "transferToRecipient",
			"inputs": [
				{"name":"recipient_public_key","type":"uint256"},
				{"name":"recipient_address","type":"address"},
				{"name":"tokens","type":"uint128"},
				{"name":"deploy_grams","type":"uint128"},
				{"name":"transfer_grams","type":"uint128"},
				{"name":"send_gas_to","type":"address"},
				{"name":"notify_receiver","type":"bool"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "transfer",
			"inputs": [
				{"name":"to","type":"address"},
				{"name":"tokens","type":"uint128"},
				{"name":"grams","type":"uint128"},
				{"name":"send_gas_to","type":"address"},
				{"name":"notify_receiver","type":"bool"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "transferFrom",
			"inputs": [
				{"name":"from","type":"address"},
				{"name":"to","type":"address"},
				{"name":"tokens","type":"uint128"},
				{"name":"grams","type":"uint128"},
				{"name":"send_gas_to","type":"address"},
				{"name":"notify_receiver","type":"bool"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "internalTransfer",
			"inputs": [
				{"name":"tokens","type":"uint128"},
				{"name":"sender_public_key","type":"uint256"},
				{"name":"sender_address","type":"address"},
				{"name":"send_gas_to","type":"address"},
				{"name":"notify_receiver","type":"bool"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "internalTransferFrom",
			"inputs": [
				{"name":"to","type":"address"},
				{"name":"tokens","type":"uint128"},
				{"name":"send_gas_to","type":"address"},
				{"name":"notify_receiver","type":"bool"},
				{"name":"payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "burnByOwner",
			"inputs": [
				{"name":"tokens","type":"uint128"},
				{"name":"grams","type":"uint128"},
				{"name":"send_gas_to","type":"address"},
				{"name":"callback_address","type":"address"},
				{"name":"callback_payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "burnByRoot",
			"inputs": [
				{"name":"tokens","type":"uint128"},
				{"name":"send_gas_to","type":"address"},
				{"name":"callback_address","type":"address"},
				{"name":"callback_payload","type":"cell"}
			],
			"outputs": [
			]
		},
		{
			"name": "setReceiveCallback",
			"inputs": [
				{"name":"receive_callback_","type":"address"},
				{"name":"allow_non_notifiable_","type":"bool"}
			],
			"outputs": [
			]
		},
		{
			"name": "setBouncedCallback",
			"inputs": [
				{"name":"bounced_callback_","type":"address"}
			],
			"outputs": [
			]
		},
		{
			"name": "destroy",
			"inputs": [
				{"name":"gas_dest","type":"address"}
			],
			"outputs": [
			]
		}
	],
	"data": [
		{"key":1,"name":"root_address","type":"address"},
		{"key":2,"name":"code","type":"cell"},
		{"key":3,"name":"wallet_public_key","type":"uint256"},
		{"key":4,"name":"owner_address","type":"address"}
	],
	"events": [
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
        let tx = Transaction::construct_from_base64("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").unwrap();
        let out = ExtractInput {
            transaction: &tx,
            hash: tx.tx_hash().unwrap(),
            what_to_extract: &evs,
        }
        .process()
        .unwrap()
        .unwrap();
        for name in out.output {
            assert_eq!("DepositLiquidity", name.function_name);
        }
    }

    #[test]
    fn send_tokens() {
        env_logger::init();
        let fun: Vec<_> = ton_abi::contract::Contract::load(std::io::Cursor::new(TOKEN_WALLET))
            .unwrap()
            .functions()
            .iter()
            .map(|x| x.1.clone())
            .collect();

        let first_in = "te6ccgECDAEAAsMAA7V/5tfdn4snTzD3mpEERfYIzpH/ZUdEtsUp/JmiMK9gG9AAAKHxPVJkHHnLsYvGOOa5uIxUfoZp0N+7vh4tguxyvr/gpS/1sCrwAACh8RRQWBYDYtrgADR8rtkIBQQBAhcEaMkHc1lAGHtQrREDAgBvyZBpLEwrwvQAAAAAAAQAAgAAAALLrUKeztBOuHLLx1RWl1Y0S5Jz+55Kyp2jXbR1+dd1zEDQM8QAnkb+LB6EgAAAAAAAAAABSQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgnI8VM5MHnxQYZZPcO3rSmw4aUs1NG2EI7Ip1d/zYwWB3PV/8tK3PYU3SCLlQ6FjikeMS9eU3gtetXZuJ6wYRREvAgHgCgYBAd8HAbFoAfza+7PxZOnmHvNSIIi+wRnSP+yo6JbYpT+TNEYV7AN7AAHuDUXhWs1Sy11bGZj4BpfAOCMEC1zg//hNNgzw/eWmkHNIMnQGK8M2AAAUPieqTITAbFtcwAgB7RjSFwIAAAAAAAAAAAAAAAAAlw/gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAEcSwzASifTNKv0V8EKwo04+co0+rRLLuqxzv3leScPaQAjiWGYCUT6ZpV+ivghWFGnHzlGn1aJZd1WOd+8ryTh7RCQAAAbFoARxLDMBKJ9M0q/RXwQrCjTj5yjT6tEsu6rHO/eV5Jw9pAD+bX3Z+LJ08w95qRBEX2CM6R/2VHRLbFKfyZojCvYBvUHc1lAAGIavcAAAUPidOvwTAbFtKwAsAi3sBdBeAAPcGovCtZqllrq2MzHwDS+AcEYIFrnB//CabBnh+8tNAAAAAAAAAAAAAAAAAEuH8AAAAAAAAAAAAAAAAAAAAABA=";
        let second_in = "te6ccgECCwEAAnsAA7d+o/i0drPNLp1Aqpqvp7mPj9ZBwuent2axPfCACL5jU9AAAOos5hsoNsnwWbHWWcN3vuAKJG7kkh0oyCea7U3eRRBj3RxxsW6wAADqLOYbKBYOdIKQADSAJw24CAUEAQIXBAkExiz0GIAmavYRAwIAb8mHoSBMFFhAAAAAAAAEAAIAAAACf8Vu1SbfckG3GDgjpIaVYS57+yQguN2E/l7uma99s55AUBYMAJ5J1cwTjggAAAAAAAAAATEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIJyniWiC5OTUu3Taq+jrROnx1X2atnLFC55gWwUdNv1g4yXTRZaUGOAsZMdZkICn4dvIuFAKLLBpSE2IqxVcP3gbwIB4AgGAQHfBwCxaAHUfxaO1nml06gVU1X09zHx+sg4XPT27NYnvhABF8xqewAjiWGYCUT6ZpV+ivghWFGnHzlGn1aJZd1WOd+8ryTh7RBHV/v4BhRYYAAAHUWcw2UIwc6QUkABsWgANX0wLMj6oT6zQ9W4oAyYcD7Cxnoi0AXAZhQxeAyakXcAOo/i0drPNLp1Aqpqvp7mPj9ZBwuent2axPfCACL5jU9QTGLPQAYrwzYAAB1FnCrOhsHOkDTACQHtGNIXAgAAAAAAAAAAAAAAAAf4U8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIARxLDMBKJ9M0q/RXwQrCjTj5yjT6tEsu6rHO/eV5Jw9pACOJYZgJRPpmlX6K+CFYUacfOUafVoll3VY537yvJOHtEKAAA=";
        let first_out = "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";
        let second_out = "te6ccgECBwEAAZsAA7Vxq+mBZkfVCfWaHq3FAGTDgfYWM9EWgC4DMKGLwGTUi7AAAOos6eu4FSmzDnoaDRnjP8Ac/rnkJgqA6BSV+Q8j/9dHQUKWv0JQAADqLOFWdBYOdIMgABRpucMIBQQBAhcMSAkBa6yWGGm5vxEDAgBbwAAAAAAAAAAAAAAAAS1FLaRJ5QuM990nhh8UYSKv4bVGu4tw/IIW8MYUE5+OBACeQn1sBdGcAAAAAAAAAAB/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCco4WsVzUu58EKc/wM6HiEQtweqf+WzkudRJx5E203Wm5EsY1HZ4XMaQeR0fLT2T0mII6avap960GwJbDnWDZ/cQBAaAGAMFYAdR/Fo7WeaXTqBVTVfT3MfH6yDhc9Pbs1ie+EAEXzGp7AAavpgWZH1Qn1mh6txQBkw4H2FjPRFoAuAzChi8Bk1Iu0Ba6yWAGFFhgAAAdRZzDZQTBzpBSf////7Rar5/A";

        let txs: Vec<_> = [first_in, second_in, first_out, second_out]
            .iter()
            .map(|x| Transaction::construct_from_base64(x).unwrap())
            .collect();
        for tx in &txs {
            let out = ExtractInput {
                transaction: tx,
                hash: tx.tx_hash().unwrap(),
                what_to_extract: &fun,
            }
            .process()
            .unwrap();

            if let Some(a) = out {
                let name = &a.output.first().unwrap().function_name;
                assert!(name == "internalTransfer" || name == "transferToRecipient");
            }
        }
    }
}
