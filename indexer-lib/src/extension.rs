use anyhow::Result;
use ton_block::{CommonMsgInfo, MsgAddressInt, Serializable, Transaction, TransactionDescr};
use ton_types::UInt256;

use shared_deps::{NoFailure, TrustMe};

use crate::{address_from_account_id, ExtractInput, Extractable, TransactionMessages};

pub trait TransactionExt {
    fn time(&self) -> u32;
    fn contract_address(&self) -> Result<MsgAddressInt>;
    fn sender_address(&self) -> Result<Option<MsgAddressInt>>;
    fn messages(&self) -> Result<TransactionMessages>;
    fn tx_hash(&self) -> Result<UInt256>;
    fn success(&self) -> bool;
    /// Err if no in messages
    fn bounced(&self) -> Result<bool>;
}

impl TransactionExt for Transaction {
    fn time(&self) -> u32 {
        (&self).time()
    }

    fn contract_address(&self) -> Result<MsgAddressInt> {
        (&self).contract_address()
    }

    fn sender_address(&self) -> Result<Option<MsgAddressInt>> {
        (&self).sender_address()
    }

    fn messages(&self) -> Result<TransactionMessages> {
        (&self).messages()
    }

    fn tx_hash(&self) -> Result<UInt256> {
        (&self).tx_hash()
    }

    fn success(&self) -> bool {
        (&self).success()
    }

    fn bounced(&self) -> Result<bool> {
        (&self).bounced()
    }
}

impl TransactionExt for &Transaction {
    fn time(&self) -> u32 {
        self.now
    }

    fn contract_address(&self) -> Result<MsgAddressInt> {
        //todo check correctness
        let wc = self
            .messages()?
            .in_message
            .trust_me()
            .msg
            .dst()
            .trust_me()
            .workchain_id();
        address_from_account_id(self.account_addr.clone(), wc as i8)
    }

    fn sender_address(&self) -> Result<Option<MsgAddressInt>> {
        let addr = self
            .in_msg
            .as_ref()
            .ok_or(TransactionExtError::TickTok)?
            .read_struct()
            .convert()?
            .src();
        Ok(addr)
    }

    fn messages(&self) -> Result<TransactionMessages> {
        Ok(crate::parse_transaction_messages(&self)?)
    }

    fn tx_hash(&self) -> Result<UInt256> {
        Ok(self.serialize().convert()?.hash(0))
    }
    fn success(&self) -> bool {
        let res = self.description.read_struct().map(|x| match x {
            TransactionDescr::Ordinary(a) => !a.aborted,
            _ => false,
        });
        match res {
            Ok(a) => a,
            Err(_) => false,
        }
    }

    fn bounced(&self) -> Result<bool> {
        let in_msg = self
            .messages()?
            .in_message
            .ok_or_else(|| anyhow::anyhow!("No im messages"))?;
        let bounce = match in_msg.msg.header() {
            CommonMsgInfo::IntMsgInfo(a) => a.bounce,
            _ => anyhow::bail!("Not an internal message"),
        };
        match self.success() {
            false => Ok(bounce),
            true => Ok(false),
        }
    }
}
#[derive(thiserror::Error, Debug, Clone)]
enum TransactionExtError {
    #[error("Tick tocks are not our target")]
    TickTok,
}

impl<T> TransactionExt for ExtractInput<'_, T>
where
    T: Extractable,
{
    fn time(&self) -> u32 {
        self.transaction.time()
    }

    fn contract_address(&self) -> Result<MsgAddressInt> {
        self.transaction.contract_address()
    }

    fn sender_address(&self) -> Result<Option<MsgAddressInt>> {
        self.transaction.sender_address()
    }

    fn messages(&self) -> Result<TransactionMessages> {
        self.transaction.messages()
    }

    fn tx_hash(&self) -> Result<UInt256> {
        Ok(self.hash)
    }

    fn success(&self) -> bool {
        self.transaction.success()
    }

    fn bounced(&self) -> Result<bool> {
        self.transaction.bounced()
    }
}

impl<T> TransactionExt for &ExtractInput<'_, T>
where
    T: Extractable,
{
    fn time(&self) -> u32 {
        self.transaction.time()
    }

    fn contract_address(&self) -> Result<MsgAddressInt> {
        self.transaction.contract_address()
    }

    fn sender_address(&self) -> Result<Option<MsgAddressInt>> {
        self.transaction.sender_address()
    }

    fn messages(&self) -> Result<TransactionMessages> {
        self.transaction.messages()
    }

    fn tx_hash(&self) -> Result<UInt256> {
        Ok(self.hash)
    }

    fn success(&self) -> bool {
        self.transaction.success()
    }

    fn bounced(&self) -> Result<bool> {
        self.transaction.bounced()
    }
}
