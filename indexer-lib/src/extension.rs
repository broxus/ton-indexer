use anyhow::Result;
use ton_block::{MsgAddressInt, Serializable, Transaction};
use ton_types::UInt256;

use shared_deps::{NoFailure, TrustMe};

use crate::{address_from_account_id, ExtractInput, Extractable, TransactionMessages};

pub trait TransactionExt {
    fn time(&self) -> u32;
    fn contract_address(&self) -> Result<MsgAddressInt>;
    fn sender_address(&self) -> Result<Option<MsgAddressInt>>;
    fn messages(&self) -> Result<TransactionMessages>;
    fn tx_hash(&self) -> Result<UInt256>;
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
}
