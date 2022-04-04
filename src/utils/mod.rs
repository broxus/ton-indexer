use std::ops::Deref;

use bytes::Bytes;
use tiny_adnl::utils::*;

pub use block::*;
pub use block_proof::*;
pub use mapped_file::*;
pub use shard_state::*;
pub use shard_state_cache::*;
pub use stream_utils::*;
pub use trigger::*;

mod block;
mod block_proof;
mod mapped_file;
mod shard_state;
mod shard_state_cache;
mod stream_utils;
mod trigger;

pub type ActivePeers = FxDashSet<AdnlNodeIdShort>;

#[derive(Clone)]
pub struct WithArchiveData<T> {
    pub data: T,
    pub archive_data: ArchiveData,
}

impl<T> WithArchiveData<T> {
    pub fn new<A>(data: T, archive_data: A) -> Self
    where
        Bytes: From<A>,
    {
        Self {
            data,
            archive_data: ArchiveData::New(Bytes::from(archive_data)),
        }
    }

    pub fn loaded(data: T) -> Self {
        Self {
            data,
            archive_data: ArchiveData::Existing,
        }
    }

    pub fn new_archive_data(&self) -> Result<&[u8], WithArchiveDataError> {
        match &self.archive_data {
            ArchiveData::New(data) => Ok(data),
            ArchiveData::Existing => Err(WithArchiveDataError),
        }
    }
}

impl<T> Deref for WithArchiveData<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Clone)]
pub enum ArchiveData {
    New(Bytes),
    Existing,
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
#[error("Archive data not loaded")]
pub struct WithArchiveDataError;
