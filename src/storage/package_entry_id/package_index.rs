use std::cmp::Ordering;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::TrustMe;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::storage::{columns, Tree};
use crate::Engine;

pub struct PackageMetaStorage {
    pub(super) db: Tree<columns::PackageMeta>,
    engine: Arc<Engine>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ArchiveId {
    Key(u32),
    Regular(u32),
}

impl ArchiveId {
    pub fn as_bytes(&self) -> SmallVec<[u8; 8]> {
        let mut buf = SmallVec::with_capacity(8);
        bincode::serialize_into(&mut buf, &self).trust_me();
        buf
    }
}

impl From<ArchiveId> for u64 {
    fn from(id: ArchiveId) -> Self {
        u64::from_le_bytes(id.as_bytes().into_inner().trust_me())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Copy, Clone)]
pub struct PackageMetaEntry {
    pub deleted: bool,
    pub finalized: bool,
    pub num_blobs: u8,
    pub ty: PackageType,
}

impl PackageMetaEntry {
    pub const fn new(package_type: PackageType) -> Self {
        Self::with_data(false, false, 0, package_type)
    }

    pub const fn with_data(
        deleted: bool,
        finalized: bool,
        num_blobs: u8,
        package_type: PackageType,
    ) -> Self {
        Self {
            deleted,
            finalized,
            num_blobs,
            ty: package_type,
        }
    }

    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    pub const fn finalized(&self) -> bool {
        self.finalized
    }
    pub fn as_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self)?)
    }
}

impl PackageMetaStorage {
    pub fn with_db(db: Tree<columns::PackageMeta>, engine: Arc<Engine>) -> Result<Self> {
        Ok(Self { db, engine })
    }

    pub fn update(&self, id: ArchiveId, state: &PackageMetaEntry) -> Result<()> {
        let key = id.as_bytes();
        let value = bincode::serialize(state).trust_me();
        self.db.insert(key, value)
    }

    pub fn get_state(&self, id: ArchiveId) -> Result<Option<PackageMetaEntry>> {
        let data = self.db.get(id.as_bytes())?;
        Ok(match data {
            Some(a) => Some(bincode::deserialize(a.as_ref())?),
            None => None,
        })
    }

    pub fn iter(
        db: &Tree<columns::PackageMeta>,
    ) -> Result<impl Iterator<Item = (u32, PackageMetaEntry)> + '_> {
        let iter = db.iterator(rocksdb::IteratorMode::Start)?;
        Ok(
            iter.filter_map(|(k, v)| -> Option<(u32, PackageMetaEntry)> {
                let k: [u8; 4] = k.as_ref().try_into().ok()?;
                let k = u32::from_le_bytes(k);
                let v = bincode::deserialize(v.as_ref()).ok()?;
                Some((k, v))
            }),
        )
    }

    async fn get_package_id(&self, seq_no: ArchiveId) -> Result<PackageId> {
        match seq_no {
            ArchiveId::Key(seq_no) => {
                Ok(PackageId::for_key_block(seq_no / KEY_ARCHIVE_PACKAGE_SIZE))
            }
            ArchiveId::Regular(seq_no) => {
                let meta = self.get_state(ArchiveId::Regular(seq_no))?.ok_or_else(|| {
                    log::error!(target: "storage", "Package not found for seq_no: {}", seq_no);
                    anyhow::anyhow!("Package not found for seq_no: {}", seq_no)
                })?;

                Ok(PackageId {
                    id: seq_no,
                    package_type: meta.ty,
                })
            }
        }
    }

    pub fn calculate_package_id(
        &self,
        mc_seq_no: u32,
        key_archive: bool,
        key_block: bool,
    ) -> PackageId {
        if key_block {
            if key_archive {
                PackageId::for_key_block(mc_seq_no / KEY_ARCHIVE_PACKAGE_SIZE)
            } else {
                PackageId::for_block(mc_seq_no)
            }
        } else {
            PackageId::for_block(mc_seq_no - (mc_seq_no % ARCHIVE_SLICE_SIZE as u32))
        }
    }
    /// calculates archive id for `FullNodeOverlayService`
    pub async fn get_archive_id(&self, mc_seq_no: u32) -> Option<u64> {
        let archive_id = if is_key_block(&self.engine, mc_seq_no).await.ok()? {
            ArchiveId::Key(mc_seq_no)
        } else {
            ArchiveId::Regular(mc_seq_no)
        };
        self.get_state(archive_id).ok()??;
        Some(archive_id.into())
    }
}

async fn is_key_block(engine: &Engine, mc_seq_no: u32) -> Result<bool> {
    engine
        .find_block_by_seq_no(
            &ton_block::AccountIdPrefixFull::any_masterchain(),
            mc_seq_no,
        )
        .map(|x| x.meta().is_key_block())
}

pub const ARCHIVE_SIZE: u32 = 100_000;
pub const ARCHIVE_PACKAGE_SIZE: u32 = 100;

pub const KEY_ARCHIVE_SIZE: u32 = 10_000_000;
pub const KEY_ARCHIVE_PACKAGE_SIZE: u32 = 200_000;

pub const ARCHIVE_SLICE_SIZE: u32 = 20_000;

#[derive(
    Debug, Copy, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
pub enum PackageType {
    Blocks,
    KeyBlocks,
}

impl PackageType {
    fn blobs(&self) -> Option<u16> {
        match self {
            PackageType::Blocks => Some(100),
            PackageType::KeyBlocks => None,
        }
    }

    fn slice_len(&self) -> u32 {
        match self {
            PackageType::Blocks => ARCHIVE_PACKAGE_SIZE,
            PackageType::KeyBlocks => KEY_ARCHIVE_PACKAGE_SIZE,
        }
    }
}

#[derive(Debug, Clone, Eq, serde::Serialize, serde::Deserialize)]
pub struct PackageId {
    id: u32,
    package_type: PackageType,
}

impl PackageId {
    pub const fn with_values(id: u32, package_type: PackageType) -> Self {
        Self { id, package_type }
    }

    pub const fn for_block(mc_seq_no: u32) -> Self {
        Self::with_values(mc_seq_no, PackageType::Blocks)
    }

    pub const fn for_key_block(mc_seq_no: u32) -> Self {
        Self::with_values(mc_seq_no, PackageType::KeyBlocks)
    }

    pub const fn id(&self) -> u32 {
        self.id
    }

    pub const fn package_type(&self) -> PackageType {
        self.package_type
    }
}

impl PartialEq for PackageId {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl PartialOrd for PackageId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id().partial_cmp(&other.id())
    }
}

impl Ord for PackageId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for PackageId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
