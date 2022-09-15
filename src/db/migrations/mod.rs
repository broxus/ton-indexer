use std::collections::hash_map::{self, HashMap};
use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;

use super::columns;
use super::tree::Tree;

mod v2_0_7;
mod v2_0_8;

const CURRENT_VERSION: Semver = [2, 0, 8];

pub async fn apply(db: &Arc<rocksdb::DB>) -> Result<()> {
    const DB_VERSION_KEY: &str = "db_version";

    let mut migrations = Migrations::default();
    v2_0_7::register(&mut migrations).context("Failed to register v2.0.7")?;
    v2_0_8::register(&mut migrations).context("Failed to register v2.0.8")?;

    let state = Tree::<columns::NodeStates>::new(db)?;
    let is_empty = state
        .iterator(rocksdb::IteratorMode::Start)
        .next()
        .transpose()?
        .is_none();
    if is_empty {
        log::info!("Starting with empty db");
        state
            .insert(DB_VERSION_KEY, CURRENT_VERSION)
            .context("Failed to save new DB version")?;
        return Ok(());
    }

    loop {
        let version: [u8; 3] = state
            .get(DB_VERSION_KEY)?
            .map(|v| v.to_vec())
            .ok_or(MigrationsError::VersionNotFound)?
            .try_into()
            .map_err(|_| MigrationsError::InvalidDbVersion)?;

        match version.cmp(&CURRENT_VERSION) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                log::info!("Stored DB version is compatible");
                break Ok(());
            }
            std::cmp::Ordering::Greater => {
                break Err(MigrationsError::IncompatibleDbVersion).with_context(|| {
                    format!(
                        "Too new version found: {version:?}. Expected version: {CURRENT_VERSION:?}"
                    )
                })
            }
        }

        let migration = migrations
            .get(&version)
            .with_context(|| format!("No suitable migration found for version {version:?}"))?;
        log::info!("Applying migration for version: {version:?}");

        state
            .insert(DB_VERSION_KEY, (*migration)(db.clone()).await?)
            .context("Failed to save new DB version")?;
    }
}

#[derive(Default)]
struct Migrations(HashMap<Semver, Migration>);

impl Migrations {
    pub fn get(&self, version: &Semver) -> Option<&Migration> {
        self.0.get(version)
    }

    pub fn register<F, FR>(&mut self, from: Semver, to: Semver, migration: F) -> Result<()>
    where
        F: Fn(Arc<rocksdb::DB>) -> FR + 'static,
        FR: Future<Output = Result<()>> + Send + 'static,
    {
        match self.0.entry(from) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(move |db| {
                    let fut = migration(db);
                    async move {
                        fut.await?;
                        Ok(to)
                    }
                    .boxed()
                }));
                Ok(())
            }
            hash_map::Entry::Occupied(entry) => {
                Err(MigrationsError::DuplicateMigration(*entry.key()).into())
            }
        }
    }
}

type Semver = [u8; 3];
type Migration = Box<dyn Fn(Arc<rocksdb::DB>) -> BoxFuture<'static, Result<Semver>>>;

#[derive(thiserror::Error, Debug)]
enum MigrationsError {
    #[error("Incompatible DB version")]
    IncompatibleDbVersion,
    #[error("Existing DB version not found")]
    VersionNotFound,
    #[error("Invalid version")]
    InvalidDbVersion,
    #[error("Duplicate migration: {0:?}")]
    DuplicateMigration(Semver),
}
