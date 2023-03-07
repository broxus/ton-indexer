use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;

use crate::utils::FastDashMap;

pub struct OperationsPool<K, R> {
    name: &'static str,
    operations: FastDashMap<K, Arc<Operation<R>>>,
}

impl<K, R> OperationsPool<K, R>
where
    K: Eq + Hash + Clone + std::fmt::Display,
    R: Clone,
{
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            operations: FastDashMap::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub async fn do_or_wait<O>(
        &self,
        id: &K,
        timeout_ms: Option<u64>,
        operation: O,
    ) -> Result<Option<R>>
    where
        O: Future<Output = Result<R>>,
    {
        use dashmap::mapref::entry::Entry;

        let awaiter = match self.operations.entry(id.clone()) {
            Entry::Occupied(entry) => {
                let operation = entry.get().clone();
                let started = operation.stared.swap(true, Ordering::SeqCst);
                drop(entry);

                if started {
                    return self
                        .wait_operation(id, timeout_ms, &operation, || Ok(false))
                        .await;
                }

                operation
            }
            Entry::Vacant(entry) => entry.insert(Operation::new(true)).clone(),
        };

        // tracing::trace!("{}: started operation {}", self.name, id);
        let result = operation.await;
        // tracing::trace!("{}: done operation {}", self.name, id);

        self.operations.remove(id);

        let _ = awaiter.result_tx.send(Some(match &result {
            Ok(result) => Ok(result.clone()),
            Err(e) => Err(e.to_string()),
        }));

        Some(result).transpose()
    }

    pub async fn wait<F>(
        &self,
        id: &K,
        timeout_ms: Option<u64>,
        check_complete: F,
    ) -> Result<Option<R>>
    where
        F: Fn() -> Result<bool>,
    {
        let operation = self
            .operations
            .entry(id.clone())
            .or_insert_with(|| Operation::new(false))
            .clone();

        self.wait_operation(id, timeout_ms, &operation, check_complete)
            .await
    }

    async fn wait_operation<F>(
        &self,
        id: &K,
        timeout_ms: Option<u64>,
        operation: &Operation<R>,
        check_complete: F,
    ) -> Result<Option<R>>
    where
        F: Fn() -> Result<bool>,
    {
        let mut result_rx = operation.result_rx.clone();

        loop {
            // tracing::trace!("{}: waiting operation {}", self.name, id);

            let possible_result =
                tokio::time::timeout(Duration::from_millis(1), result_rx.changed()).await;

            let result = match (possible_result, timeout_ms) {
                (Ok(result), _) => result,
                (Err(_), _) if check_complete()? => return Ok(None),
                (Err(_), Some(timeout_ms)) => {
                    tokio::time::timeout(Duration::from_millis(timeout_ms), result_rx.changed())
                        .await
                        .map_err(|_| {
                            anyhow::Error::msg(format!("{}: operation timeout {}", self.name, id))
                        })?
                }
                _ => result_rx.changed().await,
            };

            if result.is_err() {
                return Ok(None);
            }

            let result = match result_rx.borrow().as_ref() {
                Some(Ok(result)) => Ok(Some(result.clone())),
                Some(Err(e)) => Err(anyhow::Error::msg(e.to_string())),
                None => continue,
            };

            // tracing::trace!("{}: waited for operation {}", self.name, id);
            break result;
        }
    }
}

struct Operation<R> {
    stared: AtomicBool,
    result_tx: watch::Sender<Option<std::result::Result<R, String>>>,
    result_rx: watch::Receiver<Option<std::result::Result<R, String>>>,
}

impl<R> Operation<R> {
    fn new(stared: bool) -> Arc<Self> {
        let (result_tx, result_rx) = watch::channel(None);
        Arc::new(Self {
            stared: AtomicBool::new(stared),
            result_tx,
            result_rx,
        })
    }
}
