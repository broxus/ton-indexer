use std::cmp::Ord;
use std::hash::Hash;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};
use std::time::Duration;

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use tokio::sync::watch;

struct OperationAwaiters<R> {
    pub is_started: AtomicBool,
    pub tx: watch::Sender<Option<std::result::Result<R, String>>>,
    pub rx: watch::Receiver<Option<std::result::Result<R, String>>>,
}

impl<R: Clone> OperationAwaiters<R> {
    fn new(is_started: bool) -> Arc<Self> {
        let (tx, rx) = watch::channel(None);
        Arc::new(Self {
            is_started: AtomicBool::new(is_started),
            tx,
            rx,
        })
    }
}

pub struct AwaitersPool<I, R> {
    ops_awaiters: DashMap<I, Arc<OperationAwaiters<R>>>,
    description: &'static str,
}

impl<I, R> AwaitersPool<I, R>
where
    I: Ord + Hash + Clone + std::fmt::Display,
    R: Clone,
{
    pub fn new(description: &'static str) -> Self {
        Self {
            ops_awaiters: DashMap::new(),
            description,
        }
    }

    pub async fn do_or_wait(
        &self,
        id: &I,
        wait_timeout_ms: Option<u64>,
        operation: impl futures::Future<Output = Result<R>>,
    ) -> Result<Option<R>> {
        use dashmap::mapref::entry::Entry;

        loop {
            if let Some(op_awaiters) = self.ops_awaiters.get(id) {
                return if !op_awaiters.value().is_started.swap(true, Ordering::SeqCst) {
                    Some(self.do_operation(id, operation, &op_awaiters.value()).await).transpose()
                } else {
                    self.wait_operation(id, wait_timeout_ms, &op_awaiters.value(), || Ok(false))
                        .await
                };
            } else {
                let new_awaiters = OperationAwaiters::new(true);

                if let Entry::Vacant(entry) = self.ops_awaiters.entry(id.clone()) {
                    entry.insert(new_awaiters.clone());

                    return Some(self.do_operation(id, operation, &new_awaiters).await).transpose();
                };
            }
        }
    }

    pub async fn wait(
        &self,
        id: &I,
        timeout_ms: Option<u64>,
        check_complete: impl Fn() -> Result<bool>,
    ) -> Result<Option<R>> {
        use dashmap::mapref::entry::Entry;

        loop {
            if let Some(op_awaiters) = self.ops_awaiters.get(id) {
                return self
                    .wait_operation(id, timeout_ms, &op_awaiters.value(), check_complete)
                    .await;
            } else {
                let new_awaiters = OperationAwaiters::new(false);

                if let Entry::Vacant(entry) = self.ops_awaiters.entry(id.clone()) {
                    entry.insert(new_awaiters.clone());
                    return self
                        .wait_operation(id, timeout_ms, &new_awaiters, check_complete)
                        .await;
                }
            }
        }
    }

    async fn wait_operation(
        &self,
        id: &I,
        timeout_ms: Option<u64>,
        op_awaiters: &OperationAwaiters<R>,
        check_complete: impl Fn() -> Result<bool>,
    ) -> Result<Option<R>> {
        let mut rx = op_awaiters.rx.clone();
        loop {
            log::trace!("{}: wait_operation: waiting... {}", self.description, id);

            let result = if let Ok(result) =
                tokio::time::timeout(Duration::from_millis(1), rx.changed()).await
            {
                result
            } else if check_complete()? {
                // Operation might be done before calling `wait_operation` - check it and return
                return Ok(None);
            } else if let Some(timeout_ms) = timeout_ms {
                tokio::time::timeout(Duration::from_millis(timeout_ms), rx.changed())
                    .await
                    .map_err(|_| anyhow!("{}: timeout {}", self.description, id))?
            } else {
                rx.changed().await
            };
            if result.is_err() {
                return Ok(None);
            }

            let r = match &*rx.borrow() {
                Some(Ok(r)) => Ok(Some(r.clone())),
                Some(Err(e)) => Err(anyhow!("{}", e)),
                None => continue,
            };
            log::trace!("{}: wait_operation: done {}", self.description, id);
            break r;
        }
    }

    async fn do_operation(
        &self,
        id: &I,
        operation: impl futures::Future<Output = Result<R>>,
        op_awaiters: &OperationAwaiters<R>,
    ) -> Result<R> {
        log::trace!("{}: do_operation: doing... {}", self.description, id);
        let result = operation.await;
        log::trace!("{}: do_operation: done {}", self.description, id);

        self.ops_awaiters.remove(id);

        let r = match result {
            Ok(ref r) => Ok(r.clone()),
            Err(ref e) => Err(format!("{}", e)), // failure::Error doesn't impl Clone,
                                                 // so it is impossible to clone full result
        };
        let _ = op_awaiters.tx.send(Some(r));
        result
    }
}
