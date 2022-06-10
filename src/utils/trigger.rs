use tokio_util::sync::CancellationToken;

/// Creates [`CancellationToken`] paired with [`TriggerOnDrop`].
///
/// Can be used to notify that the task is either finished normally or the scope is dropped.
pub fn trigger_on_drop() -> (TriggerOnDrop, CancellationToken) {
    let cancellation_token = CancellationToken::new();
    (
        TriggerOnDrop(cancellation_token.clone()),
        cancellation_token,
    )
}

/// Triggers [`CancellationToken`] when dropped
pub struct TriggerOnDrop(CancellationToken);

impl Drop for TriggerOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancels_on_drop() {
        let (trigger, cancellation_token) = trigger_on_drop();
        drop(trigger);
        assert!(cancellation_token.is_cancelled());
    }
}
