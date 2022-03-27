use tokio_util::sync::CancellationToken;

pub fn trigger_on_drop() -> (TriggerOnDrop, CancellationToken) {
    let cancellation_token = CancellationToken::new();
    (
        TriggerOnDrop(cancellation_token.clone()),
        cancellation_token,
    )
}

pub struct TriggerOnDrop(CancellationToken);

impl Drop for TriggerOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
