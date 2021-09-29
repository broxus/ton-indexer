use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait WhileSomeExt: Sized {
    fn while_some(self) -> WhileSome<Self>;
}

impl<S, T> WhileSomeExt for S
where
    S: Stream<Item = Option<T>>,
{
    fn while_some(self) -> WhileSome<Self> {
        WhileSome { stream: self }
    }
}

pin_project_lite::pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct WhileSome<S> {
        #[pin]
        stream: S,
    }
}

impl<S, T> Stream for WhileSome<S>
where
    S: Stream<Item = Option<T>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Some(item))) => Poll::Ready(Some(item)),
            Poll::Ready(_) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
