use crate::anyhow::Error;
pub use anyhow;
pub use async_trait;
pub use config;
pub use serde;
pub use serde_yaml;
pub use tokio;
pub use ton_abi;
pub use ton_api;
pub use ton_block;
pub use ton_executor;
pub use ton_types;
pub use ton_vm;

pub trait NoFailure {
    type Output;
    fn convert(self) -> Result<Self::Output, Error>;
}

impl<T> NoFailure for Result<T, failure::Error> {
    type Output = T;
    fn convert(self) -> Result<Self::Output, Error> {
        self.map_err(|e| Error::msg(e.to_string()))
    }
}

pub trait TrustMe<T>: Sized {
    #[track_caller]
    fn trust_me(self) -> T;
}

impl<T, E> TrustMe<T> for Result<T, E>
where
    E: std::fmt::Debug,
{
    #[track_caller]
    fn trust_me(self) -> T {
        self.expect("Shouldn't fail")
    }
}

impl<T> TrustMe<T> for Option<T> {
    #[track_caller]
    fn trust_me(self) -> T {
        self.expect("Shouldn't fail")
    }
}
