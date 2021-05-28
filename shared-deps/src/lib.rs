use anyhow::Error;

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
