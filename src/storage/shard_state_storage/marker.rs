#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct Marker(pub u8);

impl Marker {
    const MIN: u8 = 1;
    const MAX: u8 = 127;

    pub const PERSISTENT: Self = Self(0);

    pub fn from_temp(marker: u8) -> Option<Self> {
        let result = Self(marker);
        result.is_temp().then_some(result)
    }

    /// Transition:
    /// * tempN -> Some(tempN+1),
    /// * to_persistent -> None
    pub fn next(self) -> Option<Self> {
        match self.0 {
            Self::MAX => Some(Self(Self::MIN)),
            marker @ Self::MIN..=Self::MAX => Some(Self(marker + 1)),
            _ => None,
        }
    }

    pub fn is_temp(&self) -> bool {
        self.0 >= Self::MIN && self.0 <= Self::MAX
    }

    #[inline]
    pub fn is_persistent(&self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Debug for Marker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            Self::PERSISTENT => "Persistent",
            marker if marker.is_temp() => return f.write_fmt(format_args!("Marker({})", marker.0)),
            _ => "Unknown",
        })
    }
}

impl Default for Marker {
    #[inline]
    fn default() -> Self {
        Self(Self::MIN)
    }
}

impl From<Marker> for u8 {
    #[inline]
    fn from(value: Marker) -> Self {
        value.0
    }
}

impl PartialEq<u8> for Marker {
    #[inline]
    fn eq(&self, other: &u8) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Marker> for u8 {
    #[inline]
    fn eq(&self, other: &Marker) -> bool {
        *self == other.0
    }
}
