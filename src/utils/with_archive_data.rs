use bytes::Bytes;

/// Parsed data wrapper, augmented with the optional raw data.
///
/// Stores the raw data only in the context of the archive parser, or received block.
///
/// NOTE: Can be safely closed, all raw bytes are shared (see [`Bytes`])
///
/// [`ArchiveData`]
#[derive(Clone)]
pub struct WithArchiveData<T> {
    pub data: T,
    pub archive_data: ArchiveData,
}

impl<T> WithArchiveData<T> {
    /// Constructs a new object from the context with known raw data
    pub fn new<A>(data: T, archive_data: A) -> Self
    where
        Bytes: From<A>,
    {
        Self {
            data,
            archive_data: ArchiveData::New(Bytes::from(archive_data)),
        }
    }

    /// Construct a new object from the context without known raw data
    pub fn loaded(data: T) -> Self {
        Self {
            data,
            archive_data: ArchiveData::Existing,
        }
    }

    /// Assumes that the object is constructed with known raw data
    pub fn new_archive_data(&self) -> Result<&[u8], WithArchiveDataError> {
        match &self.archive_data {
            ArchiveData::New(data) => Ok(data),
            ArchiveData::Existing => Err(WithArchiveDataError),
        }
    }
}

impl<T> std::ops::Deref for WithArchiveData<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Clone)]
pub enum ArchiveData {
    /// The raw data is known
    New(Bytes),
    /// Raw data is not known (due to nondeterministic serialization)
    Existing,
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
#[error("Archive data not loaded")]
pub struct WithArchiveDataError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn correct_context() {
        const DATA: &[u8] = &[1, 2, 3];

        assert_eq!(
            WithArchiveData::new((), DATA.to_vec())
                .new_archive_data()
                .unwrap(),
            DATA
        );
        assert!(WithArchiveData::loaded(()).new_archive_data().is_err());
    }
}
