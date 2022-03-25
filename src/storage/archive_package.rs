use std::io::Write;

pub fn make_archive_segment(filename: &str, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut vec = Vec::with_capacity(2 + 2 + 4 + filename.len() + data.len());
    vec.write_all(&ARCHIVE_ENTRY_PREFIX)?;
    vec.write_all(&(filename.len() as u16).to_le_bytes())?;
    vec.write_all(&(data.len() as u32).to_le_bytes())?;
    vec.write_all(filename.as_bytes())?;
    vec.write_all(data)?;
    Ok(vec)
}

pub struct ArchivePackageViewReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ArchivePackageViewReader<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self, ArchivePackageError> {
        let mut offset = 0;
        read_package_header(data, &mut offset)?;
        Ok(Self { data, offset })
    }

    pub fn read_next(
        &mut self,
    ) -> Result<Option<ArchivePackageEntryView<'a>>, ArchivePackageError> {
        ArchivePackageEntryView::read_from_view(self.data, &mut self.offset)
    }
}

pub fn read_package_header(buf: &[u8], offset: &mut usize) -> Result<(), ArchivePackageError> {
    let end = *offset;

    // NOTE: `end > end + 4` is needed here because it eliminates useless
    // bounds check with panic. It is not even included into result assembly
    if buf.len() < end + 4 || end > end + 4 {
        return Err(ArchivePackageError::UnexpectedArchiveEof);
    }

    if buf[end..end + 4] == ARCHIVE_PREFIX {
        *offset += 4;
        Ok(())
    } else {
        Err(ArchivePackageError::InvalidArchiveHeader)
    }
}

pub struct ArchivePackageEntryView<'a> {
    pub name: &'a str,
    pub data: &'a [u8],
}

impl<'a> ArchivePackageEntryView<'a> {
    fn read_from_view(
        buf: &'a [u8],
        offset: &mut usize,
    ) -> Result<Option<Self>, ArchivePackageError> {
        if buf.len() < *offset + 8 {
            return Ok(None);
        }

        if buf[*offset..*offset + 2] != ARCHIVE_ENTRY_PREFIX {
            return Err(ArchivePackageError::InvalidArchiveEntryHeader);
        }
        *offset += 2;

        let filename_size = u16::from_le_bytes([buf[*offset], buf[*offset + 1]]) as usize;
        *offset += 2;

        let data_size = u32::from_le_bytes([
            buf[*offset],
            buf[*offset + 1],
            buf[*offset + 2],
            buf[*offset + 3],
        ]) as usize;
        *offset += 4;

        if buf.len() < *offset + filename_size + data_size {
            return Err(ArchivePackageError::UnexpectedEntryEof);
        }

        let name = std::str::from_utf8(&buf[*offset..*offset + filename_size])
            .map_err(|_| ArchivePackageError::InvalidArchiveEntryName)?;
        *offset += filename_size;

        let data = &buf[*offset..*offset + data_size];
        *offset += data_size;

        Ok(Some(Self { name, data }))
    }
}

/// Archive data stream verifier
pub enum ArchivePackageVerifier {
    Start,
    PackageEntryHeader {
        buffer: [u8; ARCHIVE_ENTRY_HEADER_LEN],
        filled: usize,
    },
    PackageFileName {
        filename_len: usize,
        data_len: usize,
    },
    PackageData {
        data_len: usize,
    },
}

impl ArchivePackageVerifier {
    pub fn final_check(&self) -> Result<(), ArchivePackageError> {
        if matches!(self, Self::PackageEntryHeader { filled: 0, .. }) {
            Ok(())
        } else {
            Err(ArchivePackageError::UnexpectedArchiveEof)
        }
    }

    pub fn verify(&mut self, part: &[u8]) -> Result<(), ArchivePackageError> {
        let mut offset = 0;

        let part_len = part.len();

        while offset < part_len {
            let remaining = part_len - offset;

            match self {
                Self::Start if part_len >= 4 => {
                    read_package_header(part, &mut offset)?;
                    *self = Self::PackageEntryHeader {
                        buffer: Default::default(),
                        filled: 0,
                    }
                }
                Self::Start => return Err(ArchivePackageError::TooSmallInitialBatch),
                Self::PackageEntryHeader { buffer, filled } => {
                    let remaining = std::cmp::min(remaining, ARCHIVE_ENTRY_HEADER_LEN - *filled);

                    // SAFETY:
                    // - `offset < part.len()`
                    // - `filled < buffer.len()`
                    // - `offset + remaining < part.len() && `
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            part.as_ptr().add(offset),
                            buffer.as_mut_ptr().add(*filled),
                            remaining,
                        )
                    };

                    offset += remaining;
                    *filled += remaining;

                    if *filled == ARCHIVE_ENTRY_HEADER_LEN {
                        if buffer[..2] != ARCHIVE_ENTRY_PREFIX {
                            return Err(ArchivePackageError::InvalidArchiveEntryHeader);
                        }

                        *self = Self::PackageFileName {
                            filename_len: u16::from_le_bytes([buffer[2], buffer[3]]) as usize,
                            data_len: u32::from_le_bytes([
                                buffer[4], buffer[5], buffer[6], buffer[7],
                            ]) as usize,
                        }
                    }
                }
                Self::PackageFileName {
                    filename_len,
                    data_len,
                } => {
                    let remaining = std::cmp::min(remaining, *filename_len);
                    *filename_len -= remaining;
                    offset += remaining;

                    if *filename_len == 0 {
                        *self = Self::PackageData {
                            data_len: *data_len,
                        }
                    }
                }
                Self::PackageData { data_len } => {
                    let remaining = std::cmp::min(remaining, *data_len);
                    *data_len -= remaining;
                    offset += remaining;

                    if *data_len == 0 {
                        *self = Self::PackageEntryHeader {
                            buffer: Default::default(),
                            filled: 0,
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl Default for ArchivePackageVerifier {
    fn default() -> Self {
        Self::Start
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ArchivePackageError {
    #[error("Invalid archive header")]
    InvalidArchiveHeader,
    #[error("Unexpected archive eof")]
    UnexpectedArchiveEof,
    #[error("Invalid archive entry header")]
    InvalidArchiveEntryHeader,
    #[error("Invalid archive entry name")]
    InvalidArchiveEntryName,
    #[error("Unexpected entry eof")]
    UnexpectedEntryEof,
    #[error("Too small initial batch")]
    TooSmallInitialBatch,
}

pub const ARCHIVE_PREFIX: [u8; 4] = u32::to_le_bytes(0xae8fdd01);
const ARCHIVE_ENTRY_PREFIX: [u8; 2] = u16::to_le_bytes(0x1e8b);
const ARCHIVE_ENTRY_HEADER_LEN: usize = ARCHIVE_ENTRY_PREFIX.len() + 2 + 4; // magic + filename len + data len
