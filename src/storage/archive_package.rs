use std::io::Read;

use anyhow::Result;
use ton_types::ByteOrderRead;

pub struct ArchivePackageViewReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ArchivePackageViewReader<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self> {
        let mut offset = 0;
        read_package_header(data, &mut offset)?;
        Ok(Self { data, offset })
    }

    pub fn read_next(&mut self) -> Result<Option<ArchivePackageEntryView<'a>>> {
        ArchivePackageEntryView::read_from_view(self.data, &mut self.offset)
    }
}

fn read_package_header(buf: &[u8], offset: &mut usize) -> Result<()> {
    const PKG_HEADER_MAGIC: u32 = 0xae8fdd01;

    if buf.len() < *offset + 4 {
        return Err(ArchivePackageError::UnexpectedArchiveEof.into());
    }

    let magic = u32::from_le_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
    ]);
    *offset += 4;

    if magic == PKG_HEADER_MAGIC {
        Ok(())
    } else {
        Err(ArchivePackageError::InvalidArchiveHeader.into())
    }
}

pub struct ArchivePackageEntryView<'a> {
    pub name: &'a str,
    pub data: &'a [u8],
}

impl<'a> ArchivePackageEntryView<'a> {
    fn read_from_view(buf: &'a [u8], offset: &mut usize) -> Result<Option<Self>> {
        const ENTRY_HEADER_MAGIC: u16 = 0x1e8b;

        if buf.len() < *offset + 8 {
            return Ok(None);
        }

        if u16::from_le_bytes([buf[*offset], buf[*offset + 1]]) != ENTRY_HEADER_MAGIC {
            return Err(ArchivePackageError::InvalidArchiveEntryHeader.into());
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
            return Err(ArchivePackageError::UnexpectedEntryEof.into());
        }

        let name = std::str::from_utf8(&buf[*offset..*offset + filename_size])?;
        *offset += filename_size;

        let data = &buf[*offset..*offset + data_size];
        *offset += data_size;

        Ok(Some(Self { name, data }))
    }
}

#[derive(thiserror::Error, Debug)]
enum ArchivePackageError {
    #[error("Invalid archive header")]
    InvalidArchiveHeader,
    #[error("Unexpected archive eof")]
    UnexpectedArchiveEof,
    #[error("Invalid archive entry header")]
    InvalidArchiveEntryHeader,
    #[error("Unexpected entry eof")]
    UnexpectedEntryEof,
}
