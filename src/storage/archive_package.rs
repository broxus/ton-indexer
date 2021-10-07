use std::io::Write;

use anyhow::Result;

pub fn make_empty_archive() -> Vec<u8> {
    PKG_HEADER_MAGIC.to_le_bytes().to_vec()
}

pub fn make_archive_segment(filename: &str, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut vec = Vec::with_capacity(2 + 2 + 4 + filename.len() + data.len());
    vec.write_all(&ENTRY_HEADER_MAGIC.to_le_bytes())?;
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

const PKG_HEADER_MAGIC: u32 = 0xae8fdd01;
const ENTRY_HEADER_MAGIC: u16 = 0x1e8b;

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
