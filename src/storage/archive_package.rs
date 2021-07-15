use std::io::Read;

use anyhow::Result;
use ton_types::ByteOrderRead;

pub struct ArchivePackageEntryView<'a> {
    pub name: &'a str,
    pub data: &'a u8,
}

impl<'a> ArchivePackageEntryView<'a> {
    pub fn read_from<R>(reader: &mut R)
    where
        R: Read,
    {
        let mut buffer = [0; 8];

        reader.read_exact(reader)?;
        todo!()
    }
}

fn read_header<R>(reader: &mut R) -> Result<()>
where
    R: Read,
{
    const PKG_HEADER_MAGIC: u32 = 0xae8fdd01;
    if reader.read_le_u32()? == PKG_HEADER_MAGIC {
        Ok(())
    } else {
        Err(ArchivePackageError::InvalidArchiveHeader.into())
    }
}

#[derive(thiserror::Error, Debug)]
enum ArchivePackageError {
    #[error("Invalid archive header")]
    InvalidArchiveHeader,
}
