use bytes::{BufMut, BytesMut};

use super::{ENTRY_HEADER_MAGIC, PKG_HEADER_MAGIC};

pub struct PackageWriter {
    buffer: BytesMut,
}

impl PackageWriter {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            buffer: BytesMut::from(bytes),
        }
    }

    pub fn with_data(name: &str, data: &[u8]) -> Self {
        let mut pack = Self::new();
        pack.add_package_segment(name, data);
        pack
    }

    pub fn new() -> Self {
        let mut bytes = BytesMut::with_capacity(2 * 1024 * 1024);
        bytes.put_u32(PKG_HEADER_MAGIC);
        Self { buffer: bytes }
    }

    pub fn add_package_segment(&mut self, name: &str, data: &[u8]) {
        let mut buf = BytesMut::with_capacity(name.len() + data.len() + 8);

        buf.put_u16(ENTRY_HEADER_MAGIC);
        buf.put_u16(name.len() as u16);
        buf.put_u32(data.len() as u32);
        buf.put_slice(name.as_bytes());
        buf.put_slice(data);
        self.buffer.extend_from_slice(buf.as_ref());
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}
