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

    pub fn with_segment(name: &str, data: &[u8]) -> Self {
        let mut buffer = BytesMut::with_capacity(4 + name.len() + data.len() + 8);
        buffer.put_u32(PKG_HEADER_MAGIC);
        write_package_segment(&mut buffer, name, data);
        Self { buffer }
    }

    pub fn empty() -> Self {
        let mut buffer = BytesMut::with_capacity(4);
        buffer.put_u32(PKG_HEADER_MAGIC);
        Self { buffer }
    }

    pub fn add_package_segment(&mut self, name: &str, data: &[u8]) {
        self.buffer.reserve(name.len() + data.len() + 8);
        write_package_segment(&mut self.buffer, name, data);
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

fn write_package_segment(buffer: &mut BytesMut, name: &str, data: &[u8]) {
    buffer.put_u16(ENTRY_HEADER_MAGIC);
    buffer.put_u16(name.len() as u16);
    buffer.put_u32(data.len() as u32);
    buffer.put_slice(name.as_bytes());
    buffer.put_slice(data);
}
