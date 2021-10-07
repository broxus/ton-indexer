mod reader;
mod writer;

pub use reader::*;
pub use writer::*;

const PKG_HEADER_MAGIC: u32 = 0xae8fdd01;
const ENTRY_HEADER_MAGIC: u16 = 0x1e8b;
