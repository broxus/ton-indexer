use std::fs::File;
use std::io::{IoSlice, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};

use super::block_maps::*;

pub struct ArchiveWritersPool {
    save_to_disk_threshold: usize,
    acquired_memory: Arc<AtomicUsize>,
    temp_file_index: AtomicUsize,
    base_path: PathBuf,
}

impl ArchiveWritersPool {
    pub fn new(base_path: impl AsRef<Path>, save_to_disk_threshold: usize) -> Self {
        Self {
            save_to_disk_threshold,
            acquired_memory: Arc::new(Default::default()),
            temp_file_index: Default::default(),
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    pub fn acquire(&self) -> Result<Box<dyn AcquiredArchiveWriter>> {
        let acquired_memory = self.acquired_memory.load(Ordering::Acquire);
        if acquired_memory < self.save_to_disk_threshold {
            return Ok(Box::new(InMemoryWriter {
                acquired_memory: self.acquired_memory.clone(),
                buffer: Vec::new(),
            }));
        }

        let temp_file_index = self.temp_file_index.fetch_add(1, Ordering::AcqRel);
        let path = self.base_path.join(format!("archive{temp_file_index:04}"));

        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .context("Failed to create file writer")?;

        Ok(Box::new(FileWriter { path, file }))
    }
}

pub trait AcquiredArchiveWriter: Write + Send + Sync + 'static {
    fn parse_block_maps(&self) -> Result<Arc<BlockMaps>>;
}

struct FileWriter {
    path: PathBuf,
    file: File,
}

impl AcquiredArchiveWriter for FileWriter {
    fn parse_block_maps(&self) -> Result<Arc<BlockMaps>> {
        let mapped_file =
            FileWriterView::new(&self.file).context("Failed to map temp archive file")?;

        BlockMaps::new(mapped_file.as_slice())
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            log::error!("Failed to remove temp archive file {:?}: {e:?}", self.path);
        }
    }
}

// NOTE: buffered writer is not needed here because we are going to write big chunks of data
impl Write for FileWriter {
    #[inline(always)]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    #[inline(always)]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        self.file.write_vectored(bufs)
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

struct FileWriterView<'a> {
    _file: &'a File,
    length: usize,
    ptr: *mut libc::c_void,
}

impl<'a> FileWriterView<'a> {
    fn new(file: &'a File) -> std::io::Result<Self> {
        use std::os::unix::io::AsRawFd;

        let length = file.metadata()?.len() as usize;

        // SAFETY: File was opened successfully, file mode is R, offset is aligned
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        if unsafe { libc::madvise(ptr, length, libc::MADV_SEQUENTIAL) } != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            _file: file,
            length,
            ptr,
        })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.length) }
    }
}

impl Drop for FileWriterView<'_> {
    fn drop(&mut self) {
        // SAFETY: File still exists, ptr and length were initialized once on creation
        if unsafe { libc::munmap(self.ptr, self.length) } != 0 {
            // TODO: how to handle this?
            let error = std::io::Error::last_os_error();
            panic!("failed to unmap temp archive file: {error}");
        }
    }
}

struct InMemoryWriter {
    acquired_memory: Arc<AtomicUsize>,
    buffer: Vec<u8>,
}

impl AcquiredArchiveWriter for InMemoryWriter {
    fn parse_block_maps(&self) -> Result<Arc<BlockMaps>> {
        BlockMaps::new(&self.buffer)
    }
}

impl Drop for InMemoryWriter {
    fn drop(&mut self) {
        self.acquired_memory
            .fetch_sub(self.buffer.len(), Ordering::Release);
    }
}

impl Write for InMemoryWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.acquired_memory.fetch_add(buf.len(), Ordering::Release);
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        let len = bufs.iter().map(|b| b.len()).sum();
        self.acquired_memory.fetch_add(len, Ordering::Release);
        self.buffer.reserve(len);
        for buf in bufs {
            self.buffer.extend_from_slice(buf);
        }
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.acquired_memory.fetch_add(buf.len(), Ordering::Release);
        self.buffer.extend_from_slice(buf);
        Ok(())
    }
}
