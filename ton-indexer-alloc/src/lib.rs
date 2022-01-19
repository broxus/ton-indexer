use std::ffi::{c_void, CString};

pub use tikv_jemalloc_ctl::Error;
use tikv_jemalloc_ctl::{epoch, stats};

pub type Allocator = tikv_jemallocator::Jemalloc;

pub const fn allocator() -> Allocator {
    tikv_jemallocator::Jemalloc
}

/// Sets jemalloc config tuned for the indexer load
///
/// # Safety
/// no
pub unsafe fn apply_config() {
    log::info!("Applying jemalloc conf");

    set_jemalloc_param("opt.abort_conf", true);
    set_jemalloc_param("opt.lg_extent_max_active_fit", 2_usize);
    set_jemalloc_param("opt.narenas", 2_u32);
    set_jemalloc_param("opt.lg_tcache_max", 10_usize);
    set_jemalloc_param("opt.muzzy_decay_ms", 100_isize);
    set_jemalloc_param("opt.dirty_decay_ms", 100_isize);

    log::info!("Done");
}

fn set_jemalloc_param<T>(name: &str, mut value: T) {
    let name_buffer = CString::new(name).unwrap();

    let res = unsafe {
        tikv_jemalloc_sys::mallctl(
            name_buffer.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut value as *mut T as *mut c_void,
            std::mem::size_of::<T>(),
        )
    };

    if res != 0 {
        log::error!("Failed to set {}: {}", name, errno::Errno(res));
    }
}

pub fn fetch_stats() -> Result<JemallocStats, Error> {
    // Stats are cached. Need to advance epoch to refresh.
    epoch::advance()?;

    Ok(JemallocStats {
        allocated: stats::allocated::read()? as u64,
        active: stats::active::read()? as u64,
        metadata: stats::metadata::read()? as u64,
        resident: stats::resident::read()? as u64,
        mapped: stats::mapped::read()? as u64,
        retained: stats::retained::read()? as u64,
        dirty: (stats::resident::read()? - stats::active::read()? - stats::metadata::read()?)
            as u64,
        fragmentation: (stats::active::read()? - stats::allocated::read()?) as u64,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JemallocStats {
    pub allocated: u64,
    pub active: u64,
    pub metadata: u64,
    pub resident: u64,
    pub mapped: u64,
    pub retained: u64,
    pub dirty: u64,
    pub fragmentation: u64,
}
