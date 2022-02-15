use std::ffi::{c_void, CString};
use std::os::raw::c_char;

use anyhow::Context;
use anyhow::Result;
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

const PROF_ACTIVE: &[u8] = b"prof.active\0";
const PROF_DUMP: &[u8] = b"prof.dump\0";

pub fn activate_prof() -> Result<()> {
    log::info!("start profiler");
    unsafe {
        if let Err(e) = tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, true) {
            log::error!("failed to activate profiling: {}", e);
            anyhow::bail!("failed to activate profiling: {}", e);
        }
    }
    Ok(())
}

pub fn deactivate_prof() -> Result<()> {
    log::info!("stop profiler");
    unsafe {
        if let Err(e) = tikv_jemalloc_ctl::raw::update(PROF_ACTIVE, false) {
            log::error!("failed to deactivate profiling: {}", e);
            anyhow::bail!("failed to deactivate profiling: {}", e);
        }
    }
    Ok(())
}

/// Dump the profile to the `path`.
pub fn dump_prof(path: &str) -> anyhow::Result<()> {
    let mut bytes = CString::new(path)?.into_bytes_with_nul();
    let ptr = bytes.as_mut_ptr() as *mut c_char;
    let res = unsafe { tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr) };
    match res {
        Err(e) => {
            log::error!("failed to dump the profile to {:?}: {}", path, e);
            Err(anyhow::Error::msg(e.to_string()))
                .with_context(|| format!("failed to dump the profile to {:?}", path))
        }
        Ok(_) => {
            log::info!("dump profile to {}", path);
            Ok(())
        }
    }
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

// run with `MALLOC_CONF="prof:true" cargo test --ignored`
#[cfg(test)]
mod test {
    const OPT_PROF: &[u8] = b"opt.prof\0";

    fn is_profiling_on() -> bool {
        match unsafe { tikv_jemalloc_ctl::raw::read(OPT_PROF) } {
            Err(e) => {
                // Shouldn't be possible since mem-profiling is set
                panic!("is_profiling_on: {:?}", e);
            }
            Ok(prof) => prof,
        }
    }

    #[test]
    #[ignore]
    fn test_profile() {
        use super::*;
        use std::io::Write;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("profile.txt");

        activate_prof().unwrap();
        assert!(is_profiling_on());

        dump_prof(&path.as_path().as_os_str().to_string_lossy()).unwrap();
        deactivate_prof().unwrap();
    }
}
