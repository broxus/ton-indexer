use std::ffi::{c_void, CString};

pub type Allocator = tikv_jemallocator::Jemalloc;

pub const fn allocator() -> Allocator {
    tikv_jemallocator::Jemalloc
}

/// Sets jemalloc config tuned for the indexer load
///
/// SAFETY: no
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
