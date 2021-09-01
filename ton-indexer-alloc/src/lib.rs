use std::ffi::CString;

/// Sets jemalloc config
/// tuned for the indexer load
pub fn apply_je_config() {
    log::info!("Applying jemalloc conf");
    use std::ffi::c_void;
    use std::ptr::null_mut;
    fn cstr(str: &str) -> CString {
        CString::new(str).unwrap()
    }
    let res = unsafe {
        jemalloc_sys::mallctl(
            cstr("opt.abort_conf").as_ptr(),
            1 as *mut c_void,
            null_mut(),
            null_mut(),
            0,
        )
    };
    if res != 0 {
        log::error!("Abort fail: {}", errno::Errno(res));
    }
    let res = unsafe {
        jemalloc_sys::mallctl(
            cstr("opt.lg_extent_max_active_fit").as_ptr(),
            2 as *mut c_void,
            null_mut(),
            null_mut(),
            0,
        )
    };
    if res != 0 {
        log::error!("opt.lg_extent_max_active_fit fail: {}", errno::Errno(res));
    }
    let res = unsafe {
        jemalloc_sys::mallctl(
            cstr("opt.narenas").as_ptr(),
            2 as *mut c_void,
            null_mut(),
            null_mut(),
            0,
        )
    };
    if res != 0 {
        log::error!("opt.narenas fail: {}", errno::Errno(res));
    }
    let res = unsafe {
        jemalloc_sys::mallctl(
            cstr("opt.lg_tcache_max").as_ptr(),
            10 as *mut c_void,
            null_mut(),
            null_mut(),
            0,
        )
    };
    if res != 0 {
        log::error!("opt.lg_tcache_max fail: {}", errno::Errno(res));
    }
    let res = unsafe {
        jemalloc_sys::mallctl(
            cstr("opt.muzzy_decay_ms").as_ptr(),
            (100_isize) as *mut c_void,
            null_mut(),
            null_mut(),
            0,
        )
    };
    if res != 0 {
        log::error!("opt.muzzy_decay_ms fail: {}", errno::Errno(res));
    }
    let res = unsafe {
        jemalloc_sys::mallctl(
            cstr("opt.dirty_decay_ms").as_ptr(),
            (100_isize) as *mut c_void,
            null_mut(),
            null_mut(),
            0,
        )
    };
    if res != 0 {
        log::error!("opt.muzzy_decay_ms fail: {}", errno::Errno(res));
    }
    let res = unsafe {
        jemalloc_sys::mallctl(
            cstr("opt.tcache").as_ptr(),
            std::ptr::null_mut::<c_void>(),
            null_mut(),
            null_mut(),
            0,
        )
    };
    if res != 0 {
        log::error!("opt.tcache fail: {}", errno::Errno(res));
    }
    log::info!("Done");
}
