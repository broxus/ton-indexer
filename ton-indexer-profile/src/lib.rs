#[cfg(feature = "active")]
#[macro_export]
macro_rules! profile_start {
    ($name:ident) => {
        let $name = std::time::Instant::now();
    };
}

#[cfg(feature = "active")]
#[macro_export]
macro_rules! profile_elapsed {
    ($name:ident) => {
        ::log::trace!(
            "#PERF#{{\"took\":{},\"context\":\"{}\",\"path\":\"{{{}::{}::{}}}\"}}#PERF#",
            $name.elapsed().as_nanos(),
            stringify!($name),
            file!(),
            module_path!(),
            line!()
        )
    };
    ($name:ident, $ctx:literal) => {
        // log_record!(
        //     &$name,
        //     $ctx,
        //     &format!("{}::{}::{}", file!(), module_path!(), line!()),
        // );
        ::log::trace!(
            "#PERF#{{\"took\":{},\"context\":\"{}\",\"path\":\"{{{}::{}::{}}}\"}}#PERF#",
            $name.elapsed().as_nanos(),
            $ctx,
            file!(),
            module_path!(),
            line!()
        )
    };
}

#[cfg(not(feature = "active"))]
#[macro_export]
macro_rules! profile_elapsed {
    ($name:ident) => {};
    ($name:ident, $ctx:literal) => {};
}

#[cfg(not(feature = "active"))]
#[macro_export]
macro_rules! profile_start {
    ($name:ident) => {};
}

#[macro_export]
macro_rules! log_record {
    ($time:expr,$context:expr,$path:expr) => {
        ::log::trace!(
            "#PERF#{{\"took\":{},\"context\":\"{}\",\"path\":\"{}\"}}#PERF#",
            $time.elapsed().as_nanos(),
            $context,
            $path
        )
    };
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use log::LevelFilter;

    use crate::log_record;

    use super::*;

    #[test]
    fn test1() {
        env_logger::builder()
            .is_test(true)
            .filter_level(LevelFilter::Trace)
            .try_init()
            .ok();
        log::info!("Start");
        profile_start!(lol);
        std::thread::sleep(Duration::from_secs(1));
        profile_elapsed!(lol, "heh");
        std::thread::sleep(Duration::from_millis(100));
        profile_elapsed!(lol);
    }
}
