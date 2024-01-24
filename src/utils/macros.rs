#[macro_export]
macro_rules! metrics_updater_loop {
    // custom sleep duration via sleep => time
    ( sleep => $sleep_duration:expr, $( $metric_name:literal => $metric_value:expr ),+ $(,)? ) => {
        metrics_updater_loop!(INNER, $sleep_duration, $( $metric_name => $metric_value ),+)
    };
    // default 5 sec sleep
    ( $( $metric_name:literal => $metric_value:expr ),+ $(,)? ) => {
        metrics_updater_loop!(INNER, 5, $( $metric_name => $metric_value ),+)
    };
    // Inner implementation
    (INNER, $sleep_duration:expr, $( $metric_name:literal => $metric_value:expr ),+ $(,)? ) => {{
        use tokio::time::{self, Duration};
        loop {
            $(
                metrics::gauge!($metric_name).set($metric_value as f64);
            )+
            time::sleep(Duration::from_secs($sleep_duration)).await;
        }
    }};

}
