#[macro_export]
macro_rules! set_metrics {
    ($($metric_name:expr => $metric_value:expr),* $(,)?) => {
        $(
            metrics::gauge!($metric_name).set($metric_value as f64);
        )*
    };
}

#[macro_export]
macro_rules! set_metrics_with_label {
    ($label:expr, { $($metric_name:expr => $metric_value:expr),* $(,)? }) => {
        $(
            metrics::gauge!($metric_name, [$label].iter()).set($metric_value as f64);
        )*
    };
}
