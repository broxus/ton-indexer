use std::io::Write;
use std::path::Path;

use bincode::Options;
use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_channel::Sender;
use serde::{Deserialize, Serialize};

pub static mut COLLECTOR: Collector = Collector { tx: None };

pub fn init<P: AsRef<Path>>(logs: P) -> std::io::Result<()> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let out_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(logs.as_ref())?;

    std::thread::spawn(|| {
        let mut writer = std::io::BufWriter::new(out_file);
        let options = bincode::DefaultOptions::new().with_varint_encoding();
        let mut buffer = Vec::with_capacity(1024);
        for record in rx {
            buffer.clear();
            options.serialize_into(&mut buffer, &record).unwrap();
            if let Err(e) = writer.write_u16::<LittleEndian>(buffer.len() as u16) {
                log::error!("Failed saving profile: {}", e);
            }
            if let Err(e) = writer.write_all(&buffer) {
                log::error!("Failed saving profile: {}", e);
            }
        }
    });

    unsafe {
        COLLECTOR.tx = Some(tx);
    }

    Ok(())
}

#[derive(Default)]
pub struct Collector {
    tx: Option<Sender<Record>>,
}

impl Collector {
    pub fn add_record(&self, record: Record) {
        if let Some(tx) = &self.tx {
            tx.send(record).ok();
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Record {
    x: Option<u64>,
    /// NOTE: 500 years should be enough
    y: u64,
    id: &'static str,
    path: &'static str,
}

impl Record {
    pub fn new(
        time: &std::time::Instant,
        x: Option<u64>,
        id: &'static str,
        path: &'static str,
    ) -> Self {
        Self {
            y: time.elapsed().as_nanos() as u64,
            x,
            id,
            path,
        }
    }
}

#[cfg(feature = "active")]
#[macro_export]
macro_rules! span {
    ($name:tt, $expr:expr) => {{
        let __time = ::std::time::Instant::now();
        let __res = $expr;
        // SAFETY: yes
        unsafe {
            $crate::COLLECTOR.add_record($crate::Record::new(
                &__time,
                None,
                $crate::span!(@expand_id $name),
                module_path!(),
            ));
        }
        __res
    }};

    (@expand_id $id:literal) => { $id };
    (@expand_id $id:ident) => { stringify!($id) };
}

#[cfg(feature = "active")]
#[macro_export]
macro_rules! start {
    ($name:ident) => {
        let $name = ::std::time::Instant::now();
    };
}

#[cfg(feature = "active")]
#[macro_export]
macro_rules! tick {
    ($name:ident$( => $id:literal)?$(, x = $x:expr)?) => {
        // SAFETY: yes
        unsafe {
            $crate::COLLECTOR.add_record($crate::Record::new(
                &$name,
                $crate::tick!(@expand_x $($x)?),
                $crate::tick!(@expand_id $name $($id)?),
                module_path!(),
            ));
        }
    };

    (@expand_x $x:expr) => { Some(From::from($x)) };
    (@expand_x) => { None };

    (@expand_id $name:ident $id:literal) => { $id };
    (@expand_id $name:ident) => { stringify!($id) };
}

#[cfg(not(feature = "active"))]
#[macro_export]
macro_rules! span {
    ($name:tt, $expr:expr) => {
        $expr
    };
}

#[cfg(not(feature = "active"))]
#[macro_export]
macro_rules! start {
    ($name:ident) => {};
}

#[cfg(not(feature = "active"))]
#[macro_export]
macro_rules! tick {
    ($name:ident$( => $id:literal)?$(, x = $x:expr)?) => {};
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use log::LevelFilter;

    use super::*;

    #[test]
    fn test1() {
        env_logger::builder()
            .is_test(true)
            .filter_level(LevelFilter::Trace)
            .try_init()
            .ok();
        log::info!("Start");
        start!(lol);
        start!(lel);
        std::thread::sleep(Duration::from_secs(1));
        tick!(lol, "heh");
        std::thread::sleep(Duration::from_millis(100));
        tick!(lol);
        tick!(lel);
    }
}
