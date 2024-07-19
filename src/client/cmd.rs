use std::num::ParseIntError;
use std::time::Duration;

use bytes::Bytes;
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum Command {
    Ping {
        msg: Option<String>,
    },

    Get {
        key: String,
    },

    Set {
        key: String,

        #[clap(parse(from_str = bytes_from_str))]
        value: Bytes,

        #[clap(parse(try_from_str = duration_from_ms_str))]
        expire: Option<Duration>
    },

    Publish {
        channel: String,

        #[clap(parse(from_str = bytes_from_str))]
        message: Bytes,
    },

    Subscribe {
        channels: Vec<String>,
    },
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

fn bytes_from_str(src: &str) -> Bytes {
    Bytes::from(src.to_string())
}