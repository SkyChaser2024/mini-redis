use bytes::Bytes;

use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::MiniRedisParseError;

#[derive(Clone, Debug)]
pub struct Unsubscribe {
    pub(crate) channels: Vec<String>,
}

impl Unsubscribe {
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Unsubscribe, MiniRedisParseError> {
        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(MiniRedisParseError::EndOfStream) => break,
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()))?;

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()))?;
        }

        Ok(frame)
    }
}

pub(crate) fn make_unsubscribe_frame(
    channel_name: String,
    num_subs: usize,
) -> Result<Frame, MiniRedisParseError> {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"))?;
    response.push_bulk(Bytes::from(channel_name))?;
    response.push_int(num_subs as u64)?;
    Ok(response)
}
