use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::storage::db::Db;
use crate::storage::traits::KvStore;
use bytes::Bytes;
use log::debug;

#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Self {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Publish, MiniRedisParseError> {
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;
        Ok(Publish { channel, message })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> Result<(), MiniRedisConnectionError> {
        let num_subscribers = db.publish(&self.channel, self.message);
        let response = Frame::Integer(num_subscribers as u64);
        debug!("apply cmd applied response: {}", response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()))?;
        frame.push_bulk(Bytes::from(self.channel.into_bytes()))?;
        frame.push_bulk(Bytes::from(self.message))?;
        Ok(frame)
    }
}
