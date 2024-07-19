use crate::client::subscriber::Subscriber;
use crate::cmd::get::Get;
use bytes::Bytes;
use log::{debug, error};
use std::time::Duration;

use crate::cmd::ping::Ping;
use crate::cmd::publish::Publish;
use crate::cmd::set::Set;
use crate::cmd::subscribe::Subscribe;
use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::error::MiniRedisConnectionError;

pub struct Client {
    pub(crate) conn: Connection,
}

impl Client {
    pub(crate) async fn read_response(&mut self) -> Result<Frame, MiniRedisConnectionError> {
        let response = self.conn.read_frame().await?;
        debug!("read response: {:?}", response);
        match response {
            Some(Frame::Error(msg)) => Err(MiniRedisConnectionError::CommandExecute(msg)),
            Some(frame) => Ok(frame),
            None => {
                Err(MiniRedisConnectionError::Disconnect)
            }
        }
    }

    async fn set_cmd(&mut self, cmd: Set) -> Result<(), MiniRedisConnectionError> {
        let frame = cmd.into_frame()?;
        debug!("set request: {:?}", frame);

        self.conn.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    pub(crate) async fn subscribe_cmd(&mut self, channels: &[String]) -> Result<(), MiniRedisConnectionError> {
        let frame = Subscribe::new(channels).into_frame()?;
        debug!("subcribe request: {:?}", frame);

        self.conn.write_frame(&frame).await?;

        for channel in channels {
            let response = self.read_response().await?;
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {
                            debug!("subscribe channel: {} successfully", channel);
                        }
                    _ => {
                        error!("subscribe channel failed, response: {:?}", response);
                        return Err(MiniRedisConnectionError::CommandExecute(response.to_string()));
                    }
                },
                frame => {
                    error!("subscribe channel failed, response frame tyep not match: {:?}", frame);
                    return Err(MiniRedisConnectionError::InvalidFrameType);
                }
            };
        }

        Ok(())
    }

    pub async fn ping(&mut self, msg: Option<String>) -> Result<Bytes, MiniRedisConnectionError> {
        let frame = Ping::new(msg).into_frame()?;
        debug!("ping request: {:?}", frame);
        self.conn.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(v) => Ok(v.into()),
            Frame::Bulk(v) => Ok(v),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, MiniRedisConnectionError> {
        let frame = Get::new(key).into_frame()?;
        debug!("get request: {:?}", frame);

        self.conn.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(v) => Ok(Some(v.into())),
            Frame::Bulk(v) => Ok(Some(v)),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<(), MiniRedisConnectionError> {
        self.set_cmd(Set::new(key, value, None)).await
    }

    pub async fn set_expire(&mut self, key: &str, value: Bytes, epxiration: Duration) -> Result<(), MiniRedisConnectionError> {
        self.set_cmd(Set::new(key, value, Some(epxiration))).await
    }

    pub async fn publish(&mut self, channel: &str, message: Bytes) -> Result<u64, MiniRedisConnectionError> {
        let frame = Publish::new(channel, message).into_frame()?;
        debug!("publish request: {:?}", frame);

        self.conn.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    pub async fn subscribe(mut self, channels: Vec<String>) -> Result<Subscriber, MiniRedisConnectionError> {
        self.subscribe_cmd(&channels).await?;
        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

}