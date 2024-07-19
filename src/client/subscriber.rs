use async_stream::try_stream;
use bytes::Bytes;
use log::{debug, error};
use tokio_stream::Stream;

use crate::client::cli::Client;
use crate::cmd::unsubscribe::Unsubscribe;
use crate::connection::frame::Frame;
use crate::error::MiniRedisConnectionError;

pub struct Subscriber {
    pub(crate) client: Client,
    pub(crate) subscribed_channels: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Subscriber {
    pub async fn subcribe(&mut self, channels: &[String]) -> Result<(), MiniRedisConnectionError> {
        self.client.subscribe_cmd(channels).await?;

        self.subscribed_channels
            .extend(channels.iter().map(Clone::clone));

        Ok(())
    }

    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    pub async fn next_message(&mut self) -> Result<Option<Message>, MiniRedisConnectionError> {
        match self.client.conn.read_frame().await? {
            Some(frame) => {
                debug!("subscribe received: {:?}", frame);

                match frame {
                    Frame::Array(ref frame) => match frame.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message {
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_string()),
                        })),
                        _ => {
                            error!("invalid message, frame: {:?}", frame);
                            return Err(MiniRedisConnectionError::InvalidFrameType);
                        }
                    },
                    frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
                }
            }
            None => Ok(None),
        }
    }

    pub fn into_stream(mut self) -> impl Stream<Item = Result<Message, MiniRedisConnectionError>> {
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    pub async fn unsubscribe(
        &mut self,
        channels: &[String],
    ) -> Result<(), MiniRedisConnectionError> {
        let frame = Unsubscribe::new(channels).into_frame()?;
        debug!("unsubscribe request: {:?}", frame);
        self.client.conn.write_frame(&frame).await?;

        let num = if channels.is_empty() {
            self.subscribed_channels.len()
        } else {
            channels.len()
        };

        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len = self.subscribed_channels.len();

                        if len == 0 {
                            return Err(MiniRedisConnectionError::InvalidArgument(
                                response.to_string(),
                            ));
                        }

                        self.subscribed_channels.retain(|c| *channel != &c[..]);
                        if self.subscribed_channels.len() != len - 1 {
                            return Err(MiniRedisConnectionError::InvalidArgument(
                                response.to_string(),
                            ));
                        }
                    }
                    _ => {
                        // error!("unsubscribe channel failed, response: {:?}", response);
                        return Err(MiniRedisConnectionError::InvalidFrameType);
                    }
                },
                frame => return Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
            };
        }

        Ok(())
    }
}
