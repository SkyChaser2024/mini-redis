use std::pin::Pin;

use bytes::Bytes;
use log::{debug, warn};
use tokio::select;
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::cmd::unknown::Unknown;
use crate::cmd::unsubscribe::make_unsubscribe_frame;
use crate::cmd::Command;
use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::server::shutdown::Shutdown;
use crate::storage::db::Db;
use crate::storage::traits::KvStore;

type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

impl Subscribe {
    pub(crate) fn new(channels: &[String]) -> Self {
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Subscribe, MiniRedisParseError> {
        let mut channels = vec![parse.next_string()?];
        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(MiniRedisParseError::EndOfStream) => break,
                Err(err) => return Err(err),
            }
        }
        Ok(Subscribe { channels })
    }

    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<(), MiniRedisConnectionError> {
        let mut subscriptions = StreamMap::new();
        loop {
            for channel_name in self.channels.drain(..) {
                Self::subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            select! {
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)?).await?;
                }

                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        None => {
                            warn!("remote subscribe client disconnected");
                            return Ok(());
                        }
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }

                _ = shutdown.recv() => {
                    warn!("server shutdown, stop subscribe");
                    return Ok(());
                }
            }
        }
    }

    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()))?;
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()))?;
        }
        Ok(frame)
    }

    async fn subscribe_to_channel(
        channel_name: String,
        subscriptions: &mut StreamMap<String, Messages>,
        db: &Db,
        dst: &mut Connection,
    ) -> Result<(), MiniRedisConnectionError> {
        let mut rx = db.subscribe(channel_name.clone());
        let rx = Box::pin(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(msg) => yield msg,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(e)) => {
                        warn!("subscribe received lagged: {}", e);
                    }
                    Err(e) => {
                        warn!("subscribe received error: {}", e);
                        break;
                    }
                }
            }
        });

        subscriptions.insert(channel_name.clone(), rx);
        debug!("subscribed to channel success: {}", channel_name);
        let response = make_subscribe_frame(channel_name, subscriptions.len())?;
        dst.write_frame(&response).await?;

        Ok(())
    }
}

fn make_subscribe_frame(
    channel_name: String,
    num_subs: usize,
) -> Result<Frame, MiniRedisParseError> {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"))?;
    response.push_bulk(Bytes::from(channel_name))?;
    response.push_int(num_subs as u64)?;
    Ok(response)
}

fn make_message_frame(channel_name: String, msg: Bytes) -> Result<Frame, MiniRedisParseError> {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"))?;
    response.push_bulk(Bytes::from(channel_name))?;
    response.push_bulk(msg)?;
    Ok(response)
}

async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> Result<(), MiniRedisConnectionError> {
    match Command::from_frame(frame)? {
        Command::Subscribe(subscirbe) => {
            subscribe_to.extend(subscirbe.channels.into_iter());
        }

        Command::Unsubscribe(mut unsubscirbe) => {
            if unsubscirbe.channels.is_empty() {
                unsubscirbe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscirbe.channels {
                debug!("begin unsubscribe: {}", channel_name);
                subscriptions.remove(&channel_name);
                let response = make_unsubscribe_frame(channel_name, subscriptions.len())?;
                dst.write_frame(&response).await?;
                debug!("unsubscribe success: {}", response);
            }
        }

        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}
