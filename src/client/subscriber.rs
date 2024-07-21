use async_stream::try_stream;
use bytes::Bytes;
use log::{debug, error};
use tokio_stream::Stream;

use crate::client::cli::Client;
use crate::cmd::unsubscribe::Unsubscribe;
use crate::connection::frame::Frame;
use crate::error::MiniRedisConnectionError;

/// 进入发布/订阅模式的客户端。
///
/// 一旦客户端订阅了一个频道，它只能执行与发布/订阅相关的命令。
/// `Client` 类型转换为 `Subscriber` 类型以防止调用非发布/订阅方法。
pub struct Subscriber {
    /// 订阅的客户端
    pub(crate) client: Client,
    /// `Subscriber` 当前订阅的频道集合
    pub(crate) subscribed_channels: Vec<String>,
}

/// 在订阅的频道上收到的消息
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Subscriber {
    /// 订阅新的频道列表
    pub async fn subscribe(&mut self, channels: &[String]) -> Result<(), MiniRedisConnectionError> {
        // 发出订阅命令
        self.client.subscribe_cmd(channels).await?;

        // 更新订阅的频道集合
        self.subscribed_channels
            .extend(channels.iter().map(Clone::clone));

        Ok(())
    }

    /// 返回当前订阅的频道集合
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// 接收在订阅的频道上发布的下一条消息，必要时等待。
    ///
    /// `None` 表示订阅已终止。
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

    /// 将订阅者转换为 `Stream`，返回在订阅的频道上发布的新消息。
    ///
    /// `Subscriber` 自身不实现 stream，因为使用安全代码实现这个功能并非易事。
    /// 使用 async/await 需要手动实现 Stream 来使用 `unsafe` 代码。
    /// 因此，提供了一个转换函数，返回的 stream 使用 `async-stream` crate 的帮助实现。
    pub fn into_stream(mut self) -> impl Stream<Item = Result<Message, MiniRedisConnectionError>> {
        // 使用 `async-stream` crate 的 `try_stream` 宏来实现 stream
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// 取消订阅新的频道列表
    pub async fn unsubscribe(
        &mut self,
        channels: &[String],
    ) -> Result<(), MiniRedisConnectionError> {
        let frame = Unsubscribe::new(channels).into_frame()?;
        debug!("unsubscribe request: {:?}", frame);
        self.client.conn.write_frame(&frame).await?;

        // 如果输入的频道列表为空，服务器会确认取消订阅所有订阅的频道，因此我们断言接收到的取消订阅列表匹配客户端订阅的频道
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

                        // 至少应该有一个频道
                        if len == 0 {
                            return Err(MiniRedisConnectionError::InvalidArgument(
                                response.to_string(),
                            ));
                        }

                        // 在这一点，取消订阅的频道应该存在于订阅列表中
                        self.subscribed_channels.retain(|c| *channel != &c[..]);

                        // 订阅的频道列表中应该只删除一个频道
                        if self.subscribed_channels.len() != len - 1 {
                            return Err(MiniRedisConnectionError::CommandExecute(
                                response.to_string(),
                            ));
                        }
                    }
                    _ => {
                        return Err(MiniRedisConnectionError::InvalidFrameType);
                    }
                },
                frame => return Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
            };
        }

        Ok(())
    }
}
