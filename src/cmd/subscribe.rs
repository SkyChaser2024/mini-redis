use std::pin::Pin; // 提供一个安全的机制来防止被 Pin 的值被移动

use bytes::Bytes;
use log::{debug, warn};
use tokio::select;
use tokio_stream::{Stream, StreamExt, StreamMap}; // 提供 tokio 中的流处理功能

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

/// 消息流。流从 `broadcast::Receiver` 接收消息。
/// 我们使用 `stream!` 来创建一个消费消息的 `Stream`。
/// 由于 `stream!` 值不能被命名，因此我们使用特征对象来装箱流。
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

/// 订阅客户端到一个或多个频道。
///
/// 一旦客户端进入订阅状态，除了额外的 SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
/// PUNSUBSCRIBE, PING 和 QUIT 命令之外，不应发出任何其他命令。
#[derive(Debug)]
pub struct Subscribe {
    /// 订阅的频道列表。
    channels: Vec<String>,
}

impl Subscribe {
    /// 创建一个新的 `Subscribe` 实例。
    ///
    /// # 参数
    ///
    /// * `channels` - 要订阅的频道名称列表。
    ///
    /// # 返回值
    ///
    /// 返回 `Subscribe` 实例。
    pub(crate) fn new(channels: &[String]) -> Self {
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    /// 从接收到的帧解析 `Subscribe` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字接收。
    ///
    /// `SUBSCRIBE` 字符串已经被消费。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Subscribe` 值。如果帧格式错误，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含两个或更多条目的数组帧。
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Subscribe, MiniRedisParseError> {
        let mut channels = vec![parse.next_string()?];
        // 现在，帧的剩余部分被消费。每个值必须是字符串，否则帧格式错误
        // 一旦帧中的所有值都被消费，命令即被完全解析
        loop {
            match parse.next_string() {
                // 从 `parse` 中消费了一个字符串，将其推入订阅频道列表。
                Ok(s) => channels.push(s),
                // `EndOfStream` 错误表示没有更多数据可解析
                Err(MiniRedisParseError::EndOfStream) => break,
                // 所有其他错误会冒泡上来，导致连接终止
                Err(err) => return Err(err),
            }
        }
        Ok(Subscribe { channels })
    }

    /// 应用订阅操作。
    ///
    /// # 参数
    ///
    /// * `db` - 数据库实例的引用。
    /// * `dst` - 连接实例的可变引用。
    /// * `shutdown` - 服务器关闭信号的可变引用。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Ok(())`，失败时返回连接错误。
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<(), MiniRedisConnectionError> {
        // 每个单独的频道订阅都使用 `sync::broadcast` 频道来处理。
        // 然后消息被分发给当前订阅这些频道的所有客户端。
        //
        // 一个客户端可以订阅多个频道，并可以动态地添加和删除其订阅集中的频道。
        // 为了解决这个问题，使用 `StreamMap` 来跟踪活动订阅。
        // `StreamMap` 将来自单个广播频道的消息合并在一起。
        let mut subscriptions = StreamMap::new();
        loop {
            // `self.channels` 用于跟踪要订阅的额外频道。
            // 在 `apply` 的执行过程中收到新的 `SUBSCRIBE` 命令时，
            // 新的频道会被推入这个向量。
            for channel_name in self.channels.drain(..) {
                Self::subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // 等待以下情况之一发生：
            //
            // - 从订阅的频道接收消息。
            // - 从客户端接收订阅或取消订阅命令。
            // - 服务器关闭信号。
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

    /// 将 `Subscribe` 实例转换为帧。
    ///
    /// # 返回值
    ///
    /// 成功时返回帧，失败时返回解析错误。
    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()))?;
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()))?;
        }
        Ok(frame)
    }

    /// 订阅指定频道。
    ///
    /// # 参数
    ///
    /// * `channel_name` - 要订阅的频道名称。
    /// * `subscriptions` - 订阅映射的可变引用。
    /// * `db` - 数据库实例的引用。
    /// * `dst` - 连接实例的可变引用。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Ok(())`，失败时返回连接错误。
    async fn subscribe_to_channel(
        channel_name: String,
        subscriptions: &mut StreamMap<String, Messages>,
        db: &Db,
        dst: &mut Connection,
    ) -> Result<(), MiniRedisConnectionError> {
        let mut rx = db.subscribe(channel_name.clone());
        // 订阅频道
        let rx = Box::pin(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(msg) => yield msg,
                    // 如果我们在消费消息时滞后了，只需恢复
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
        // 在此客户端的订阅集中跟踪订阅
        subscriptions.insert(channel_name.clone(), rx);
        debug!("subscribed to channel success: {}", channel_name);
        let response = make_subscribe_frame(channel_name, subscriptions.len())?;
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// 创建订阅请求的响应。
///
/// 所有这些函数都将 `channel_name` 作为 `String` 而不是 `&str`，因为
/// `Bytes::from` 可以重用 `String` 中的分配，而使用 `&str` 会要求复制数据。
/// 这允许调用者决定是否克隆频道名称。
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

/// 创建一个消息，通知客户端关于其订阅频道的新消息。
fn make_message_frame(channel_name: String, msg: Bytes) -> Result<Frame, MiniRedisParseError> {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"))?;
    response.push_bulk(Bytes::from(channel_name))?;
    response.push_bulk(msg)?;
    Ok(response)
}

/// 处理在 `Subscribe::apply` 内接收到的命令。只有订阅和取消订阅命令在此上下文中被允许。
///
/// 新的订阅将被添加到 `subscribe_to` 中，而不是修改 `subscriptions`。
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> Result<(), MiniRedisConnectionError> {
    // 从客户端接收到一个命令。
    //
    // 在此上下文中只允许 `SUBSCRIBE` 和 `UNSUBSCRIBE` 命令。
    match Command::from_frame(frame)? {
        Command::Subscribe(subscirbe) => {
            // `apply` 方法将订阅我们添加到这个向量中的频道
            subscribe_to.extend(subscirbe.channels.into_iter());
        }

        Command::Unsubscribe(mut unsubscirbe) => {
            // 如果未指定频道，这表示请求取消订阅 **所有** 频道
            // 为了实现这一点，`unsubscribe.channels` 向量会填充当前订阅的频道列表
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
