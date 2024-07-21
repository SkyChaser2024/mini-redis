use bytes::Bytes;
use log::debug;

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::storage::db::Db;
use crate::storage::traits::KvStore;

/// 向指定频道发布消息。
///
/// 在不知道具体消费者的情况下，将消息发送到频道中。
/// 消费者可以订阅频道以接收消息。
///
/// 频道名称与键值命名空间没有关系。在名为 "foo" 的频道上发布与设置 "foo" 键没有关系。
#[derive(Debug)]
pub struct Publish {
    /// 频道名称
    channel: String,
    /// 消息内容，使用 `Bytes` 类型以支持二进制数据
    message: Bytes,
}

impl Publish {
    /// 创建一个新的 `Publish` 实例。
    ///
    /// # 参数
    ///
    /// * `channel` - 发布消息的频道名称。
    /// * `message` - 要发布的消息内容。
    ///
    /// # 返回值
    ///
    /// 返回一个 `Publish` 实例。
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Self {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// 从接收到的帧中解析 `Publish` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API 用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字接收到。
    ///
    /// `PUBLISH` 字符串已被使用。
    ///
    /// # 返回
    ///
    /// 成功时返回 `Publish` 值。如果帧格式不正确，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望数组帧包含三个条目。
    ///
    /// ```text
    /// PUBLISH channel message
    /// ```
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Publish, MiniRedisParseError> {
        // `channel` 必须是有效的字符串
        let channel = parse.next_string()?;
         // `message` 是任意字节
        let message = parse.next_bytes()?;
        Ok(Publish { channel, message })
    }

    /// 将 `Publish` 操作应用到数据库，并将响应写入连接。
    ///
    /// # 参数
    ///
    /// * `db` - 数据库实例。
    /// * `dst` - 目标连接。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Ok(())`，失败时返回 `MiniRedisConnectionError`。
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
    ) -> Result<(), MiniRedisConnectionError> {
        // 共享状态包含所有活跃频道的 `tokio::sync::broadcast::Sender`。
        // 调用 `db.publish` 将消息发送到适当的频道。
        //
        // 返回当前监听频道的订阅者数量。这并不意味着 `num_subscriber` 频道将接收消息。
        // 订阅者可能在接收消息之前退出。鉴于此，`num_subscribers` 仅应作为“提示”使用。
        let num_subscribers = db.publish(&self.channel, self.message);
        // 订阅者数量作为发布请求的响应返回
        let response = Frame::Integer(num_subscribers as u64);
        debug!("apply cmd applied response: {}", response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// 将 `Publish` 操作转换为用于网络传输的 `Frame` 格式。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Frame` 实例，失败时返回 `MiniRedisParseError`。
    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()))?;
        frame.push_bulk(Bytes::from(self.channel.into_bytes()))?;
        // frame.push_bulk(Bytes::from(self.message))?;
        frame.push_bulk(self.message)?;
        Ok(frame)
    }
}
