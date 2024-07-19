use bytes::Bytes;
use log::debug;

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::storage::db::Db;
use crate::storage::traits::KvStore;

/// 获取 key 的值。
///
/// 如果 key 不存在，则返回特殊值 nil。如果 key 中存储的值不是字符串，则返回错误，因为 GET 仅处理字符串值。
#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    /// 创建一个新的 `Get` 命令来获取 `key`。
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// 获取 `key` 的值。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 从接收到的帧中解析 `Get` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。此时，整个帧已从套接字接收。
    ///
    /// `GET` 字符串已被使用。
    ///
    /// # 返回
    ///
    /// 成功时返回 `Get` 值。如果帧格式不正确，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含两个条目的数组帧。
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Get, MiniRedisParseError> {
        let key = parse.next_string()?;

        Ok(Get { key })
    }

    /// 将 `Get` 命令应用于指定的 `Db` 实例。
    ///
    /// 将响应写入 `dst`。服务器调用此方法以便执行收到的命令。
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
    ) -> Result<(), MiniRedisConnectionError> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(Bytes::from(value))
        } else {
            Frame::Null
        };

        debug!("get cmd applied response: {:?}", response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将 `GET` 操作转换为用于网络传输的 `Frame` 格式。
    ///
    /// # 返回值
    ///
    /// 返回一个表示 `GET` 请求的 `Frame` 实例。
    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()))?;
        frame.push_bulk(Bytes::from(self.key.into_bytes()))?;

        Ok(frame)
    }
}
