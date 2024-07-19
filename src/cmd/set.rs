use std::time::Duration;

use bytes::Bytes;
use log::{debug, warn};

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::storage::db::Db;
use crate::storage::traits::KvStore;

/// 设置 `key` 来保存字符串 `value`。
///
/// 如果 `key` 已经保存了一个值，则无论其类型如何，该值都会被覆盖。
/// 成功执行 SET 操作后，与该键相关联的任何先前生存时间都将被丢弃。
///
/// # 选项
///
/// 目前，支持以下选项：
///
/// * EX `seconds` -- 设置指定的过期时间，以秒为单位。
/// * PX `milliseconds` -- 设置指定的过期时间，以毫秒为单位。
#[derive(Debug)]
pub struct Set {
    /// 键
    key: String,
    /// 值
    value: Bytes,
    /// 可选的过期时间
    expire: Option<Duration>,
}

impl Set {
    /// 创建一个新的SET操作实例。
    ///
    /// # 参数
    ///
    /// * `key` - 键，实现了`ToString` trait。
    /// * `value` - 值，`Bytes`类型。
    /// * `expire` - 可选的过期时间，`Duration`类型。
    ///
    /// # 返回值
    ///
    /// 返回一个包含指定键、值和过期时间的SET操作实例。
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// 从接收到的帧中解析 `Set` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。此时，已从套接字接收到整个帧。
    ///
    /// `SET` 字符串已被使用。
    ///
    /// # 返回
    ///
    /// 成功时返回 `Set` 值。如果帧格式不正确，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望数组帧包含至少 3 个条目。
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Set, MiniRedisParseError> {
        let key = parse.next_string()?; // 读取 key
        let value = parse.next_bytes()?; // 读取 value
        let mut expire = None; // 因为 expire 是可选的， 下面使用 match 语句处理

        match parse.next_string() {
            // seconds
            Ok(s) if s.to_uppercase() == "EX" => {
                let seconds = parse.next_int()?;
                expire = Some(Duration::from_secs(seconds));
            }
            // milliseconds
            Ok(s) if s.to_uppercase() == "PX" => {
                let millis = parse.next_int()?;
                expire = Some(Duration::from_millis(millis));
            }
            // invalid，暂时不支持其他类型
            Ok(s) => {
                warn!("invalid set command argument: {:?}", s);
                return Err(MiniRedisParseError::Parse(
                    "currently `SET` only support the expiration option".into(),
                ));
            }
            // `EndOfStream` 错误表示没有进一步的数据需要解析。在这种情况下，这是正常的运行时情况，并且表示没有指定的 `SET` 选项。
            Err(MiniRedisParseError::EndOfStream) => {
                debug!("no extra SET option");
            }
            // 其他错误
            Err(e) => return Err(e),
        }

        Ok(Set { key, value, expire })
    }

    /// 将 `Set` 命令应用于指定的 `Db` 实例。
    ///
    /// 将响应写入 `dst`。服务器调用此方法以便执行收到的命令。
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
    ) -> Result<(), MiniRedisConnectionError> {
        // 在 db 中设置 key-value
        db.set(self.key, self.value, self.expire);

        let response = Frame::Simple("OK".to_string());

        debug!("set cmd applied response: {:?}", response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将 `SET` 操作转换为用于网络传输的 `Frame` 格式。
    ///
    /// # 返回值
    ///
    /// 返回一个表示 `SET` 请求的 `Frame` 实例。
    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()))?;
        frame.push_bulk(Bytes::from(self.key.into_bytes()))?;
        frame.push_bulk(self.value)?;

        if let Some(ms) = self.expire {
            // 我们选择 PX 选项，因为它提供了更多的精度
            frame.push_bulk(Bytes::from("px".as_bytes()))?;
            frame.push_int(ms.as_millis() as u64)?;
        }

        Ok(frame)
    }

    /// 获取 `key`。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 获取 `value`。
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// 获取 `expire`。
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }
}
