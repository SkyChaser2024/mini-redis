use bytes::Bytes; // 引入 bytes 库中的 Bytes 类型，用于处理原始二进制数据
use log::debug; // 引入 log 库的 debug 宏，用于输出调试信息

// 引入本项目内的模块，用于处理连接、帧解析和错误处理
use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};

/// 表示一个 Ping 操作的结构体。
///
/// 包含一个可选的 `Bytes` 类型的消息。如果没有提供参数 `msg`，则返回 PONG，否者返回参数的副本
/// 作为 bulk 返回。
///
/// 此命令主要用于测试连接是否有效，或者测量延迟。
#[derive(Debug, Default)]
pub struct Ping {
    // 可选的消息，如果提供，则在响应中返回该消息
    msg: Option<String>,
}

impl Ping {
    /// 创建一个包含 optional `msg` 的 Ping 实例。
    pub fn new(msg: Option<String>) -> Ping {
        Ping { msg }
    }

    /// 从接收到的帧中解析 `Ping` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。此时，已从套接字接收到整个帧。
    ///
    /// `PING` 字符串已被使用。
    ///
    /// # 返回
    ///
    /// 成功时返回 `Ping` 值。如果帧格式不正确，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含 `PING` 和可选消息的数组帧。
    ///
    /// ```text
    /// PING [消息]
    /// ```
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Ping, MiniRedisParseError> {
        match parse.next_string() {
            Ok(msg) => Ok(Ping::new(Some(msg))), // 成功解析 msg
            Err(MiniRedisParseError::EndOfStream) => Ok(Ping::default()), // 流结束，返回默认 Ping
            Err(e) => Err(e),                    // 其他错误
        }
    }

    /// 应用 `Ping` 命令并返回消息。
    ///
    /// 将响应写入 `dst`。服务器调用此方法以便执行收到的命令。
    pub(crate) async fn apply(self, dst: &mut Connection) -> Result<(), MiniRedisConnectionError> {
        let response = match self.msg {
            Some(msg) => Frame::Bulk(Bytes::from(msg)), // 如果有消息，创建一个 bulk frame
            None => Frame::Simple("PONG".to_string()),  // 没有消息，创建一个 PONG frame
        };

        debug!("ping cmd applied response: {}", response);

        // 将 response 写入 dst（客户端）
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将 `PING` 命令转换为用于网络传输的 `Frame` 格式，主要为客户端使用。
    ///
    /// # 返回值
    ///
    /// 返回一个表示 `PING` 请求的 `Frame` 实例。
    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()))?;
        if let Some(msg) = self.msg {
            frame.push_bulk(Bytes::from(msg))?;
        }
        Ok(frame)
    }
}
