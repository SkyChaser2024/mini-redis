use bytes::Bytes;

use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::MiniRedisParseError;

/// 从一个或多个频道取消客户端的订阅。
///
/// 当没有指定频道时，客户端会从所有之前订阅的频道取消订阅。
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    pub(crate) channels: Vec<String>, // 存储需要取消订阅的频道名称列表
}

impl Unsubscribe {
    /// 使用给定的 `channels` 创建一个新的 `Unsubscribe` 命令。
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// 从接收到的帧解析 `Unsubscribe` 实例。
    ///
    /// `Parse` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字接收。
    ///
    /// `UNSUBSCRIBE` 字符串已经被消费。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Unsubscribe` 值。如果帧格式错误，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含至少一个条目的数组帧。
    ///
    /// ```text
    /// UNSUBSCRIBE [channel [channel ...]]
    /// ```
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Unsubscribe, MiniRedisParseError> {
        // 可能没有列出频道，因此以空向量开始
        let mut channels = vec![];

        // 帧中的每个条目必须是字符串，否则帧格式错误。
        // 一旦帧中的所有值都被消费，命令即被完全解析。
        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(MiniRedisParseError::EndOfStream) => break,
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 当客户端编码要发送到服务器的 `Unsubscribe` 命令时会调用此函数。
    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()))?;

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()))?;
        }

        Ok(frame)
    }
}

/// 创建取消订阅请求的响应。
pub(crate) fn make_unsubscribe_frame(
    channel_name: String,
    num_subs: usize,
) -> Result<Frame, MiniRedisParseError> {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"))?;
    response.push_bulk(Bytes::from(channel_name))?;
    response.push_int(num_subs as u64)?;
    Ok(response)
}
