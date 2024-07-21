use std::vec;

use bytes::Bytes;

use crate::connection::frame::Frame;
use crate::error::MiniRedisParseError;

#[derive(Debug)]
pub(crate) struct Parse {
    /// Array frame iterator.
    /// 保存 Frame::Array 内部的迭代器，允许顺序访问每个 Frame。
    parts: vec::IntoIter<Frame>,
}

impl Parse {
    /// 创建一个新的 `Parse` 来解析 `frame` 的内容。
    /// 如果 `frame` 不是数组帧，则返回错误。
    pub(crate) fn new(frame: Frame) -> Result<Parse, MiniRedisParseError> {
        // 使用 match 表达式来检查传入的 `frame` 是否为 `Frame::Array` 类型
        let array = match frame {
            // 如果 `frame` 是数组类型，直接使用这个数组
            Frame::Array(array) => array,
            // 如果 `frame` 不是数组类型，则返回一个错误
            frame => {
                return Err(MiniRedisParseError::Parse(format!(
                    "protocol error; expected array, got {:?}",
                    frame
                )))
            }
        };

        // 如果成功，使用数组的迭代器初始化 `Parse` 结构体
        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// 返回下一个帧。数组帧是帧的数组，因此下一个条目是一个帧。
    fn next(&mut self) -> Result<Frame, MiniRedisParseError> {
        // 尝试从迭代器中获取下一个元素
        self.parts.next().ok_or(MiniRedisParseError::EndOfStream)
    }

    /// 从 Parse 对象中提取下一个帧，并尝试将其解析为字符串。
    /// 如果下一个帧不是字符串或不能被解析为有效的字符串，则返回错误。
    pub(crate) fn next_string(&mut self) -> Result<String, MiniRedisParseError> {
        // 使用 match 表达式处理从 Parse 中获取的下一个 Frame
        match self.next()? {
            // 如果下一个 Frame 是简单字符串（Simple），直接返回这个字符串
            Frame::Simple(s) => Ok(s),

            // 如果下一个 Frame 是批量字符串（Bulk）
            Frame::Bulk(data) => {
                // 尝试将批量字符串的字节数据解码为 UTF-8 字符串
                std::str::from_utf8(&data[..])
                    // 如果解码成功，转换为 String 类型并返回
                    .map(|s| s.to_string())
                    // 如果解码失败（因为数据不是有效的 UTF-8），返回解析错误
                    .map_err(|_| {
                        MiniRedisParseError::Parse("protocol error; invalid string".into())
                    })
            }

            // 如果下一个 Frame 不是期待的简单或批量字符串类型
            frame => Err(MiniRedisParseError::Parse(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            ))),
        }
    }

    /// 返回下一个条目作为原始字节。
    /// 如果下一个条目不能表示为原始字节，返回错误。
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, MiniRedisParseError> {
        match self.next()? {
            // 如果下一个帧是简单字符串类型，转换字符串为字节并返回
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),

            // 如果下一个帧是批量字符串类型，直接返回包含的字节
            Frame::Bulk(data) => Ok(data),

            // 如果下一个帧不是期望的简单或批量字符串类型，返回错误
            frame => Err(MiniRedisParseError::Parse(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            ))),
        }
    }

    /// 返回下一个条目作为整数。
    /// 包括 `Simple`、`Bulk` 和 `Integer` 类型的帧，`Simple` 和 `Bulk` 类型需要解析。
    /// 如果下一个条目不能表示为整数，则返回错误。
    pub(crate) fn next_int(&mut self) -> Result<u64, MiniRedisParseError> {
        use atoi::atoi; // 使用 atoi 库来转换字符串为整数

        match self.next()? {
            // 如果下一个帧本身就是整数类型，直接返回这个整数
            Frame::Integer(v) => Ok(v),

            // 如果下一个帧是简单字符串或批量字符串类型，尝试解析为整数
            Frame::Simple(data) => atoi::<u64>(data.as_bytes())
                .ok_or_else(|| MiniRedisParseError::Parse("protocol error; invalid number".into())),
            Frame::Bulk(data) => atoi::<u64>(&data)
                .ok_or_else(|| MiniRedisParseError::Parse("protocol error; invalid number".into())),

            // 如果下一个帧不是期望的整数、简单字符串或批量字符串类型，返回错误
            frame => Err(MiniRedisParseError::Parse(format!(
                "protocol error; expected int frame but got {:?}",
                frame
            ))),
        }
    }

    /// 确保数组中没有更多条目
    pub(crate) fn finish(&mut self) -> Result<(), MiniRedisParseError> {
        // 尝试从迭代器中获取下一个帧
        if self.parts.next().is_none() {
            // 如果没有更多的帧，表示正常结束，返回 Ok
            Ok(())
        } else {
            // 如果还有额外的帧，表示数据超出了预期，返回错误
            Err(MiniRedisParseError::Parse(
                "protocol error; expected end of frame, but there was more".into(),
            ))
        }
    }
}
