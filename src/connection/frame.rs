//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.
//!
//! Redis serialization protocol (RESP) specification:
//!  https://redis.io/docs/reference/protocol-spec/

use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::str;

use bytes::{Buf, Bytes};

use crate::error::MiniRedisParseError;

// 定义 RESP 协议中的不同帧类型
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

// 允许 Frame 和 &str 类型比较，主要用于测试和某些特定逻辑判断
// 实现 PartialEq<&str> 特征，让 Frame 类型可以和 &str 类型进行比较
impl PartialEq<&str> for Frame {
    // eq 方法定义了如何判断 Frame 实例和 &str 是否相等
    fn eq(&self, other: &&str) -> bool {
        // 使用 match 表达式根据 Frame 实例的具体类型来决定如何比较
        match self {
            // 如果 Frame 是 Simple 类型，将包含的 String 和 other 进行比较
            Frame::Simple(s) => s.eq(other),
            // 如果 Frame 是 Bulk 类型，将包含的 Bytes 和 other 进行字符串比较
            Frame::Bulk(s) => s.eq(other),
            // 对于其他类型的 Frame，直接返回 false，因为它们不能与字符串相等
            _ => false,
        }
    }
}

// 为 Frame 枚举实现 Display trait，以提供自定义的显示（输出）格式
impl fmt::Display for Frame {
    // 必须实现 fmt 函数，它定义了如何将 Frame 格式化输出
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        // 使用 match 表达式来区分 Frame 的不同变体
        match self {
            // 对于 Simple 类型，直接调用 String 的 fmt 方法来输出
            Frame::Simple(response) => response.fmt(fmt),

            // 对于 Error 类型，输出格式为 "error: " 后跟错误消息
            Frame::Error(msg) => write!(fmt, "error: {}", msg),

            // 对于 Integer 类型，直接调用 u64 的 fmt 方法来输出
            Frame::Integer(num) => num.fmt(fmt),

            // 对于 Bulk 类型，尝试将 Bytes 解码为 UTF-8 字符串并输出
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),      // 如果解码成功，输出字符串
                Err(_) => write!(fmt, "{:?}", msg), // 如果解码失败，输出 Bytes 的调试信息
            },

            // 对于 Null 类型，输出固定的字符串 "(nil)"
            Frame::Null => "(nil)".fmt(fmt),

            // 对于 Array 类型，遍历数组中的每个元素并输出
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        // 如果不是第一个元素，先输出一个空格分隔
                        write!(fmt, " ")?;
                    }
                    part.fmt(fmt)?; // 调用每个元素的 fmt 方法进行格式化输出
                }
                Ok(()) // 当所有元素都处理完毕后，返回 Ok
            }
        }
    }
}

impl Frame {
    // 创建一个空的 Array 类型的 Frame
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    // 向 Array 类型的 Frame 添加 Bulk 类型的数据
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) -> Result<(), MiniRedisParseError> {
        match self {
            Frame::Array(vec) => {
                // 向数组中添加一个新的 Bulk 类型帧
                vec.push(Frame::Bulk(bytes));
                Ok(())
            }
            // 当前帧不是 Array 类型时返回错误
            _ => Err(MiniRedisParseError::ParseArrayFrame),
        }
    }

    // 向 Array 类型的 Frame 添加 Integer 类型的数据
    pub(crate) fn push_int(&mut self, value: u64) -> Result<(), MiniRedisParseError> {
        match self {
            Frame::Array(vec) => {
                // 向数组中添加一个新的 Integer 类型帧
                vec.push(Frame::Integer(value));
                Ok(())
            }
            // 当前帧不是 Array 类型时返回错误
            _ => Err(MiniRedisParseError::ParseArrayFrame),
        }
    }

    // 检查 src 中的数据是否可以解析为合法的 Frame
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), MiniRedisParseError> {
        match get_u8(src)? {
            // '+' 开头表示简单字符串 Simple Strings
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            // '-' 开头表示错误信息 Errors
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            // ':' 开头表示整数 Integers
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            // '$' 开头表示Bulk String
            // Bulk String 是一种数据类型，用于存储和传输二进制安全的数据。
            // 此类型数据可以包含任意字符，包括换行符和空字节（null 字符）。
            // Bulk String 是 Redis 协议的一部分，主要用于表示二进制数据或较长的字符串。
            b'$' => {
                // 处理 Null Bulk String
                if b'-' == peek_u8(src)? {
                    skip(src, 4)?;
                } else {
                    let len: usize = get_decimal(src)?.try_into()?; // 读取并解析数据长度
                    skip(src, len + 2)?; // 跳过数据和结尾的 \r\n
                }
                Ok(())
            }
            // '*' 开头表示数组 Arrays
            b'*' => {
                let len = get_decimal(src)?; // 读取数组长度
                for _ in 0..len {
                    Frame::check(src)?; // 递归检查每个元素
                }
                Ok(())
            }
            // 其他情况为非法类型
            actual => Err(MiniRedisParseError::Parse(format!(
                "protocol error; invalid frame type byte `{}`",
                actual
            ))),
        }
    }

    // 解析 src 中的数据为 Frame
    // 解析 src 中的数据为 Frame
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, MiniRedisParseError> {
        // 从字节流中读取一个字节，并根据该字节确定 RESP 类型
        match get_u8(src)? {
            // '+' 表示 RESP 简单字符串
            b'+' => {
                // 获取整行数据，转换为 Vec<u8>
                let line = get_line(src)?.to_vec();
                // 尝试将字节向量转换为 UTF-8 字符串
                let string = String::from_utf8(line)?;
                // 如果转换成功，返回 Frame::Simple 类型
                Ok(Frame::Simple(string))
            }
            // '-' 表示 RESP 错误信息
            b'-' => {
                // 同样获取整行数据，转换为 Vec<u8>
                let line = get_line(src)?.to_vec();
                // 将字节向量转换为字符串
                let string = String::from_utf8(line)?;
                // 返回 Frame::Error 类型
                Ok(Frame::Error(string))
            }
            // ':' 表示 RESP 整数
            b':' => {
                // 解析整行数据为整数
                let num = get_decimal(src)?;
                // 返回 Frame::Integer 类型
                Ok(Frame::Integer(num))
            }
            // '$' 表示 RESP Bulk 字符串
            b'$' => {
                // 首先检查是否为 Null Bulk String
                if b'-' == peek_u8(src)? {
                    // 如果是 Null Bulk String，确认格式并返回 Frame::Null
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err(MiniRedisParseError::Parse(
                            "protocol error; invalid frame format".into(),
                        ));
                    }
                    Ok(Frame::Null)
                } else {
                    // 解析 Bulk 字符串的长度
                    let len = get_decimal(src)?.try_into()?;
                    // 计算并验证数据长度
                    let n = len + 2; // 加上结尾的 \r\n
                    if src.remaining() < n {
                        return Err(MiniRedisParseError::Incomplete);
                    }
                    // 获取具体的数据部分
                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);
                    // 跳过已经读取的部分
                    skip(src, n)?;
                    // 返回 Frame::Bulk 类型
                    Ok(Frame::Bulk(data))
                }
            }
            // '*' 表示 RESP 数组
            b'*' => {
                // 解析数组的长度
                let len = get_decimal(src)?.try_into()?;
                // 创建数组容器
                let mut out = Vec::with_capacity(len);
                // 递归解析每个数组元素
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }
                // 返回 Frame::Array 类型
                Ok(Frame::Array(out))
            }
            // 其他情况为非法类型
            _ => Err(MiniRedisParseError::Unimplemented),
        }
    }
}

// 跳过 n 个字节
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), MiniRedisParseError> {
    // 检查是否有足够的字节可供跳过
    if src.remaining() < n {
        // 如果剩余字节少于需要跳过的字节，返回错误
        return Err(MiniRedisParseError::Incomplete);
    }
    // 调用 Cursor 的 advance 方法跳过 n 个字节
    src.advance(n);
    // 返回成功
    Ok(())
}

// 查看但不消耗当前字节
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, MiniRedisParseError> {
    // 检查缓冲区是否还有数据
    if !src.has_remaining() {
        // 如果没有剩余数据，返回错误
        return Err(MiniRedisParseError::Incomplete);
    }
    // 返回当前光标位置的字节，但不移动光标
    Ok(src.chunk()[0])
}

// 获取并消耗当前字节
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, MiniRedisParseError> {
    // 确保还有数据可读
    if !src.has_remaining() {
        // 如果没有数据，返回错误
        return Err(MiniRedisParseError::Incomplete);
    }
    // 读取当前字节，并自动前进光标
    Ok(src.get_u8())
}

// 解析整数值
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, MiniRedisParseError> {
    use atoi::atoi;
    // 首先获取一行数据
    let line = get_line(src)?;
    // 使用 atoi 库尝试将数据转换为 u64 类型的整数
    atoi::<u64>(line).ok_or_else(|| {
        // 如果转换失败，返回格式错误
        MiniRedisParseError::Parse("protocol error; invalid frame format to get decimal".into())
    })
}

// 获取一行数据，以 \r\n 结尾
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], MiniRedisParseError> {
    // 记录当前光标位置
    let start = src.position() as usize;
    // 获取缓冲区的长度减1（预留检查 \r\n 的位置）
    let end = src.get_ref().len() - 1;
    // 遍历缓冲区寻找 \r\n
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // 更新光标位置到 \r\n 之后
            src.set_position((i + 2) as u64);
            // 返回行数据
            return Ok(&src.get_ref()[start..i]);
        }
    }
    // 如果没有找到 \r\n，返回错误
    Err(MiniRedisParseError::Incomplete)
}
