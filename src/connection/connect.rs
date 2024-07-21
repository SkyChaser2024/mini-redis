use std::io::Cursor;

use bytes::{Buf, BytesMut};
use log::warn;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::connection::frame::Frame;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};

//// 从远程对等方发送和接收 `Frame` 值。
///
/// 在实现网络协议时，协议中的消息通常由几个较小的消息组成，称为帧。`Connection` 的目的是在底层的 `TcpStream` 上读取和写入帧。
///
/// 为了读取帧，`Connection` 使用内部缓冲区，直到有足够的字节来创建一个完整的帧。一旦完成，`Connection` 创建帧并将其返回给调用者。
///
/// 当发送帧时，帧首先被编码到写缓冲区中。然后，写缓冲区的内容被写入到套接字中。
#[derive(Debug)]
pub struct Connection {
    /// `TcpStream`。它被 `BufWriter` 装饰，提供写入级别的缓冲。
    /// Tokio 提供的 `BufWriter` 实现满足我们的需求。
    stream: BufWriter<TcpStream>,

    /// 读取帧的缓冲区。
    buffer: BytesMut,
}

impl Connection {
    /// 创建一个新的 `Connection` 实例。
    ///
    /// # 参数
    /// * `socket` - 一个已经建立的 TCP 连接。
    ///
    /// # 返回
    /// 返回一个包含缓冲区和流的 `Connection` 实例。
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            // 使用 BufWriter 包装 TcpStream 以提供写缓冲功能。
            stream: BufWriter::new(socket),
            // 初始化一个 4KB 的缓冲区用于读取数据。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 异步读取数据并解析为 `Frame`。
    ///
    /// # 返回
    /// 如果成功，返回解析出的 `Frame`；如果远程关闭连接且没有剩余数据，则返回 `None`。
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, MiniRedisConnectionError> {
        loop {
            // 尝试从缓冲区解析一个帧，如果成功则返回帧。
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 如果缓冲区中的数据不足以解析一个帧，则从流中读取更多数据。
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 远程关闭了连接。如果缓冲区中没有数据，则正常关闭，否则返回断开连接错误。
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(MiniRedisConnectionError::Disconnect)
                };
            }
        }
    }

    /// 解析缓冲区中的数据为 `Frame`。
    ///
    /// # 返回
    /// 如果成功，返回解析出的 `Frame`；如果数据不足，返回 `None`。
    fn parse_frame(&mut self) -> Result<Option<Frame>, MiniRedisConnectionError> {
        // 创建一个 Cursor 以便在缓冲区中移动和读取数据。
        let mut buf = Cursor::new(&self.buffer[..]);

        // 调用 Frame::check 检查缓冲区中是否有完整的帧。
        match Frame::check(&mut buf) {
            Ok(_) => {
                // 获取当前 buf 的位置，表示帧的长度。
                let len = buf.position() as usize;

                // 将 cursor 的位置重置为起始位置。
                buf.set_position(0);

                // 调用 Frame::parse 解析帧。
                let frame = Frame::parse(&mut buf)?;

                // 移动缓冲区的起始位置，丢弃已经解析的数据。
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            // 如果数据不足以构成一个完整的帧，则返回 None。
            Err(MiniRedisParseError::Incomplete) => Ok(None),
            // 其他错误则直接返回。
            Err(e) => Err(e.into()),
        }
    }

    /// 异步写入 `Frame` 数据到 TCP 流。
    ///
    /// # 参数
    /// * `frame` - 要写入的 `Frame` 数据。
    ///
    /// # 返回
    /// 如果成功，返回 `Ok(())`。
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), MiniRedisConnectionError> {
        // 根据帧的类型进行处理。
        match frame {
            // 如果是数组类型
            Frame::Array(val) => {
                // 写入数组类型的标识符 `*`
                self.stream.write_u8(b'*').await?;

                // 写入数组的长度
                self.write_decimal(val.len() as u64).await?;

                // 遍历数组中的每个元素并写入
                for entry in val {
                    self.write_value(entry).await?;
                }
            }
            // 其他类型的帧
            _ => self.write_value(frame).await?,
        }

        // 刷新缓冲区，将数据真正发送到网络中。
        self.stream.flush().await.map_err(|e| e.into())
    }

    /// 根据 `Frame` 类型写入具体数据。
    ///
    /// # 参数
    /// * `frame` - 要写入的 `Frame` 数据。
    ///
    /// # 返回
    /// 如果成功，返回 `Ok(())`。
    async fn write_value(&mut self, frame: &Frame) -> Result<(), MiniRedisConnectionError> {
        // 使用 match 语句根据 frame 的类型进行处理
        match frame {
            // 写入简单字符串
            Frame::Simple(val) => {
                // 写入简单字符串类型的标识符 `+`
                self.stream.write_u8(b'+').await?;
                // 写入字符串的内容
                self.stream.write_all(val.as_bytes()).await?;
                // 写入结尾标识 `\r\n`
                self.stream.write_all(b"\r\n").await?;
            }
            // 写入错误信息
            Frame::Error(val) => {
                // 写入错误信息类型的标识符 `-`
                self.stream.write_u8(b'-').await?;
                // 写入错误信息的内容
                self.stream.write_all(val.as_bytes()).await?;
                // 写入结尾标识 `\r\n`
                self.stream.write_all(b"\r\n").await?;
            }
            // 写入整数
            Frame::Integer(val) => {
                // 写入整数类型的标识符 `:`
                self.stream.write_u8(b':').await?;
                // 写入整数值
                self.write_decimal(*val).await?;
            }
            // 写入空值
            Frame::Null => {
                // 写入表示空值的特殊标识 `$-1\r\n`
                self.stream.write_all(b"$-1\r\n").await?;
            }
            // 写入批量字符串
            Frame::Bulk(val) => {
                // 获取字符串的长度
                let len = val.len();
                // 写入批量字符串类型的标识符 `$`
                self.stream.write_u8(b'$').await?;
                // 写入字符串的长度
                self.write_decimal(len as u64).await?;
                // 写入字符串的内容
                self.stream.write_all(val).await?;
                // 写入结尾标识 `\r\n`
                self.stream.write_all(b"\r\n").await?;
            }
            // 数组类型目前不支持递归写入，直接返回未实现错误
            Frame::Array(_val) => {
                // 记录警告信息
                warn!("unreachable code: recursive write_value: {:?}", _val);
                // 返回未实现错误
                return Err(MiniRedisParseError::Unimplemented.into());
            }
        }

        // 所有写入操作成功后，返回 Ok(())
        Ok(())
    }

    /// 异步地将十进制数值写入 TCP 流。
    ///
    /// # 参数
    /// * `val` - 要写入的十进制数值。
    ///
    /// # 返回
    /// 如果成功，返回 `Ok(())`。
    async fn write_decimal(&mut self, val: u64) -> Result<(), MiniRedisConnectionError> {
        // 引入 std::io::Write trait 以便使用其提供的写入方法。
        use std::io::Write;

        // 创建一个长度为20的字节数组缓冲区，用于存储转换后的字符串。
        let mut buf = [0u8; 20];
        // 将缓冲区包装成 Cursor，以便在其上执行写入操作。
        let mut buf = Cursor::new(&mut buf[..]);

        // 将十进制数值转换为字符串并写入缓冲区。
        // write! 宏用于格式化输出，将 val 作为十进制数值写入 buf 中。
        write!(&mut buf, "{}", val)?;

        // 获取当前 Cursor 的位置，该位置表示写入的数据长度。
        let pos = buf.position() as usize;
        // 将缓冲区中的有效内容（从起始位置到当前 Cursor 位置）写入到 TCP 流中。
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        // 写入结尾标识符 `\r\n` 到 TCP 流中，以表示结束。
        self.stream.write_all(b"\r\n").await?;

        // 返回 Ok(()) 表示写入操作成功完成。
        Ok(())
    }
}