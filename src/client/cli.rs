//! 最简化的 Redis 客户端实现
//!
//! 提供异步连接和发出支持的命令的方法。

use std::time::Duration;

use bytes::Bytes;
use log::{debug, error};

use crate::client::subscriber::Subscriber;
use crate::cmd::get::Get;
use crate::cmd::ping::Ping;
use crate::cmd::publish::Publish;
use crate::cmd::set::Set;
use crate::cmd::subscribe::Subscribe;
use crate::cmd::del::Del;
use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::error::MiniRedisConnectionError;

/// 与 Redis 服务器建立连接。
///
/// 由单个 `TcpStream` 支持，`Client` 提供基本的网络客户端功能（没有池化、重试等）。
/// 连接使用 [`connect`](fn@connect) 函数建立。
///
/// 请求通过 `Client` 的各种方法发出。
pub struct Client {
    /// 使用缓冲的 `TcpStream` 实现的带有 Redis 协议编码/解码器的 TCP 连接。
    ///
    /// 当 `Listener` 接收到传入连接时，`TcpStream` 被传递给 `Connection::new`，
    /// 这将初始化相关的缓冲区。
    /// `Connection` 允许处理程序在“帧”级别操作，并将字节级协议解析细节封装在 `Connection` 中。
    pub(crate) conn: Connection,
}

impl Client {
    /// 从套接字读取响应帧。
    ///
    /// 如果收到 `Error` 帧，则将其转换为 `Err`。
    pub(crate) async fn read_response(&mut self) -> Result<Frame, MiniRedisConnectionError> {
        let response = self.conn.read_frame().await?;
        debug!("read response: {:?}", response);
        match response {
            Some(Frame::Error(msg)) => Err(MiniRedisConnectionError::CommandExecute(msg)),
            Some(frame) => Ok(frame),
            // 在这里接收 `None` 表示服务器在没有发送帧的情况下关闭了连接。这是意外的，
            // 表示为“对等连接重置”错误。
            None => Err(MiniRedisConnectionError::Disconnect),
        }
    }

    /// 核心 `SET` 逻辑，由 `set` 和 `set_expires` 使用。
    async fn set_cmd(&mut self, cmd: Set) -> Result<(), MiniRedisConnectionError> {
        let frame = cmd.into_frame()?;
        debug!("set request: {:?}", frame);

        self.conn.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    /// 核心 `SUBSCRIBE` 逻辑，由各种订阅函数使用。
    pub(crate) async fn subscribe_cmd(
        &mut self,
        channels: &[String],
    ) -> Result<(), MiniRedisConnectionError> {
        // 将 `Subscribe` 命令转换为帧
        let frame = Subscribe::new(channels).into_frame()?;
        debug!("subcribe request: {:?}", frame);

         // 将帧写入套接字
        self.conn.write_frame(&frame).await?;

        // 对于每个被订阅的频道，服务器会发送一条确认订阅该频道的消息
        for channel in channels {
            let response = self.read_response().await?;
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // 服务器以如下形式响应一个数组帧：
                    //
                    // ```
                    // [ "subscribe", channel, num-subscribed ]
                    // ```
                    //
                    // 其中 channel 是频道名称，num-subscribed 是客户端当前订阅的频道数量。
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel =>
                    {
                        debug!("subscribe channel: {} success", channel);
                    }
                    _ => {
                        error!("subscribe channel failed, response: {}", response);
                        return Err(MiniRedisConnectionError::CommandExecute(
                            response.to_string(),
                        ));
                    }
                },
                frame => {
                    error!(
                        "subscribe channel failed, response frame tyep not match: {}",
                        frame
                    );
                    return Err(MiniRedisConnectionError::InvalidFrameType);
                }
            };
        }

        Ok(())
    }

    /// 向服务器发送 Ping。
    ///
    /// 如果没有提供参数，则返回 PONG，否则返回参数的副本作为批量数据。
    ///
    /// 该命令通常用于测试连接是否仍然存活，或测量延迟。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    /// ```no_run
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = mini_redis::client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    pub async fn ping(&mut self, msg: Option<String>) -> Result<Bytes, MiniRedisConnectionError> {
        let frame = Ping::new(msg).into_frame()?;
        debug!("ping request: {:?}", frame);
        self.conn.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(v) => Ok(v.into()),
            Frame::Bulk(v) => Ok(v),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    /// 获取键的值。
    ///
    /// 如果键不存在，则返回特殊值 `None`。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = mini_redis::client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("获得 = {:?}", val);
    /// }
    /// ```
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, MiniRedisConnectionError> {
        let frame = Get::new(key).into_frame()?;
        debug!("get request: {:?}", frame);

        self.conn.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(v) => Ok(Some(v.into())),
            Frame::Bulk(v) => Ok(Some(v)),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    /// 设置键的值。
    ///
    /// 该值与键关联，直到它被下次调用 `set` 覆盖或被移除。
    ///
    /// 如果键已经持有一个值，它将被覆盖。成功的 SET 操作将丢弃与键相关的任何先前的生存时间。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = mini_redis::client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set("foo", "bar".into()).await.unwrap();
    ///
    ///     // 立即获取值
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<(), MiniRedisConnectionError> {
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// 设置键的值。该值在 `expiration` 之后过期。
    ///
    /// 该值与键关联，直到以下之一：
    /// - 它过期。
    /// - 它被下次调用 `set` 覆盖。
    /// - 它被移除。
    ///
    /// 如果键已经持有一个值，它将被覆盖。成功的 SET 操作将丢弃与键相关的任何先前的生存时间。
    ///
    /// # 示例
    ///
    /// 演示基本用法。此示例不能 **保证** 始终有效，因为它依赖于基于时间的逻辑并假设客户端和服务器在时间上保持相对同步。
    /// 现实世界往往不会那么有利。
    ///
    /// ```no_run
    /// use tokio::time;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = mini_redis::client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set_expire("foo", "bar".into(), ttl).await.unwrap();
    ///
    ///     // 立即获取值
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // 等待 TTL 过期
    ///     time::sleep(ttl).await;
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    pub async fn set_expire(
        &mut self,
        key: &str,
        value: Bytes,
        epxiration: Duration,
    ) -> Result<(), MiniRedisConnectionError> {
        // 创建一个 `Set` 命令并传递给 `set_cmd`。一个单独的方法用于设置带有过期时间的值。
        // 两个函数的共同部分由 `set_cmd` 实现。
        self.set_cmd(Set::new(key, value, Some(epxiration))).await
    }

    /// 向指定的 `channel` 发布 `message`。
    ///
    /// 返回当前在频道上收听的订阅者数量。不能保证这些订阅者会收到消息，因为他们可能随时断开连接。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = mini_redis::client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.publish("foo", "bar".into()).await.unwrap();
    ///     println!("获得 = {:?}", val);
    /// }
    /// ```
    pub async fn publish(
        &mut self,
        channel: &str,
        message: Bytes,
    ) -> Result<u64, MiniRedisConnectionError> {
        // 将 `Publish` 命令转换为帧
        let frame = Publish::new(channel, message).into_frame()?;
        debug!("publish request: {:?}", frame);
        // 将帧写入套接字
        self.conn.write_frame(&frame).await?;
        // 读取响应
        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),
        }
    }

    /// 订阅客户端到指定的频道。
    ///
    /// 一旦客户端发出订阅命令，它不能再发出任何非发布/订阅命令。该函数消耗 `self` 并返回一个 `Subscriber`。
    ///
    /// `Subscriber` 值用于接收消息以及管理客户端订阅的频道列表。
    pub async fn subscribe(
        mut self,
        channels: Vec<String>,
    ) -> Result<Subscriber, MiniRedisConnectionError> {
        // 向服务器发出订阅命令并等待确认。
        // 然后客户端将被转换为“订阅者”状态，从那时起只能发出发布/订阅命令。
        self.subscribe_cmd(&channels).await?;
        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    /// 删除指定的键。  
    ///  
    /// 如果键不存在，则此操作无效。  
    ///  
    /// # 示例  
    ///  
    /// 演示基本用法。  
    ///  
    /// ```no_run  
    /// #[tokio::main]  
    /// async fn main() {  
    ///     let mut client = mini_redis::client::connect("localhost:6379").await.unwrap();  
    ///  
    ///     client.set("foo", "bar".into()).await.unwrap();  
    ///     client.del("foo").await.unwrap();  
    ///  
    ///     let val = client.get("foo").await.unwrap();  
    ///     assert!(val.is_none());  
    /// }  
    /// ```  
    pub async fn del(&mut self, key: &str) -> Result<u64, MiniRedisConnectionError> {  
        // 构造 DEL 命令的帧  
        let frame = Del::new(key).into_frame()?;
        debug!("del request: {:?}", frame);

        // 将帧写入套接字  
        self.conn.write_frame(&frame).await?;  
  
        // 读取响应  
        match self.read_response().await? {  
            Frame::Integer(deleted_cnt) => Ok(deleted_cnt), 
            frame => Err(MiniRedisConnectionError::CommandExecute(frame.to_string())),  
        }  
    }  
}
