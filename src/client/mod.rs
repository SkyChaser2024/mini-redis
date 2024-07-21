// 引入Tokio异步网络库中的TcpStream和ToSocketAddrs，用于网络通信和地址解析
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::client::cli::Client;
use crate::connection::connect::Connection;
use crate::error::MiniRedisConnectionError;

pub mod cli;
pub mod cmd;
mod subscriber; // 订阅者模块，内部使用，因此不公开

// 定义异步函数 connect，用于创建与 Redis 服务器的连接
// 泛型参数T必须实现 ToSocketAddrs 特质，允许传入多种类型的地址
pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client, MiniRedisConnectionError> {
    // `addr` 参数直接传递给 `TcpStream::connect`。这将执行任何异步 DNS 查找并尝试建立 TCP 连接。
    // 任一步骤中的错误都会返回一个错误，然后会被传递给调用 `mini_redis` 连接的调用者。
    let socket = TcpStream::connect(addr).await?;

    // 初始化连接状态。这会分配读/写缓冲区以执行 redis 协议帧解析
    let conn = Connection::new(socket);

    // 返回Client实例，包含已建立的连接
    Ok(Client { conn })
}