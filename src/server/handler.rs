use log::debug; // 用于日志记录
use tokio::sync::mpsc; // 异步消息传递

use crate::cmd::Command; // 命令处理模块
use crate::connection::connect::Connection; // 连接处理模块
use crate::error::MiniRedisConnectionError; // 错误处理模块
use crate::server::shutdown::Shutdown; // 服务器关闭处理模块
use crate::storage::db::Db; // 数据库处理模块

/// 每个连接的处理器。从 `connection` 读取请求并将命令应用到 `db`。
#[derive(Debug)]
pub(crate) struct Handler {
    /// 共享的数据库句柄。
    ///
    /// 当从 `connection` 收到命令时，它会使用 `db` 应用。
    /// 命令的实现位于 `cmd` 模块中。每个命令都需要与 `db` 交互以完成工作。
    pub(crate) db: Db,

    /// 使用 redis 协议编码器/解码器装饰的 TCP 连接，通过缓冲的 `TcpStream` 实现。
    ///
    /// 当 `Listener` 收到一个入站连接时，`TcpStream` 会被传递给 `Connection::new`，
    /// 它会初始化相关的缓冲区。`Connection` 允许处理器在 "帧" 级别操作，并将字节级别的协议解析细节封装在 `Connection` 中。
    pub(crate) conn: Connection,

    /// 监听关闭通知。
    ///
    /// 包装 `broadcast::Receiver` 与 `Listener` 中的发送者配对。
    /// 连接处理器处理来自连接的请求，直到对等方断开连接**或**从 `shutdown` 接收到关闭通知。
    /// 在后者情况下，任何正在处理的对等方工作都会继续直到达到安全状态，然后终止连接。
    pub(crate) shutdown: Shutdown,

    /// 服务器关闭完成通知的发送端
    pub(crate) _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    /// 异步运行处理器。
    ///
    /// 循环等待并处理来自连接的命令，直到接收到关闭信号。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Ok(())`，失败时返回 `MiniRedisConnectionError`。
    pub(crate) async fn run(&mut self) -> Result<(), MiniRedisConnectionError> {
        // 当未接收到关闭信号时循环
        while !self.shutdown.is_shutdown() {
            // 异步等待读取帧或接收关闭信号
            let maybe_frame = tokio::select! {
                res = self.conn.read_frame() => res?, // 读取帧
                _ = self.shutdown.recv() => { // 接收关闭信号
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                // 如果没有帧可读，则返回成功
                None => {
                    debug!("peer closed the socket, return");
                    return Ok(());
                }
            };

            // 从帧中解析命令
            let cmd = Command::from_frame(frame)?;
            // 记录接收到的命令
            debug!("received command: {:?}", cmd);
            // 应用命令到数据库和连接
            cmd.apply(&self.db, &mut self.conn, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
