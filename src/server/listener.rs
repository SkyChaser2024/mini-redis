use std::sync::Arc;
use std::time::Duration;

use log::{error, info};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time;

use crate::connection::connect::Connection;
use crate::error::MiniRedisConnectionError;
use crate::server::handler::Handler;
use crate::server::shutdown::Shutdown;
use crate::storage::db::DbDropGuard;

/// `Listener` 结构体负责监听TCP连接，并管理与每个连接相关的资源。
#[derive(Debug)]
pub(crate) struct Listener {
    pub(crate) listener: TcpListener, // 监听 TCP 连接
    pub(crate) db_holder: DbDropGuard, //内部存储数据库
    pub(crate) limit_conn: Arc<Semaphore>, // 使用信号量 Semaphore 实现的连接令牌，当超过了最大连接数，则需要等待其他连接释放后才能创建新的连接
    pub(crate) notify_shutdown: broadcast::Sender<()>, // 通知所有 TCP 服务器 shutdown 信号
    pub(crate) shutdown_complete_tx: mpsc::Sender<()>, // 用于发送服务器 shutdown 完成信号的发送器
    pub(crate) shutdown_complete_rx: mpsc::Receiver<()>, // 用于接收服务器 shutdown 完成信号的接收器
}

impl Listener {
    /// 运行服务器
    ///
    /// 监听入站连接。对于每个入站连接，生成一个任务来处理该连接。
    ///
    /// # 错误
    ///
    /// 如果接受返回错误，则返回 `Err`。这种情况可能由于许多原因而发生，这些原因会随着时间的推移而解决。例如，如果底层
    /// 操作系统已达到最大套接字数的内部限制，则 accept 将失败。
    ///
    /// 进程无法检测瞬态错误何时自行解决。处理此问题的一种策略是实施 back off 策略，这就是我们在这里所做的。
    pub(crate) async fn run(&mut self) -> Result<(), MiniRedisConnectionError> {
        info!("accepting inbound connections");
        // 等待 permit 可用
        //
        // `acquire_owned` 返回与信号量绑定的许可证。
        // 当许可证值被删除时，它会自动返回
        // 到信号量。
        //
        // 当信号量已关闭时，`acquire_owned()` 返回 `Err`。我们永远不会关闭信号量，因此 `unwrap()` 是安全的。
        loop {
            let permit = self
                .limit_conn
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            
            // 接收一个连接（调用下面实现的 accept 函数）
            let socket = self.accept().await?;
            
            // 创建一个新的 Handler 来处理连接
            let mut handler = Handler {
                db: self.db_holder.db(),
                conn: Connection::new(socket),
                // shutdown 信号通知
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                // 当所有 clone drop 时，通知接收者
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // 生成一个新的任务来处理连接
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!("connection error: {:?}", err);
                }
                // 释放 permit
                drop(permit);
            });
        }
    }

    /// 接受入站连接。
    ///
    /// 通过 back off 和 retry 来处理错误。使用 exponential backoff 策略。
    /// 即第一次失败后，任务等待 1 秒。第二次失败后，任务等待 2 秒。
    /// 后续每次失败都会使等待时间加倍。如果在等待 64 秒后即第 6 次尝试接受失败，则此函数返回 error。
    async fn accept(&mut self) -> Result<TcpStream, MiniRedisConnectionError> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    return Ok(socket);
                }
                Err(err) => {
                    if backoff > 64 {
                        error!("Accept has failed too many times. Error: {}", err);
                        return Err(err.into());
                    }
                    else {
                        error!("failed to accept socket. Error: {}", err);
                    }
                }
            }

            // 等待一段时间后重试，时间随重试次数指数增长
            time::sleep(Duration::from_secs(backoff)).await;
            
            // double
            backoff *= 2;
        }
    }
}