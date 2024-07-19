use std::future::Future;
use std::sync::Arc;
use log::{debug, error, info};

use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::consts::MAX_CONNECTIONS;
use crate::server::listener::Listener;
use crate::storage::db::DbDropGuard;

mod handler;
// 只在当前 crate 模块中可见
pub(crate) mod listener;
pub(crate) mod shutdown;


/// 运行 mini-redis 服务器。
///
/// 这个函数启动 mini-redis 服务器并在提供的 `TcpListener` 上监听传入的连接
/// 它还接受一个 `shutdown` future 作为参数，该参数可以用来地关闭服务器
///
/// # 参数
///
/// * `listener` - 用于监听传入连接的 `TcpListener`
/// * `shutdown` - 表示服务器关闭信号的 future (我们默认使用 `tokio::signal::ctrl_c`)
/// ```
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 打印服务器启动信息，监听的地址和端口
    info!("mini-redis server start, listening on: {}", listener.local_addr().unwrap());

    // 创建一个广播通道，用于通知关闭
    let (notify_shutdown, _) = broadcast::channel(1);
    // 创建一个消息通道，用于完成关闭（缓冲 1 条消息）
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // 创建一个 Listener 实例
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_conn: Arc::new(Semaphore::new(MAX_CONNECTIONS)), // 最多允许 MAX_CONNECTIONS 个连接
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    // 使用 tokio 的 select 宏来同时运行 server 和监听 shutdown 信号
    tokio::select! {
        res = server.run() => {
            if let Err(e) = res {
                // server 运行出错
                error!("server error: {:?}", e);
            }
        }

        _ = shutdown => {
            // 接收到关闭信号
            debug!("server shutdown");
        }
    }

    // 解构 Listener 实例，获取需要的字段（是个不错的写法）
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

   // 丢弃 notify_shutdown 和 shutdown_complete_tx，以便它们可以被正确关闭，此时其他 TCP 连接也能够接收到 shutdown 信号
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    // 等待关闭完成
    let _ = shutdown_complete_rx.recv().await;
}