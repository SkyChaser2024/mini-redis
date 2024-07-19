use tokio::sync::broadcast;

/// 监听服务器 shutdown 信号。
///
/// 使用 `broadcast::Receiver` 发出关闭信号。只发送一个值。一旦通过广播通道发送了一个值，服务器就应该关闭。
///
/// `Shutdown` 结构监听信号并跟踪信号是否已收到。调用者可以查询是否已收到关闭信号。
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` 表示已收到关闭信号。
    is_shutdown: bool,
    /// 用于接收关闭信号的广播接收器。
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// 创建一个新的 `Shutdown` 实例。
    ///
    /// # 参数
    ///
    /// * `notify` - 一个 `broadcast::Receiver<()>`，用于接收关闭通知。
    ///
    /// # 返回值
    ///
    /// 返回一个 `Shutdown` 实例。
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// 检查服务器是否已经接收到 shutdown 信号。
    ///
    /// # 返回值
    ///
    /// 如果服务器已经接收到 shutdown 信号，则返回 `true`；否则返回 `false`。
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// 异步等待接收关闭通知。
    ///
    /// 当接收到关闭通知后，将 `shutdown` 标记设置为 `true`，表示服务器已经开始关闭过程。
    /// 如果 `shutdown` 已经是 `true`，则此方法将直接返回，不会等待接收新的通知。
    pub(crate) async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }
        
        // 由于仅发送一个值，因此无法接收 "lag error"
        self.notify.recv().await.ok();
        // 以及收到了 shutdown 信号
        self.is_shutdown = true;
    }
}