// 引入需要使用的标准库模块
use std::sync::{Arc, Mutex};

// 引入字节流库
use bytes::Bytes;
// 引入日志库中的info宏
use log::info;
// 引入Tokio异步库中的广播和通知模块
use tokio::sync::{broadcast, Notify};
// 引入Tokio异步库中的时间相关模块
use tokio::time::{self, Duration, Instant};

// 引入store.rs中的Store结构体
use crate::storage::store::Store;
// 引入traits.rs中的KvStore特性
use crate::storage::traits::KvStore;

// Debug 和 Clone trait 都自动实现
#[derive(Debug, Clone)]
// 使用Arc共享指向SharedDb结构体的引用
pub(crate) struct Db {
    shared: Arc<SharedDb>,
}

// 实现Db结构体
impl Db {
    /// 创建一个新的 `Db` 实例。
    pub(crate) fn new() -> Db {
        // 创建一个新的SharedDb实例，并使用Arc进行包裹
        let shared = Arc::new(SharedDb::new());
        // 使用Tokio异步库启动一个任务来清理过期键
        tokio::spawn(Db::purge_expired_tasks(shared.clone()));

        // 返回创建好的Db实例
        Db { shared }
    }

    /// 异步任务：清理过期键的方法。
    async fn purge_expired_tasks(shared: Arc<SharedDb>) {
        // 当共享的数据库未关闭时
        while !shared.is_shutdown() {
            // 如果有过期键需要清理
            if let Some(when) = shared.purge_expired_keys() {
                tokio::select! {
                    // 等待直到指定的时间
                    _ = time::sleep_until(when) => {}
                    // 或者等待后台任务通知
                    _ = shared.background_task.notified() => {}
                }
            } else {
                // 如果没有过期键需要清理，则等待后台任务的通知
                shared.background_task.notified().await;
            }
        }
        // 打印清理任务关闭的日志信息
        info!("Purge background task shut down");
    }

    /// 关闭清理任务的方法。
    fn shutdown_purge_task(&self) {
        // 获取存储层的互斥锁来修改共享数据
        let mut store = self.shared.store.lock().unwrap();
        // 设置存储层为关闭状态
        store.set_shutdown(true);

        // 释放锁
        drop(store);
        // 通知后台任务
        self.shared.background_task.notify_one();
    }
}

// 实现KvStore特性为Db
impl KvStore for Db {
    /// 获取指定键的值。
    fn get(&self, key: &str) -> Option<Bytes> {
        // 获取存储层的互斥锁
        let store = self.shared.store.lock().unwrap();
        // 调用存储层的get方法获取键的值
        store.get(key)
    }

    /// 设置键值对和可选的过期时间。
    fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        // 获取存储层的互斥锁
        let mut store = self.shared.store.lock().unwrap();
        // 调用存储层的set方法设置键值对
        let notify = store.set(key, value, expire);

        // 释放存储层的互斥锁
        drop(store);

        // 如果需要通知后台任务，则发送通知
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    /// 删除指定键
    fn del(&self, key: String) -> usize{
        let mut store = self.shared.store.lock().unwrap();
        store.del(&key)
    }

    /// 订阅指定键的消息。
    fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        // 获取存储层的互斥锁
        let mut store = self.shared.store.lock().unwrap();
        // 调用存储层的subscribe方法订阅消息
        store.subscribe(key)
    }

    /// 发布指定键的消息。
    fn publish(&self, key: &str, value: Bytes) -> usize {
        // 获取存储层的互斥锁
        let state = self.shared.store.lock().unwrap();
        // 调用存储层的publish方法发布消息
        state.publish(key, value)
    }
}

// SharedDb结构体定义
#[derive(Debug)]
struct SharedDb {
    // 存储层的互斥锁
    store: Mutex<Store>,
    // 后台任务的通知机制
    background_task: Notify,
}

// 实现SharedDb
impl SharedDb {
    /// 创建一个新的 `SharedDb` 实例。
    fn new() -> Self {
        // 初始化存储层
        SharedDb {
            store: Mutex::new(Store::new()),
            // 初始化后台任务的通知
            background_task: Notify::new(),
        }
    }

    /// 清理过期的键。
    fn purge_expired_keys(&self) -> Option<Instant> {
        // 获取存储层的互斥锁
        let mut store = self.store.lock().unwrap();
        // 调用存储层的purge_expired_keys方法清理过期的键
        store.purge_expired_keys()
    }

    /// 检查存储层是否已关闭。
    fn is_shutdown(&self) -> bool {
        // 调用存储层的is_shutdown方法检查是否已关闭
        self.store.lock().unwrap().is_shutdown()
    }
}

// DbDropGuard结构体定义
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// 当这个 `DbDropGuard` 结构体被回收（dropped）时，将关闭的 `Db` 实例。
    db: Db,
}

// 实现DbDropGuard
impl DbDropGuard {
    /// 创建一个新的 `DbDropGuard`，封装一个 `Db` 实例。
    /// 当这个 `DbDropGuard` 被回收（dropped）时，将关闭 `Db` 的过期键清理任务。
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// 获取共享的数据库实例。内部实际上是一个 `Arc`，所以克隆操作只会增加引用计数。
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

// 实现Drop特性为DbDropGuard
impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 通知 `Db` 实例关闭清理过期键的任务
        self.db.shutdown_purge_task();
    }
}
