use bytes::Bytes; // 引入 bytes crate 中的 Bytes 类型
use std::time::Duration; // 引入标准库中的 Duration 类型
use tokio::sync::broadcast; // 引入 tokio crate 中的 broadcast 模块

// KvStore trait 定义了键值存储的基本行为
pub trait KvStore {
    // 根据给定的键返回关联的值，如果键不存在，则返回 None。
    // # 参数`key`: 要查找的键的引用
    // # 返回一个 Option 类型，如果键存在，则返回 Some 包含的 Bytes 值，否则返回 None。
    fn get(&self, key: &str) -> Option<Bytes>;

    // 设置与键关联的值以及一个可选的过期时间。
    // 如果键已经存在，则旧的值会被移除。
    // # 参数
    // - `key`: 要设置的键，类型为 String
    // - `value`: 要存储的值，类型为 Bytes
    // - `expire`: 可选的过期时间，类型为 Option<Duration>
    fn set(&self, key: String, value: Bytes, expire: Option<Duration>);

    // 返回一个接收者，用于接收指定频道的消息。
    // 返回的 `Receiver` 用于接收由 `PUBLISH` 命令广播的值。
    // # 参数- `key`: 订阅的频道，类型为 String
    // # 返回返回一个广播接收者，用于接收广播的 Bytes 值。
    fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes>;

    // 向频道发布消息。返回当前监听该频道的订阅者数量。
    // # 参数
    // - `key`: 发布消息的频道，类型为 &str
    // - `value`: 要发布的消息，类型为 Bytes
    // 返回一个 usize 类型，表示监听该频道的订阅者数量。
    fn publish(&self, key: &str, value: Bytes) -> usize;

    // 删除指定的键。
    // # 参数
    // - `key`: 键的名称，类型为 String
    // 返回一个 usize 类型，表示删除的数量。
    fn del(&self, key: String) -> usize;
}
