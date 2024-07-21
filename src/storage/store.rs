use bytes::Bytes; // 导入字节流Bytes类型
use std::collections::{BTreeMap, HashMap}; // 导入BTreeMap和HashMap类型
use tokio::sync::broadcast; // 导入tokio异步广播通道类型
use tokio::time::{Duration, Instant}; // 导入tokio时间相关类型

#[derive(Debug)]
pub(crate) struct Store {
    // 结构体的一个字段叫entries, 它用于存放 k-v 的数据。
    entries: HashMap<String, Entry>,
    // 键是String，值是bytes类型的消息广播发送者。其用于存放 pub-sub 数据。
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    // 键是一个元组，包含Instant和u64类型，值是String。元组并按顺序排列。类似于优先队列，便于快速扫描过期键并移除。
    expirations: BTreeMap<(Instant, u64), String>,
    // u64类型字段用于存储下一个id。对每一个过期键分配的 id，避免找不到对应键。
    next_id: u64,
    // bool类型字段用于指示是否关闭数据库。如果数据库关闭，则此时不再接受请求，但需要释放连接等资源。
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    id: u64,                     // 条目ID
    data: Bytes,                 // 数据字节流
    expires_at: Option<Instant>, // 过期时间点，可选
}

impl Store {
    // 创建新的Store实例
    pub(crate) fn new() -> Store {
        Store {
            entries: HashMap::new(),      // 初始化键值对存储
            pub_sub: HashMap::new(),      // 初始化订阅频道存储
            expirations: BTreeMap::new(), // 初始化过期时间映射
            next_id: 0,                   // 初始ID为0
            shutdown: false,              // 初始未关闭
        }
    }

    // 获取下一个过期时间点
    pub(crate) fn next_expiration(&self) -> Option<Instant> {
        // 使用BTreeMap的keys()方法获取所有的键（即所有的过期时间点），然后调用next()获取第一个键（最早的过期时间点），
        // 如果存在则使用map()方法处理，返回其第一个元素的第一个元素，即Instant类型的过期时间点。
        self.expirations.keys().next().map(|expire| expire.0)
    }

    // 获取指定键的值
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 使用HashMap的get()方法根据键获取对应的值，如果存在则使用map()方法处理，
        // 返回其Entry中的data字段的克隆，即Bytes类型的值的拷贝。
        self.entries.get(key).map(|entry| entry.data.clone())
    }

    // 设置键值对和可选的过期时间
    pub(crate) fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) -> bool {
        // 获取当前的唯一标识符，并立即为下一个键值对递增
        let id = self.next_id;
        self.next_id += 1;

        // 初始化一个标志变量，用于指示是否需要通知过期任务
        let mut notify = false;

        // 处理可选的过期时间
        let expires_at = expire.map(|duration| {
            // 计算过期时间点
            let when = Instant::now() + duration;

            // 判断是否需要更新过期时间处理队列，如果当前设置的过期时间早于队列中最早的过期时间，或者队列为空，需要通知
            notify = self
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            // 在过期时间映射中插入新的过期时间和键值标识符
            self.expirations.insert((when, id), key.clone());
            when // 返回设置的过期时间点
        });

        // 插入新的键值对到HashMap，如果该键之前存在，则返回之前的值
        let prev = self.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            },
        );

        // 如果之前的键存在且有设置过期时间，则从过期时间映射中删除之前的过期信息
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                self.expirations.remove(&(when, prev.id));
            }
        }

        // 返回是否需要通知过期任务的标志
        notify
    }

    // 删除指定的键  
    pub(crate) fn del(&mut self, key: &str) -> usize {  
        // 尝试从entries中移除键，并获取移除的结果  
        let removed_entry = self.entries.remove(key);  
  
        // 如果成功移除了键，则还需要从expirations中移除相关的过期信息  
        if let Some(entry) = removed_entry {  
            if let Some(expires_at) = entry.expires_at {  
                self.expirations.remove(&(expires_at, entry.id));  
            }  
            1 // 返回true表示成功移除了键  
        } else {  
            0 // 返回false表示键不存在，未进行移除操作  
        }  
    }  

    // 订阅指定键的消息
    pub(crate) fn subscribe(&mut self, key: String) -> broadcast::Receiver<Bytes> {
        // 引入hash_map模块中的Entry枚举
        use std::collections::hash_map::Entry;

        // 根据键在pub_sub哈希表中查找对应的条目
        match self.pub_sub.entry(key) {
            // 如果Entry::Occupied表示该键已经存在，则返回对应的广播发送者的订阅接收者
            Entry::Occupied(e) => e.get().subscribe(),
            // 如果Entry::Vacant表示该键不存在，则创建一个新的广播频道，将发送者存入哈希表，并返回对应的接收者
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(1024); // 创建一个新的广播频道，容量为1024
                e.insert(tx); // 将新创建的发送者存入哈希表
                rx // 返回新创建的接收者
            }
        }
    }

    // 发布指定键的消息
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        // 根据键在pub_sub哈希表中查找对应的广播发送者
        self.pub_sub
            .get(key)
            .map(|tx| tx.send(value).unwrap_or(0)) // 如果找到发送者，则发送消息并返回发送成功的接收者数量；如果发送失败，则返回0
            .unwrap_or(0) // 如果找不到对应的发送者，则返回0
    }

    // 清理过期键
    pub(crate) fn purge_expired_keys(&mut self) -> Option<Instant> {
        // 如果存储层已经关闭，则返回None，表示不执行过期清理操作
        if self.shutdown {
            return None;
        }

        let now = Instant::now(); // 获取当前时间点的Instant对象
        while let Some((&(when, id), key)) = self.expirations.first_key_value() {
            // 如果最早的过期时间大于当前时间，则返回该过期时间点，表示暂时不需要清理
            if when > now {
                return Some(when);
            }

            // 否则，从entries和expirations中移除过期键对应的条目
            self.entries.remove(key);
            self.expirations.remove(&(when, id));
        }

        None // 清理完成后返回None，表示没有需要清理的过期键
    }

    // 设置关闭标志
    pub(crate) fn set_shutdown(&mut self, value: bool) {
        self.shutdown = value;
    }

    // 检查是否已关闭
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown
    }
}
