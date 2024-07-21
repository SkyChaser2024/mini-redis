// 引入标准库中的错误处理和时间处理模块
use std::num::ParseIntError;
use std::time::Duration;

use bytes::Bytes; // 引入bytes库中的Bytes类型，用于处理原始二进制数据
use clap::Subcommand; // 引入clap库中的Subcommand特性，用于解析命令行子命令

// 定义命令枚举，用于表示不同的命令操作
#[derive(Subcommand, Debug)]
pub enum Command {
    // Ping命令，可选地接受一个消息字符串
    Ping {
        msg: Option<String>,
    },
    // Get命令，需要一个键名字符串
    Get {
        key: String,
    },
    // Set命令，需要一个键名字符串，一个值（以Bytes类型存储），以及可选的过期时间
    Set {
        key: String,

        // 使用自定义函数bytes_from_str将字符串解析为Bytes类型
        #[clap(parse(from_str = bytes_from_str))]
        value: Bytes,

        // 使用自定义函数duration_from_ms_str尝试从字符串解析出Duration类型
        #[clap(parse(try_from_str = duration_from_ms_str))]
        expire: Option<Duration>,
    },
    // Publish命令，需要一个频道名字符串和一个消息（以Bytes类型存储）
    Publish {
        channel: String,

        // 使用自定义函数bytes_from_str将字符串解析为Bytes类型
        #[clap(parse(from_str = bytes_from_str))]
        message: Bytes,
    },
    // Subscribe命令，接受一个字符串向量，表示订阅的频道列表
    Subscribe {
        channels: Vec<String>,
    },
    Del {
        key: String,
    },
}

// 自定义函数，尝试将字符串解析为Duration类型（以毫秒为单位）
fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    // 尝试将字符串解析为u64类型的毫秒值
    let ms = src.parse::<u64>()?;
    // 将毫秒值转换为Duration类型并返回
    Ok(Duration::from_millis(ms))
}

// 自定义函数，将字符串转换为Bytes类型
fn bytes_from_str(src: &str) -> Bytes {
    // 直接从字符串创建Bytes类型的实例并返回
    Bytes::from(src.to_string())
}
