//! mini-redis server.
//!
//! 此文件是库中实现的服务器的入口点,它执行命令行解析并将参数传递给 `mini_redis::server`.
//!
//! `clap` 包用于解析参数。

use clap::Parser; // 导入命令行参数解析库
use dotenv::dotenv; // 导入环境变量处理库
use tokio::net::TcpListener; // 异步网络编程库中的TCP监听器
use tokio::signal; // 异步信号处理

use mini_redis::consts::DEFAULT_PORT; // 导入默认端口常量
use mini_redis::error::MiniRedisServerError; // 导入自定义服务端错误类型
use mini_redis::{logger, server}; // 导入日志和服务器模块

/// 代表 mini-redis 服务器的命令行参数
#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-server",
    version, // 从Cargo.toml自动获取版本信息
    author, // 从Cargo.toml自动获取作者信息
    about = "A mini redis server" // 关于此程序的简短描述
)]

// 定义客户端结构体，包含命令行参数
struct Cli {
    #[clap(long)] // 用来指定命令行参数的长选项 例如 --port
    port: Option<u16>, // 可选的端口号
}

/// 初始化 mini-redis 服务器，解析命令行参数并设置日志。
///
/// 调用 `dotenv` 来加载 `.env` 文件中的环境变量，初始化日志系统，并解析命令行参数。
fn init() -> Cli {
    dotenv().ok(); //该函数的作用是从 .env 文件中加载环境变量
    logger::init(); // 初始化日志系统
    Cli::parse() // 解析命令行参数并返回一个 Cli 对象
}

/// mini-redis 服务器的入口点。
///
/// 使用 `tokio` 的异步主函数。绑定TCP监听器到指定端口，然后运行服务器。
/// 如果接收到 Ctrl+C 信号，服务器将停止运行。
#[tokio::main] // 标记为异步主函数，能够使用 await
pub async fn main() -> Result<(), MiniRedisServerError> {
    let cli = init(); // 初始化并解析命令行参数
    let port = cli.port.unwrap_or(DEFAULT_PORT); // 获取端口号，如果未指定，则使用默认值

    let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?; // 异步监听 0.0.0.0:port

    server::run(listener, signal::ctrl_c()).await; // 运行服务器，等待 Ctrl+C 信号

    Ok(())
}
