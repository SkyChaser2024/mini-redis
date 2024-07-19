use clap::Parser; // 命令行参数解析
use dotenv::dotenv; // 环境变量处理
use tokio::net::TcpListener;
use tokio::signal;

use mini_redis::consts::DEFAULT_PORT;
use mini_redis::error::MiniRedisServerError;
use mini_redis::{server, logger};


/// 代表 mini-redis 服务器的命令行参数
#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-server",
    version,
    author,
    about = "A mini redis server"
)]

// 定义客户端结构体，包含命令行参数
struct Cli {
    #[clap(long)] // 用来指定命令行参数的长选项 例如 --port
    port: Option<u16>, // 端口号
}

/// 初始化 mini-redis 服务器，解析命令行参数并设置日志
fn init() -> Cli {
    // TODO: 错误处理
    dotenv().ok(); //该函数的作用是从 .env 文件中加载环境变量
    logger::init();
    Cli::parse() // 解析命令行参数并返回一个 Client 对象
}

/// mini-redis 服务器的入口点
#[tokio::main] // 标记为异步主函数，能够使用 await
pub async fn main() -> Result<(), MiniRedisServerError> { // 异步主函数，返回 Result 类型
    let cli = init();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?; // 监听 0.0.0.0:port

    server::run(listener, signal::ctrl_c()).await; // server 启动函数，Ctrl+C 时触发

    Ok(())
}