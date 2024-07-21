use clap::Parser;
use dotenv::dotenv;
use log::debug;

use mini_redis::client::cmd::Command;
use mini_redis::consts::DEFAULT_PORT;
use mini_redis::error::{MiniRedisClientError, MiniRedisConnectionError};
use mini_redis::{client, logger};

#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-cli", // 应用名称
    version, // 版本号，从Cargo.toml自动获取
    author, // 作者信息，从Cargo.toml自动获取
    about = "Issue Redis commands" // 应用简介
)]

struct Cli {
    #[clap(subcommand)] // 表示以下字段为子命令
    cmd: Command, // Redis命令
    
    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String, // 主机名

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16, // 端口号
}

/// 初始化 mini-redis 服务器，解析命令行参数并设置日志。
///
/// 调用 `dotenv` 来加载 `.env` 文件中的环境变量，初始化日志系统，并解析命令行参数。
fn init() -> Cli {
    dotenv().ok();
    logger::init();
    Cli::parse()
}

/// CLI 工具的入口点。
///
/// `[tokio::main]` 注释表明在调用该函数时应启动 Tokio 运行时。
/// 该函数的主体将在新生成的运行时中执行。
///
/// 使用 `flavor = "current_thread"` 来避免生成后台线程。
/// 对于 CLI 工具用例，轻量级的单线程比多线程更有优势。
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), MiniRedisClientError> {
    let cli = init(); // 初始化并解析命令行参数
    debug!("client started: {:?}", cli);

    // 获取要连接的远程地址
    let addr = format!("{}:{}", cli.host, cli.port);

    // 建立连接
    let mut client = client::connect(&addr).await?;

    // 根据命令类型执行相应操作
    match cli.cmd {
        Command::Ping { msg } => {
            // 执行 Ping 命令
            let v = client.ping(msg).await?;
            if let Ok(s) = std::str::from_utf8(&v) {
                println!("\"{}\"", s);
            } else {
                println!("{:?}", v);
            }
        }

        Command::Get { key } => {
            // 执行 Get 命令
            if let Some(v) = client.get(&key).await? {
                if let Ok(s) = std::str::from_utf8(&v) {
                    println!("\"{}\"", s);
                } else {
                    println!("{:?}", v);
                }
            } else {
                println!("(nil)");
            }
        }

        Command::Set {
            key,
            value,
            expire: None,
        } => {
            // 执行 Set 命令，不带过期时间
            client.set(&key, value).await?;
            println!("OK");
        }

        Command::Set {
            key,
            value,
            expire: Some(expire),
        } => {
            // 执行 Set 命令，带过期时间
            client.set_expire(&key, value, expire).await?;
            println!("OK");
        }
        
        Command::Publish {
            channel,
            message,
        } => {
            // 执行 Publish 命令
            client.publish(&channel, message).await?;
            println!("publish ok");
        }

        Command::Subscribe { channels } => {
            // 执行 Subscribe 命令
            if channels.is_empty() {
                return Err(MiniRedisConnectionError::InvalidArgument("channel(s) must be provided".into(),).into());
            }

            let mut subscriber = client.subscribe(channels).await?;

            while let Some(msg) = subscriber.next_message().await? {
                println!("got message from the channel: {}; message = {:?}", msg.channel, msg.content);
            }
        } 

        Command::Del { key } => {
            // 执行 Del 命令
            client.del(&key).await?;
            println!("OK");
        }
    }
    Ok(())
}