use clap::Parser;
use dotenv::dotenv;
use log::debug;

use mini_redis::client::cmd::Command;
use mini_redis::consts::DEFAULT_PORT;
use mini_redis::error::{MiniRedisClientError, MiniRedisConnectionError};
use mini_redis::{client, logger};

#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-cli",
    version,
    author,
    about = "Issue Redis commands"
)]

struct Cli {
    #[clap(subcommand)]
    command: Command,
    
    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), MiniRedisClientError> {
    dotenv().ok();
    logger::init();

    let cli = Cli::parse();
    debug!("client started: {:?}", cli);
    let addr = format!("{}:{}", cli.host, cli.port);

    let mut client = client::connect(&addr).await?;

    match cli.command {
        Command::Ping { msg } => {
            let v = client.ping(msg).await?;
            if let Ok(s) = std::str::from_utf8(&v) {
                println!("\"{}\"", s);
            } else {
                println!("{:?}", v);
            }
        }

        Command::Get { key } => {
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
            client.set(&key, value).await?;
            println!("OK");
        }

        Command::Set {
            key,
            value,
            expire: Some(expire),
        } => {
            client.set_expire(&key, value, expire).await?;
            println!("OK");
        }
        
        Command::Publish {
            channel,
            message,
        } => {
            client.publish(&channel, message).await?;
            println!("publish ok");
        }

        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err(MiniRedisConnectionError::InvalidArgument("channel(s) must be provided".into(),).into());
            }

            let mut subscriber = client.subscribe(channels).await?;

            while let Some(msg) = subscriber.next_message().await? {
                println!("got message from the channel: {}; message = {:?}", msg.channel, msg.content);
            }
        } 
    }
    Ok(())
}