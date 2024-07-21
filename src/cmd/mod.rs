use crate::cmd::get::Get;
use crate::cmd::ping::Ping;
use crate::cmd::publish::Publish;
use crate::cmd::set::Set;
use crate::cmd::subscribe::Subscribe;
use crate::cmd::unknown::Unknown;
use crate::cmd::unsubscribe::Unsubscribe;
use crate::cmd::del::Del;

pub(crate) mod get;
pub(crate) mod ping;
pub(crate) mod publish;
pub(crate) mod set;
pub(crate) mod subscribe;
pub(crate) mod unknown;
pub(crate) mod unsubscribe;
pub(crate) mod del;

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::server::shutdown::Shutdown;
use crate::storage::db::Db;

/// 支持的 Redis 命令枚举。
///
/// 调用 `Command` 上的方法会委托给命令实现。
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Ping(Ping),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Unknown(Unknown),
    Del(Del),
}

impl Command {
    /// 从接收到的帧解析命令。
    ///
    /// `Frame` 必须表示 `mini-redis` 支持的 Redis 命令，并且是数组变体。
    ///
    /// # 返回值
    ///
    /// 成功时返回命令值，否则返回 `Err`。
    pub(crate) fn from_frame(frame: Frame) -> Result<Command, MiniRedisParseError> {
        // `Frame` 值被 `Parse` 装饰。`Parse` 提供了类似“光标”的 API，
        // 使得解析命令更容易。
        //
        // `Frame` 值必须是数组变体。任何其他帧变体都会导致返回错误。
        let mut parse = Parse::new(frame)?;

        // 所有 Redis 命令都以命令名称作为字符串开始
        // 读取名称并转换为小写，以便进行区分大小写的匹配
        let cmd_name = parse.next_string()?.to_lowercase();

        // 匹配命令名称，将其余的解析委托给特定的命令
        let cmd = match &cmd_name[..] {
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frame(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frame(&mut parse)?),
            "set" => Command::Set(Set::parse_frame(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frame(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frame(&mut parse)?),
            "del" => Command::Del(Del::parse_frame(&mut parse)?),
            _ => {
                // 命令未被识别，返回一个 `Unknown` 命令。
                //
                // 这里调用 `return` 跳过下面的 `finish()` 调用。
                // 由于命令未被识别，`Parse` 实例中可能还有未消费的字段。
                return Ok(Command::Unknown(Unknown::new(cmd_name)));
            }
        };

        // 检查 `Parse` 值中是否有任何未消费的字段
        // 如果有剩余字段，这表示帧格式意外，返回错误
        parse.finish()?;

        Ok(cmd)
    }

    /// 将命令应用于指定的 `Db` 实例。
    ///
    /// 响应被写入 `dst`。服务器调用此方法以执行接收到的命令。
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<(), MiniRedisConnectionError> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Ping(cmd) => cmd.apply(dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
             // `Unsubscribe` 不能被应用。它只能在 `Subscribe` 命令的上下文中接收
            Unsubscribe(_) => Err(MiniRedisConnectionError::CommandExecute(
                "`Unsubscribe` is unsupported in this context".into(),
            )),
            Unknown(cmd) => cmd.apply(dst).await,
            Del(cmd) => cmd.apply(db, dst).await,
        }
    }

    /// 根据自身枚举类型返回命令名称，方便识别命令类型。
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Ping(_) => "ping",
            Command::Publish(_) => "publish",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Unknown(cmd) => cmd.get_name(),
            Command::Del(_) => "del",
        }
    }
}
