use crate::cmd::get::Get;
use crate::cmd::ping::Ping;
use crate::cmd::publish::Publish;
use crate::cmd::set::Set;
use crate::cmd::subscribe::Subscribe;
use crate::cmd::unknown::Unknown;
use crate::cmd::unsubscribe::Unsubscribe;

pub(crate) mod get;
pub(crate) mod ping;
pub(crate) mod publish;
pub(crate) mod set;
pub(crate) mod subscribe;
pub(crate) mod unknown;
pub(crate) mod unsubscribe;

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::server::shutdown::Shutdown;
use crate::storage::db::Db;

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Ping(Ping),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Unknown(Unknown),
}

impl Command {
    pub(crate) fn from_frame(frame: Frame) -> Result<Command, MiniRedisParseError> {
        let mut parse = Parse::new(frame)?;

        let cmd_name = parse.next_string()?.to_lowercase();

        let cmd = match &cmd_name[..] {
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frame(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frame(&mut parse)?),
            "set" => Command::Set(Set::parse_frame(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frame(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frame(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(cmd_name)));
            }
        };

        parse.finish()?;

        Ok(cmd)
    }

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
            Unsubscribe(_) => Err(MiniRedisConnectionError::CommandExecute(
                "`Unsubscribe` is unsupported in this context".into(),
            )),
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Ping(_) => "ping",
            Command::Publish(_) => "publish",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
