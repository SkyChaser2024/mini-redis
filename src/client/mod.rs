use tokio::net::{TcpStream, ToSocketAddrs};

use crate::client::cli::Client;
use crate::connection::connect::Connection;
use crate::error::MiniRedisConnectionError;

pub mod cli;
pub mod cmd;
mod subscriber;

pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client, MiniRedisConnectionError> {
    let socket = TcpStream::connect(addr).await?;
    let conn = Connection::new(socket);

    Ok(Client { conn })
}