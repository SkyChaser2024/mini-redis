use log::debug;
use tokio::sync::mpsc;

use crate::cmd::Command;
use crate::connection::connect::Connection;
use crate::error::MiniRedisConnectionError;
use crate::server::shutdown::Shutdown;
use crate::storage::db::Db;

#[derive(Debug)]
pub(crate) struct Handler {
    pub(crate) db: Db,
    pub(crate) conn: Connection,
    pub(crate) shutdown: Shutdown,
    pub(crate) _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    pub(crate) async fn run(&mut self) -> Result<(), MiniRedisConnectionError> {
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.conn.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => {
                    return Ok(());
                }
            };

            let cmd = Command::from_frame(frame)?;
            debug!("received command: {:?}", cmd);
            cmd.apply(&self.db, &mut self.conn, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
