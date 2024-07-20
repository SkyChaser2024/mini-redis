use log::debug;

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::error::MiniRedisConnectionError;

#[derive(Debug)]
pub struct Unknown {
    cmd_name: String,
}

impl Unknown {
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            cmd_name: key.to_string(),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.cmd_name
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> Result<(), MiniRedisConnectionError> {
        // let response = Frame::Error(format!("err unknown command: '{}'", self.cmd_name));
        let response = Frame::Error(format!("err unknown command '{}'", self.cmd_name));
        debug!("apply unknown command resp: '{:?}'", response);
        dst.write_frame(&response).await?;
        Ok(())
    }
}
