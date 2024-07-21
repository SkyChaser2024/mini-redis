use log::debug;

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::error::MiniRedisConnectionError;

/// 表示一个“未知”命令。这不是一个真正的 `Redis` 命令。
#[derive(Debug)]
pub struct Unknown {
    cmd_name: String,
}

impl Unknown {
    /// 创建一个新的 `Unknown` 实例。
    ///
    /// # 参数
    ///
    /// * `key` - 未知命令的名称，可以是任何实现了 `ToString` trait 的类型。
    ///
    /// # 返回值
    ///
    /// 返回一个 `Unknown` 实例。
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            cmd_name: key.to_string(),
        }
    }

    /// 获取未知命令的名称。
    ///
    /// # 返回值
    ///
    /// 返回一个字符串切片，表示命令的名称。
    pub(crate) fn get_name(&self) -> &str {
        &self.cmd_name
    }

    /// 处理未知命令。
    ///
    /// 将错误信息作为响应发送给客户端。
    ///
    /// # 参数
    ///
    /// * `dst` - 目标连接，用于写入响应帧。
    ///
    /// # 返回值
    ///
    /// 成功时返回 `Ok(())`，表示响应已成功发送。
    /// 失败时返回 `MiniRedisConnectionError`，表示发送过程中出现了错误。
    pub(crate) async fn apply(self, dst: &mut Connection) -> Result<(), MiniRedisConnectionError> {
        let response = Frame::Error(format!("err unknown command '{}'", self.cmd_name));
        debug!("apply unknown command resp: '{:?}'", response);
        dst.write_frame(&response).await?;
        Ok(())
    }
}
