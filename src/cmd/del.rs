use bytes::Bytes;
use log::debug;

use crate::connection::connect::Connection;
use crate::connection::frame::Frame;
use crate::connection::parse::Parse;
use crate::error::{MiniRedisConnectionError, MiniRedisParseError};
use crate::storage::db::Db;
use crate::storage::traits::KvStore;

/// 删除指定的键及其关联的值。  
///  
/// 如果键不存在，则此操作无效。  
#[derive(Debug)]
pub struct Del {
    /// 要删除的键  
    key: String,
}

impl Del {
    /// 创建一个新的DEL操作实例。  
    ///  
    /// # 参数  
    ///  
    /// * `key` - 要删除的键，实现了`ToString` trait。  
    ///  
    /// # 返回值  
    ///  
    /// 返回一个包含指定键的DEL操作实例。  
    pub fn new(key: impl ToString) -> Del {
        Del {
            key: key.to_string(),
        }
    }

    /// 从接收到的帧中解析 `Del` 实例。  
    ///  
    /// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。此时，已从套接字接收到整个帧。  
    ///  
    /// `DEL` 字符串已被使用。  
    ///  
    /// # 返回  
    ///  
    /// 成功时返回 `Del` 值。如果帧格式不正确，则返回 `Err`。  
    ///  
    /// # 格式  
    ///  
    /// 期望数组帧包含至少 1 个条目。  
    ///  
    /// ```text  
    /// DEL key  
    /// ```  
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Del, MiniRedisParseError> {
        let key = parse.next_string()?; // 读取 key

        Ok(Del { key })
    }

    /// 将 `Del` 命令应用于指定的 `Db` 实例。  
    ///  
    /// 将响应写入 `dst`。服务器调用此方法以便执行收到的命令。  
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
    ) -> Result<(), MiniRedisConnectionError> {
        // 在 db 中删除 key
        let delete_cnt = db.del(self.key);

        let response = Frame::Integer(delete_cnt as u64);

        debug!("del cmd applied response: {:?}", response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将 `DEL` 操作转换为用于网络传输的 `Frame` 格式。  
    ///  
    /// # 返回值  
    ///  
    /// 返回一个表示 `DEL` 请求的 `Frame` 实例。  
    pub(crate) fn into_frame(self) -> Result<Frame, MiniRedisParseError> {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("del".as_bytes()))?;
        frame.push_bulk(Bytes::from(self.key.into_bytes()))?;
        Ok(frame)
    }

    /// 获取 `key`。  
    pub fn key(&self) -> &str {
        &self.key
    }
}
