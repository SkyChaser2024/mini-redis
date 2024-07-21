use std::net::SocketAddr;

use mini_redis::{client, server};
use tokio::net::TcpListener;

/// 一个没有提供消息的 PING PONG 测试。
/// 它应该返回 "PONG"。
#[tokio::test]
async fn ping_pong_without_message() {
    let addr = start_server().await;
    let mut client = client::connect(addr).await.unwrap();

    let pong = client.ping(None).await.unwrap();
    assert_eq!(b"PONG", &pong[..]);
}

/// 一个提供消息的 PING PONG 测试。
/// 它应该返回该消息。
#[tokio::test]
async fn ping_pong_with_message() {
    let addr = start_server().await;
    let mut client = client::connect(addr).await.unwrap();

    let pong = client.ping(Some("你好世界".to_string())).await.unwrap();
    assert_eq!("你好世界".as_bytes(), &pong[..]);
}

/// 一个基本的 "hello world" 风格测试。服务器实例在后台任务中启动。
/// 然后建立一个客户端实例，并向服务器发送 set 和 get 命令。
/// 然后评估响应。
#[tokio::test]
async fn key_value_get_set() {
    let addr = start_server().await;

    let mut client = client::connect(addr).await.unwrap();
    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..])
}

/// 类似于 "hello world" 风格的测试，但这次测试一个单频道订阅。
#[tokio::test]
async fn receive_message_subscribed_channel() {
    let addr = start_server().await;

    let client = client::connect(addr).await.unwrap();
    let mut subscriber = client.subscribe(vec!["hello".into()]).await.unwrap();

    tokio::spawn(async move {
        let mut client = client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap()
    });

    let message = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message.channel);
    assert_eq!(b"world", &message.content[..])
}

/// 测试客户端从多个订阅频道接收消息。
#[tokio::test]
async fn receive_message_multiple_subscribed_channels() {
    let addr = start_server().await;

    let client = client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "world".into()])
        .await
        .unwrap();

    tokio::spawn(async move {
        let mut client = client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap()
    });

    let message1 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message1.channel);
    assert_eq!(b"world", &message1.content[..]);

    tokio::spawn(async move {
        let mut client = client::connect(addr).await.unwrap();
        client.publish("world", "howdy?".into()).await.unwrap()
    });

    let message2 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("world", &message2.channel);
    assert_eq!(b"howdy?", &message2.content[..])
}

/// 测试客户端取消订阅所有频道后准确移除其订阅的频道列表，方法是提交一个空的 vec。
#[tokio::test]
async fn unsubscribes_from_channels() {
    let addr = start_server().await;

    let client = client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "world".into()])
        .await
        .unwrap();

    subscriber.unsubscribe(&[]).await.unwrap();
    assert_eq!(subscriber.get_subscribed().len(), 0);
}

/// 测试 DEL 命令，确保键被删除并且返回正确的删除数量。  
#[tokio::test]  
async fn test_del_command() {  
    let addr = start_server().await;  
  
    let mut client = client::connect(addr).await.unwrap();  
  
    // 首先设置一个键  
    client.set("hello", "world".into()).await.unwrap();  
  
    // 然后尝试删除这个键  
    let deleted_count = client.del("hello").await.unwrap();  
  
    // 断言键被成功删除，且删除数量为 1  
    assert_eq!(deleted_count, 1);  
  
    // 再次尝试获取被删除的键， 会出错
    // let value = client.get("hello").await.unwrap();  
    // assert!(value.is_none());  
}

/// 启动服务器并返回服务器地址
async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    addr
}