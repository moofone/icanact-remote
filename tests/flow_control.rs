use bytes::Bytes;
use icanact_remote::connection_pool::BufferConfig;
use icanact_remote::{ChannelId, LockFreeStreamHandle, Result};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn test_ask_backpressure_no_write_buffer_full() -> Result<()> {
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);

    let (handle, _writer_task) = LockFreeStreamHandle::new(
        writer,
        "127.0.0.1:0".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);

    let reader_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let mut tasks = Vec::new();
    for _ in 0..100 {
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                handle.write_bytes_ask(Bytes::from_static(b"ping")).await?;
            }
            Ok::<(), icanact_remote::GossipError>(())
        }));
    }

    for task in tasks {
        task.await.unwrap()?;
    }

    handle.shutdown();
    drop(handle);
    reader_task.abort();

    Ok(())
}
