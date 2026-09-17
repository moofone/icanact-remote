//! Real transport lease benchmarks.
//!
//! These measurements exercise the duplex writer and peer drain rather than
//! payload construction alone. Compare medians on the same host/configuration;
//! investigate regressions above 5% median or 10% p99 before changing a hot
//! path. This benchmark intentionally does not run paired perf samples.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use icanact_remote::lease_test_support::{BufferConfig, ChannelId, LockFreeStreamHandle};
use icanact_remote::{AskResponder, ReplyDeliveryBudget, ReplyPayload};
use std::hint::black_box;
use std::sync::{Arc, atomic::AtomicBool};
use tokio::io::AsyncReadExt;

const ADDR: &str = "127.0.0.1:40600";
const PAYLOAD_LEN: usize = 4096;
const TERMINAL: &[u8] = b"cancelled";

fn budget(job_limit: usize) -> ReplyDeliveryBudget {
    ReplyDeliveryBudget::new(
        job_limit,
        job_limit * PAYLOAD_LEN,
        ReplyPayload::from_static(TERMINAL),
    )
    .expect("benchmark budget")
}

fn handle(
    capacity: usize,
) -> (
    Arc<LockFreeStreamHandle>,
    tokio::task::JoinHandle<()>,
    tokio::io::DuplexStream,
) {
    let (io, peer) = tokio::io::duplex(capacity);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        ADDR.parse().expect("benchmark address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    (Arc::new(handle), writer_task, peer)
}

async fn drain(peer: &mut tokio::io::DuplexStream, bytes: usize) {
    let mut received = vec![0u8; bytes];
    peer.read_exact(&mut received)
        .await
        .expect("benchmark peer drain");
    black_box(received);
}

async fn bench_unleased_inline() {
    let (handle, writer_task, mut peer) = handle(64 * 1024);
    let responder = AskResponder::from_stream_handle_for_test(
        1,
        Arc::clone(&handle),
        Arc::new(AtomicBool::new(false)),
    );
    responder
        .try_reply_bytes(Bytes::from_static(&[0xA5; PAYLOAD_LEN]))
        .expect("inline transport enqueue");
    drain(&mut peer, 16 + PAYLOAD_LEN).await;
    handle.shutdown();
    let _ = writer_task.await;
}

async fn bench_lease_publish() {
    let (handle, writer_task, mut peer) = handle(64 * 1024);
    let responder = AskResponder::from_stream_handle_for_test(
        2,
        Arc::clone(&handle),
        Arc::new(AtomicBool::new(false)),
    );
    let lease = responder
        .try_reply_lease(&budget(1), PAYLOAD_LEN)
        .expect("lease reservation");
    lease
        .try_reply_bytes(ReplyPayload::from_static(&[0xA5; PAYLOAD_LEN]))
        .expect("lease publication");
    drain(&mut peer, 16 + PAYLOAD_LEN).await;
    handle.shutdown();
    let _ = writer_task.await;
}

async fn bench_shared_fanout(fanout: usize) {
    let (handle, writer_task, mut peer) = handle(2 * 1024 * 1024);
    let budget = budget(fanout);
    let payload = ReplyPayload::copy_from_slice(&[0xA5; PAYLOAD_LEN]);
    for correlation_id in 0..fanout as u32 {
        let responder = AskResponder::from_stream_handle_for_test(
            100 + correlation_id,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        );
        let lease = responder
            .try_reply_lease(&budget, PAYLOAD_LEN)
            .expect("fanout reservation");
        lease
            .try_reply_bytes(payload.clone())
            .expect("fanout publication");
    }
    drain(&mut peer, fanout * (16 + PAYLOAD_LEN)).await;
    handle.shutdown();
    let _ = writer_task.await;
}

async fn bench_cancel(occupied: usize) {
    let (handle, writer_task, mut peer) = handle(64 * 1024);
    let budget = budget(occupied.max(1));
    let mut leases = Vec::with_capacity(occupied);
    for correlation_id in 0..occupied as u32 {
        let responder = AskResponder::from_stream_handle_for_test(
            200 + correlation_id,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        );
        leases.push(
            responder
                .try_reply_lease(&budget, PAYLOAD_LEN)
                .expect("occupied reservation"),
        );
    }
    if let Some(lease) = leases.pop() {
        drop(lease);
        drain(&mut peer, 16 + TERMINAL.len()).await;
    }
    handle.shutdown();
    drop(leases);
    let _ = writer_task.await;
}

fn bench_stream_lease(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");
    c.bench_function("stream_lease/unleased_inline_transport", |b| {
        b.to_async(&runtime)
            .iter(|| async { bench_unleased_inline().await })
    });
    c.bench_function("stream_lease/reserve_publish_transport", |b| {
        b.to_async(&runtime)
            .iter(|| async { bench_lease_publish().await })
    });
    for fanout in [1usize, 8, 64] {
        let name = format!("stream_lease/shared_payload_fanout_{fanout}");
        c.bench_function(&name, |b| {
            b.to_async(&runtime)
                .iter(|| async { bench_shared_fanout(fanout).await })
        });
    }
    for occupied in [1usize, 64] {
        let name = format!("stream_lease/cancel_occupied_{occupied}");
        c.bench_function(&name, |b| {
            b.to_async(&runtime)
                .iter(|| async { bench_cancel(occupied).await })
        });
    }
}

criterion_group!(benches, bench_stream_lease);
criterion_main!(benches);
