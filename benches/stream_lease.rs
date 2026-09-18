//! Real transport lease benchmarks.
//!
//! Each benchmark owns one reusable duplex transport. Fixture construction,
//! receive-buffer allocation, and teardown are outside the measured iterator;
//! the timed body is the advertised write/admission operation. This benchmark
//! intentionally does not run paired perf samples.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use icanact_remote::lease_test_support::{BufferConfig, ChannelId, LockFreeStreamHandle};
use icanact_remote::{AskResponder, ReplyDeliveryBudget, ReplyLease, ReplyPayload};
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

struct TransportFixture {
    handle: Arc<LockFreeStreamHandle>,
    writer_task: tokio::task::JoinHandle<()>,
    peer: tokio::io::DuplexStream,
    received: Vec<u8>,
}

impl TransportFixture {
    fn new(capacity: usize, receive_capacity: usize) -> Self {
        let (io, peer) = tokio::io::duplex(capacity);
        let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            ADDR.parse().expect("benchmark address"),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        Self {
            handle: Arc::new(handle),
            writer_task,
            peer,
            received: vec![0; receive_capacity],
        }
    }

    async fn drain(&mut self, bytes: usize) {
        self.peer
            .read_exact(&mut self.received[..bytes])
            .await
            .expect("benchmark peer drain");
        black_box(&self.received[..bytes]);
    }

    async fn finish(self) {
        self.handle.shutdown();
        let _ = self.writer_task.await;
    }
}

fn lease(
    handle: &Arc<LockFreeStreamHandle>,
    budget: &ReplyDeliveryBudget,
    correlation_id: u32,
) -> ReplyLease {
    AskResponder::from_stream_handle_for_test(
        correlation_id,
        Arc::clone(handle),
        Arc::new(AtomicBool::new(false)),
    )
    .try_reply_lease(budget, PAYLOAD_LEN)
    .expect("lease reservation")
}

async fn bench_unleased_inline(fixture: &mut TransportFixture) {
    let responder = AskResponder::from_stream_handle_for_test(
        1,
        Arc::clone(&fixture.handle),
        Arc::new(AtomicBool::new(false)),
    );
    responder
        .try_reply_bytes(Bytes::from_static(&[0xA5; PAYLOAD_LEN]))
        .expect("inline transport enqueue");
    fixture.drain(16 + PAYLOAD_LEN).await;
}

async fn bench_lease_publish(fixture: &mut TransportFixture, budget: &ReplyDeliveryBudget) {
    let responder = AskResponder::from_stream_handle_for_test(
        2,
        Arc::clone(&fixture.handle),
        Arc::new(AtomicBool::new(false)),
    );
    let lease = responder
        .try_reply_lease(budget, PAYLOAD_LEN)
        .expect("lease reservation");
    lease
        .try_reply_bytes(ReplyPayload::from_static(&[0xA5; PAYLOAD_LEN]))
        .expect("lease publication");
    fixture.drain(16 + PAYLOAD_LEN).await;
}

async fn bench_shared_fanout(
    fixture: &mut TransportFixture,
    budget: &ReplyDeliveryBudget,
    payload: &ReplyPayload,
    fanout: usize,
) {
    for correlation_id in 0..fanout as u32 {
        let lease = lease(&fixture.handle, budget, 100 + correlation_id);
        lease
            .try_reply_bytes(payload.clone())
            .expect("fanout publication");
    }
    fixture.drain(fanout * (16 + PAYLOAD_LEN)).await;
}

async fn bench_cancel(
    fixture: &mut TransportFixture,
    budget: &ReplyDeliveryBudget,
    leases: &mut Vec<ReplyLease>,
    next_correlation: &mut u32,
) {
    let canceled = leases.pop().expect("occupied cancellation lease");
    drop(canceled);
    fixture.drain(16 + TERMINAL.len()).await;

    // Keep occupancy constant without putting the O(occupied) setup/teardown
    // loop in the timed operation. One replacement admission is necessary to
    // make cancellation repeatable and is independent of the occupancy size.
    leases.push(lease(&fixture.handle, budget, *next_correlation));
    *next_correlation = next_correlation.wrapping_add(1);
}

fn bench_stream_lease(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");

    let mut unleased =
        runtime.block_on(async { TransportFixture::new(64 * 1024, 16 + PAYLOAD_LEN) });
    c.bench_function("stream_lease/unleased_inline_transport", |b| {
        b.iter(|| runtime.block_on(bench_unleased_inline(&mut unleased)))
    });
    runtime.block_on(unleased.finish());

    let publish_budget = budget(1);
    let mut publish =
        runtime.block_on(async { TransportFixture::new(64 * 1024, 16 + PAYLOAD_LEN) });
    c.bench_function("stream_lease/reserve_publish_transport", |b| {
        b.iter(|| runtime.block_on(bench_lease_publish(&mut publish, &publish_budget)))
    });
    runtime.block_on(publish.finish());

    for fanout in [1usize, 8, 64] {
        let shared_budget = budget(fanout);
        let payload = ReplyPayload::copy_from_slice(&[0xA5; PAYLOAD_LEN]);
        let mut shared = runtime.block_on(async {
            TransportFixture::new(2 * 1024 * 1024, fanout * (16 + PAYLOAD_LEN))
        });
        let name = format!("stream_lease/shared_payload_fanout_{fanout}");
        c.bench_function(&name, |b| {
            b.iter(|| {
                runtime.block_on(bench_shared_fanout(
                    &mut shared,
                    &shared_budget,
                    &payload,
                    fanout,
                ))
            })
        });
        runtime.block_on(shared.finish());
    }

    for occupied in [1usize, 64] {
        let cancel_budget = budget(occupied.max(1));
        let mut cancel =
            runtime.block_on(async { TransportFixture::new(64 * 1024, 16 + TERMINAL.len()) });
        let mut leases = Vec::with_capacity(occupied);
        for correlation_id in 0..occupied as u32 {
            leases.push(lease(&cancel.handle, &cancel_budget, 200 + correlation_id));
        }
        let mut next_correlation = 10_000;
        let name = format!("stream_lease/cancel_occupied_{occupied}");
        c.bench_function(&name, |b| {
            b.iter(|| {
                runtime.block_on(bench_cancel(
                    &mut cancel,
                    &cancel_budget,
                    &mut leases,
                    &mut next_correlation,
                ))
            })
        });
        drop(leases);
        runtime.block_on(cancel.finish());
    }
}

criterion_group!(benches, bench_stream_lease);
criterion_main!(benches);
