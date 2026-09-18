//! Real transport lease benchmarks.
//!
//! The `*_targeted` cases measure publication/cancellation only. Fixture
//! construction, occupancy setup, buffer allocation, wire drain, and
//! resource-release gating are in Criterion setup/teardown callbacks, outside
//! the timed routine. The separately named `*_end_to_end` cases intentionally
//! include delivery and are not paired performance samples.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use icanact_remote::lease_test_support::{
    BufferConfig, ChannelId, LockFreeStreamHandle, lease_stats,
};
use icanact_remote::{AskResponder, ReplyDeliveryBudget, ReplyLease, ReplyPayload};
use std::sync::{Arc, atomic::AtomicBool};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

const ADDR: &str = "127.0.0.1:40600";
const PAYLOAD_LEN: usize = 4096;
const TERMINAL: &[u8] = b"cancelled";
const PAYLOAD: &[u8] = &[0xA5; PAYLOAD_LEN];

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
        std::hint::black_box(&self.received[..bytes]);
    }

    async fn drain_and_wait_for_release(&mut self, bytes: usize) {
        self.drain(bytes).await;
        while lease_stats(&self.handle).live_slots != 0 {
            tokio::task::yield_now().await;
        }
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

fn unleased_responder(fixture: &TransportFixture) -> AskResponder {
    AskResponder::from_stream_handle_for_test(
        1,
        Arc::clone(&fixture.handle),
        Arc::new(AtomicBool::new(false)),
    )
}

fn publish_unleased_inline(responder: AskResponder) {
    responder
        .try_reply_bytes(Bytes::from_static(PAYLOAD))
        .expect("inline transport enqueue");
}

fn publish_lease(lease: ReplyLease) {
    lease
        .try_reply_bytes(ReplyPayload::from_static(PAYLOAD))
        .expect("lease publication");
}

fn prepare_shared_fanout(
    fixture: &TransportFixture,
    budget: &ReplyDeliveryBudget,
    fanout: usize,
) -> Vec<ReplyLease> {
    (0..fanout as u32)
        .map(|correlation_id| lease(&fixture.handle, budget, 100 + correlation_id))
        .collect()
}

fn publish_shared_fanout(leases: Vec<ReplyLease>) {
    for lease in leases {
        publish_lease(lease);
    }
}

fn cancel_one(mut leases: Vec<ReplyLease>) -> Vec<ReplyLease> {
    let canceled = leases.pop().expect("occupied cancellation lease");
    drop(canceled);
    leases
}

async fn bench_unleased_inline_end_to_end(fixture: &mut TransportFixture) {
    publish_unleased_inline(unleased_responder(fixture));
    fixture.drain(16 + PAYLOAD_LEN).await;
}

async fn bench_lease_publish_end_to_end(
    fixture: &mut TransportFixture,
    budget: &ReplyDeliveryBudget,
) {
    publish_lease(lease(&fixture.handle, budget, 2));
    fixture.drain(16 + PAYLOAD_LEN).await;
}

async fn bench_shared_fanout_end_to_end(
    fixture: &mut TransportFixture,
    budget: &ReplyDeliveryBudget,
    fanout: usize,
) {
    publish_shared_fanout(prepare_shared_fanout(fixture, budget, fanout));
    fixture.drain(fanout * (16 + PAYLOAD_LEN)).await;
}

async fn bench_cancel_end_to_end(
    fixture: &mut TransportFixture,
    budget: &ReplyDeliveryBudget,
    leases: &mut Vec<ReplyLease>,
    next_correlation: &mut u32,
) {
    let canceled = leases.pop().expect("occupied cancellation lease");
    drop(canceled);
    fixture.drain(16 + TERMINAL.len()).await;
    leases.push(lease(&fixture.handle, budget, *next_correlation));
    *next_correlation = next_correlation.wrapping_add(1);
}

fn bench_stream_lease(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");

    let publish_budget = budget(1);
    let mut unleased =
        runtime.block_on(async { TransportFixture::new(64 * 1024, 16 + PAYLOAD_LEN) });
    c.bench_function("stream_lease/unleased_inline_end_to_end", |b| {
        b.iter(|| runtime.block_on(bench_unleased_inline_end_to_end(&mut unleased)))
    });
    runtime.block_on(unleased.finish());

    let mut targeted_unleased =
        runtime.block_on(async { TransportFixture::new(64 * 1024, 16 + PAYLOAD_LEN) });
    c.bench_function("stream_lease/unleased_inline_targeted", |b| {
        b.iter_custom(|iters| {
            let mut measured = Duration::ZERO;
            for _ in 0..iters {
                let responder = unleased_responder(&targeted_unleased);
                let start = Instant::now();
                publish_unleased_inline(responder);
                measured += start.elapsed();
                runtime.block_on(targeted_unleased.drain_and_wait_for_release(16 + PAYLOAD_LEN));
            }
            measured
        })
    });
    runtime.block_on(targeted_unleased.finish());

    let mut publish =
        runtime.block_on(async { TransportFixture::new(64 * 1024, 16 + PAYLOAD_LEN) });
    c.bench_function("stream_lease/reserve_publish_end_to_end", |b| {
        b.iter(|| {
            runtime.block_on(bench_lease_publish_end_to_end(
                &mut publish,
                &publish_budget,
            ))
        })
    });
    runtime.block_on(publish.finish());

    let mut targeted_publish =
        runtime.block_on(async { TransportFixture::new(64 * 1024, 16 + PAYLOAD_LEN) });
    c.bench_function("stream_lease/reserve_publish_targeted", |b| {
        b.iter_custom(|iters| {
            let mut measured = Duration::ZERO;
            for _ in 0..iters {
                let prepared = lease(&targeted_publish.handle, &publish_budget, 2);
                let start = Instant::now();
                publish_lease(prepared);
                measured += start.elapsed();
                runtime.block_on(targeted_publish.drain_and_wait_for_release(16 + PAYLOAD_LEN));
            }
            measured
        })
    });
    runtime.block_on(targeted_publish.finish());

    for fanout in [1usize, 8, 64] {
        let shared_budget = budget(fanout);
        let mut shared = runtime.block_on(async {
            TransportFixture::new(2 * 1024 * 1024, fanout * (16 + PAYLOAD_LEN))
        });
        let name = format!("stream_lease/shared_payload_fanout_{fanout}_end_to_end");
        c.bench_function(&name, |b| {
            b.iter(|| {
                runtime.block_on(bench_shared_fanout_end_to_end(
                    &mut shared,
                    &shared_budget,
                    fanout,
                ))
            })
        });
        runtime.block_on(shared.finish());

        let mut targeted_shared = runtime.block_on(async {
            TransportFixture::new(2 * 1024 * 1024, fanout * (16 + PAYLOAD_LEN))
        });
        let name = format!("stream_lease/shared_payload_fanout_{fanout}_targeted");
        c.bench_function(&name, |b| {
            b.iter_custom(|iters| {
                let mut measured = Duration::ZERO;
                for _ in 0..iters {
                    let prepared = prepare_shared_fanout(&targeted_shared, &shared_budget, fanout);
                    let start = Instant::now();
                    publish_shared_fanout(prepared);
                    measured += start.elapsed();
                    runtime.block_on(
                        targeted_shared.drain_and_wait_for_release(fanout * (16 + PAYLOAD_LEN)),
                    );
                }
                measured
            })
        });
        runtime.block_on(targeted_shared.finish());
    }

    for occupied in [1usize, 64] {
        let cancel_budget = budget(occupied.max(1));
        let mut cancel = runtime
            .block_on(async { TransportFixture::new(64 * 1024, occupied * (16 + TERMINAL.len())) });
        let mut leases = Vec::with_capacity(occupied);
        for correlation_id in 0..occupied as u32 {
            leases.push(lease(&cancel.handle, &cancel_budget, 200 + correlation_id));
        }
        let mut next_correlation = 10_000;
        let name = format!("stream_lease/cancel_occupied_{occupied}_end_to_end");
        c.bench_function(&name, |b| {
            b.iter(|| {
                runtime.block_on(bench_cancel_end_to_end(
                    &mut cancel,
                    &cancel_budget,
                    &mut leases,
                    &mut next_correlation,
                ))
            })
        });
        drop(leases);
        runtime.block_on(cancel.finish());

        let mut targeted_cancel = runtime
            .block_on(async { TransportFixture::new(64 * 1024, occupied * (16 + TERMINAL.len())) });
        let targeted_budget = budget(occupied.max(1));
        let name = format!("stream_lease/cancel_occupied_{occupied}_targeted");
        c.bench_function(&name, |b| {
            b.iter_custom(|iters| {
                let mut measured = Duration::ZERO;
                for _ in 0..iters {
                    let mut occupied_leases = Vec::with_capacity(occupied);
                    for correlation_id in 0..occupied as u32 {
                        occupied_leases.push(lease(
                            &targeted_cancel.handle,
                            &targeted_budget,
                            30_000 + correlation_id,
                        ));
                    }
                    let start = Instant::now();
                    let survivors = cancel_one(occupied_leases);
                    measured += start.elapsed();
                    let terminal_count = survivors.len() + 1;
                    runtime.block_on(async {
                        drop(survivors);
                        targeted_cancel
                            .drain_and_wait_for_release(terminal_count * (16 + TERMINAL.len()))
                            .await;
                    });
                }
                measured
            })
        });
        runtime.block_on(targeted_cancel.finish());
    }
}

criterion_group!(benches, bench_stream_lease);
criterion_main!(benches);
