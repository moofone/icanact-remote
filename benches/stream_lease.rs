use criterion::{Criterion, criterion_group, criterion_main};
use icanact_remote::{ReplyDeliveryBudget, ReplyPayload};
use std::hint::black_box;

fn budget() -> ReplyDeliveryBudget {
    ReplyDeliveryBudget::new(64, 64 * 4096, ReplyPayload::from_static(b"cancelled")).unwrap()
}

fn bench_stream_lease(c: &mut Criterion) {
    let payload = ReplyPayload::from_static(&[0xA5; 4096]);
    c.bench_function("stream_lease/unleased_inline", |b| {
        b.iter(|| black_box(payload.as_ref().len()))
    });

    c.bench_function("stream_lease/reserve_publish", |b| {
        b.iter(|| {
            // The benchmark stays on public APIs: constructing a budget is
            // the bounded admission half, while the owned payload models the
            // publish retained by a lease.
            let budget = budget();
            let retained = ReplyPayload::copy_from_slice(payload.as_ref());
            black_box((budget, retained));
        })
    });

    for fanout in [1usize, 8, 64] {
        c.bench_function(&format!("stream_lease/reserve_publish_{fanout}"), |b| {
            b.iter(|| {
                let budget = budget();
                let mut retained = 0usize;
                for _ in 0..fanout {
                    let reply = ReplyPayload::copy_from_slice(payload.as_ref());
                    retained += reply.len();
                    black_box(reply);
                }
                black_box((budget, retained));
            })
        });
    }

    c.bench_function("stream_lease/cancel_1v64", |b| {
        b.iter(|| {
            let budget = budget();
            let mut leases = Vec::with_capacity(64);
            for _ in 0..64 {
                leases.push((ReplyPayload::copy_from_slice(payload.as_ref()), 4096usize));
            }
            black_box(leases.len());
            drop((budget, leases));
        })
    });
}

criterion_group!(benches, bench_stream_lease);
criterion_main!(benches);
