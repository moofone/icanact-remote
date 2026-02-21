use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize)]
struct BenchPayload {
    data: [u8; 256],
}

fn make_payload() -> AlignedVec<{ icanact_remote::PAYLOAD_ALIGNMENT }> {
    let value = BenchPayload { data: [7u8; 256] };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value).expect("serialize payload");
    let mut aligned = AlignedVec::<{ icanact_remote::PAYLOAD_ALIGNMENT }>::new();
    aligned.extend_from_slice(bytes.as_ref());
    aligned
}

fn bench_archived_access(c: &mut Criterion) {
    let payload = make_payload();
    let bytes = payload.as_ref();

    let mut latency = c.benchmark_group("icanact_remote_archived_access_latency");
    latency.bench_function("validated", |b| {
        b.iter(|| {
            let archived = rkyv::access::<rkyv::Archived<BenchPayload>, rkyv::rancor::Error>(bytes)
                .expect("validated access");
            black_box(archived);
        });
    });

    latency.bench_function("trusted", |b| {
        b.iter(|| {
            let archived = unsafe { rkyv::access_unchecked::<rkyv::Archived<BenchPayload>>(bytes) };
            black_box(archived);
        });
    });
    latency.finish();

    let mut throughput = c.benchmark_group("icanact_remote_archived_access_throughput");
    let batch = 1024usize;
    throughput.throughput(Throughput::Bytes((bytes.len() * batch) as u64));

    throughput.bench_function("validated", |b| {
        b.iter(|| {
            for _ in 0..batch {
                let archived =
                    rkyv::access::<rkyv::Archived<BenchPayload>, rkyv::rancor::Error>(bytes)
                        .expect("validated access");
                black_box(archived);
            }
        });
    });

    throughput.bench_function("trusted", |b| {
        b.iter(|| {
            for _ in 0..batch {
                let archived =
                    unsafe { rkyv::access_unchecked::<rkyv::Archived<BenchPayload>>(bytes) };
                black_box(archived);
            }
        });
    });
    throughput.finish();
}

criterion_group!(benches, bench_archived_access);
criterion_main!(benches);
