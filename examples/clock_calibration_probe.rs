use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Debug)]
struct Args {
    bind: SocketAddr,
    seed: String,
    peer: Option<SocketAddr>,
    peer_seed: Option<String>,
    duration: Duration,
    interval: Duration,
}

fn usage() -> &'static str {
    "usage: cargo run --example clock_calibration_probe -- --bind <ip:port> --seed <name> [--peer <ip:port> --peer-seed <name>] [--duration-sec <n>] [--interval-ms <n>]"
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut bind = None;
    let mut seed = None;
    let mut peer = None;
    let mut peer_seed = None;
    let mut duration = Duration::from_secs(30);
    let mut interval = Duration::from_millis(1_000);

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => bind = Some(args.next().ok_or("--bind requires a value")?.parse()?),
            "--seed" => seed = Some(args.next().ok_or("--seed requires a value")?),
            "--peer" => peer = Some(args.next().ok_or("--peer requires a value")?.parse()?),
            "--peer-seed" => peer_seed = Some(args.next().ok_or("--peer-seed requires a value")?),
            "--duration-sec" => {
                let seconds: u64 = args
                    .next()
                    .ok_or("--duration-sec requires a value")?
                    .parse()?;
                duration = Duration::from_secs(seconds);
            }
            "--interval-ms" => {
                let millis: u64 = args
                    .next()
                    .ok_or("--interval-ms requires a value")?
                    .parse()?;
                interval = Duration::from_millis(millis);
            }
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}\n{}", usage()).into()),
        }
    }

    if peer.is_some() != peer_seed.is_some() {
        return Err("--peer and --peer-seed must be provided together".into());
    }

    Ok(Args {
        bind: bind.ok_or_else(|| usage().to_string())?,
        seed: seed.ok_or_else(|| usage().to_string())?,
        peer,
        peer_seed,
        duration,
        interval,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let keypair = KeyPair::new_for_testing(&args.seed);
    let config = GossipConfig {
        gossip_interval: Duration::from_millis(250),
        cleanup_interval: Duration::from_secs(1),
        peer_retry_interval: Duration::from_millis(250),
        key_pair: Some(keypair.clone()),
        ..Default::default()
    };

    let handle = GossipRegistryHandle::new_with_transport_stack(
        args.bind,
        keypair.to_secret_key(),
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    println!(
        "clock_calibration_probe_started bind={} peer_id={}",
        handle.registry.bind_addr, handle.registry.peer_id
    );

    if let (Some(peer_addr), Some(peer_seed)) = (args.peer, args.peer_seed.as_deref()) {
        let peer_keypair = KeyPair::new_for_testing(peer_seed);
        let peer_id = peer_keypair.peer_id();
        handle.add_peer(&peer_id).await.connect(&peer_addr).await?;
        println!(
            "clock_calibration_probe_connected peer={} peer_id={}",
            peer_addr, peer_id
        );
    }

    let deadline = Instant::now() + args.duration;
    while Instant::now() < deadline {
        let snapshots = handle.peer_clock_snapshots();
        if snapshots.is_empty() {
            println!("clock_calibration_sample status=waiting sample_count=0");
        } else {
            for snapshot in snapshots {
                let age_ns = snapshot.sample_age_ns(icanact_remote::current_timestamp_nanos());
                println!(
                    "clock_calibration_sample peer={} sample_id={} offset_ns={} offset_ms={:.6} rtt_ns={} rtt_ms={:.6} error_bound_ns={} age_ns={} sample_count={}",
                    snapshot.peer_addr,
                    snapshot.sample_id,
                    snapshot.offset_ns,
                    snapshot.offset_ns as f64 / 1_000_000.0,
                    snapshot.rtt_ns,
                    snapshot.rtt_ns as f64 / 1_000_000.0,
                    snapshot.error_bound_ns,
                    age_ns,
                    snapshot.sample_count,
                );
            }
        }
        sleep(args.interval).await;
    }

    handle.shutdown().await;
    Ok(())
}
