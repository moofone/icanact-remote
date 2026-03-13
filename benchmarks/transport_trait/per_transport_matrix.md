# Transport Trait Per-Transport Matrix

- Run timestamp (UTC): 2026-02-22T19:40:00Z
- Methodology for this update: post-setup routed tell delivery-verified runs, `--release`, messages=20000, warmup=2000, payload=128B, `n=3` per transport.
- `Tell msg/s` uses delivery-verified sender `e2e_msgs_per_sec`.
- `Send-loop msg/s` uses sender `send_msgs_per_sec` from the same delivery-verified runs.

## Spot-Check Comparison (UDP vs TCP-Noise)

| Transport | Tell msg/s (delivered, median n=3) | Send-loop msg/s (median n=3) | Notes |
|---|---:|---:|---|
| `TcpNoiseStack` | 4,213,371.89 | 35,539,760.11 | plain TCP + Noise auth |
| `UdpStack` | 4,831,793.20 | 58,809,000.13 | native UDP socket dataplane with lock-free writer queue + coalescing profile by peer type |

## Notes

- UDP path is native datagram transport (`UdpSocket` ingress + vectored datagram egress), not a stream adapter.
- With current UDP queue/coalescing tuning, UDP now leads TCP-noise on delivered tell throughput in this benchmark window (`+14.68%` median).
- UDP also leads on send-loop throughput in the same run window (`+65.47%` median).
