// NOTE: Older unit tests in this repo were written against pre-rkyv wire formats and older
// public APIs (e.g. `ActorLocation`, `bincode`). Rather than keep those broken/stale tests in
// the build, we maintain a small set of unit tests aligned to the current gossip/registry wire.
//
// Gossip/registry behavior is primarily validated through the e2e/integration tests in `tests/`
// and the internal deterministic tests in `src/registry.rs`.
pub mod gossip_wire;
