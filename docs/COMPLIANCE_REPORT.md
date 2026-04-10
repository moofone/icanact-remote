# Implementation Snapshot

This document is a lightweight status note for the current repository state. It intentionally avoids absolute claims such as "fully compliant" unless they are backed by reproducible verification in this checkout.

## Public API visibility

- `get_connection(...)` is not part of the public crate API exposed to callers.
- The public path for remote sends is lookup-driven and returns `RemoteActorRef`.
- This is exercised by `tests/test_new_lookup_api.rs`.

## Identity and TLS

- The crate uses Ed25519-derived identities for `SecretKey`, `NodeId`, and `PeerId`.
- TLS verification can reject mismatched peers with a `NodeId mismatch` error when an expected node identity is provided.
- The built-in bootstrap used by examples and tests is `BuilderTlsBootstrap`.

## Copy-sensitive paths

- The codebase includes `tell_bytes` and `ask_deferred` on `RemoteActorRef` and related low-copy helpers on `RemoteConnection`.
- Alignment-aware deserialization exists in the registry message path.
- Guard scripts still check for forbidden `rkyv::from_bytes` usage and for selected copy patterns.

Those facts are observable in the repository. They are stronger than vague marketing claims and weaker than a blanket statement that every messaging path is strict zero-copy.

## Validation scripts

`scripts/full_validation.sh` currently runs:

- isolated `ask_reply_end_to_end`
- workspace tests with retries
- `scripts/check_no_rkyv_from_bytes.sh`
- `scripts/check_forbidden_copy_patterns.sh`
- focused pointer tests
- focused streaming tests
- optional coverage gating if a plan path is provided

The script creates `baselines/`, `reports/`, and `logs/` on demand.

## Known doc cleanup reflected here

- Old references to `icanact-remote-transports` have been removed from the main docs because this repository already ships a built-in TLS bootstrap.
- Old references to missing zero-copy sprint artifacts and telemetry tests have been removed from script documentation.
