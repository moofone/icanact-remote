# Legacy inventory

Current-source classification for the QA review items. This is not an acceptance record and it does not say that no legacy remains. Measured non-regression is still missing for every inherited cleanup. F08–F10 are deferred and were not left in this branch. Nothing in this file is a speedup claim.

Scan date: 2026-10-09, branch `qa/post-merge-discovery-measurement`. Rust search excluded `target/`. A name with zero Rust files means no `.rs` match in this tree, including tests. Docs may still mention a deleted name.

## Classification

| Item | Class | Evidence in this tree | What is not closed |
|---|---|---|---|
| F01 receive initialization | Retained live contract | `PooledAlignedBuffer::with_len` zero-fills (`src/aligned.rs:29`). Public `with_len_uninit` (`src/aligned.rs:48`) delegates to that function. | Miri is not installed. Cost of the zero-fill is unmeasured. The unsafe signature stays for compatibility. |
| F02 validation reporting | Retained live contract | `scripts/full_validation.sh` keeps command output, inventories, and executed counts. Focused success is not full validation. | Not an optimization. Remote CI after the merge was not re-run here. |
| F03 obsolete writers | Removed from Rust, measurement blocked | `write_all_vectored`, `write_chunks_all`, and `poll_write_chunks_copy` match no Rust file. Cleanup report R1 describes the same removal. | No individual A/B. Assertion-migration evidence is the cleanup report, not a new experiment. |
| F04 duplicate reader | Canonical reader retained; copied body reported removed | Cleanup report R2. This scan did not re-walk every read call site. | No measured non-regression. |
| F05 empty direct batch | Removed from Rust, measurement blocked | `DirectResponseBatch` matches no Rust file. Direct-ask NACK handling remains. | No allocation or duplex A/B. Docs still name the old type. |
| F06 speculative parser states | Removed from Rust, measurement blocked | `FastReadOutcome` and `ReadIoResult` match no Rust file. Registry comments that say "speculative" are inbound-claim text, not those parser states. | No measured non-regression. |
| F07 writer consolidation | Retained live contract | `poll_pending_ordinary` and `write_chunks_once` in `src/connection_pool/stream_writer.rs` are the ordinary write path. Cleanup report R5. | No measured non-regression. F08 stays behind this gap. |
| F08 chunk cursor | Blocked | `skip_written_chunks` is still the progress scan (`src/connection_pool/stream_writer.rs:951`). There is no chunk index. The cleanup report already recorded a no-change decision for cursor/cache state. | No partial-write A/B. Not implemented, not accepted, and not freshly rejected by a new experiment. |
| F09 stale connection snapshot | Deferred | Release builds still store `connection` as a private field (`src/remote_actor_ref.rs`, the `cfg(not(any(test, feature = "test-helpers", debug_assertions)))` field). Test and helper builds keep the public diagnostic field. `connection_slot` is the live slot. | Not changed on this branch. No reclamation measurement. |
| F10 PubSub copies | Deferred | `deliver_local_borrowed` still builds a separate `Bytes` for each matching subscriber category. | Not changed on this branch. No copy-count or timing measurement. |
| F11 dependencies | Retained narrowing | Cleanup report R7. Lock hash used by the later matrices is `19c5ebe1eeb2cc3015e6dd6eb317fc306b58b13c47ad3135ad93b0c28b47f208`. | No build-time or runtime A/B. |
| F12 wire specification | Retained documentation | `spec/WIRE_V5.md` plus the framing table test. | Not a performance change. |
| S13 NACK admission | Retained safety contract | Pending NACK admission is fail-closed at cap 64. See `docs/SECURITY_NACK_CAPACITY_FIX.md`. | Cost unmeasured. |
| D01, D08, D09, D11, D12 | Retained correctness | D08–D12 are the production and fixture changes on this branch. D01 is already on main. D10 is test-only. | No performance A/B. Historical discovery failures stay retained. |
| Disabled negative-API example | Documented disposition; compile-fail test not added | `examples/illegal_api_usage.rs.disabled` calls `GossipRegistry::get_connection`. That method is `pub(crate)` (`src/registry.rs:11802`), as are the other `get_connection` entry points. The `.disabled` file is not a Cargo target. `docs/DESIGN_TRUTH.md` and `docs/COMPLIANCE_REPORT.md` already say it is not public API. | No trybuild/compile-fail test. In-crate callers remain. This file is not evidence that the method was deleted. |

## Explicit non-claims

- E0 stays open. The harness exists. The closed-loop A/A and the frozen 1,000/s offered-load A/A both miss the 3% latency gates.
- Validation v6 covers D08–D12. It does not cover the later offered-load fixture. F09 and F10 are unchanged from main.
- Final safe-baseline versus final-tree comparison has not been run.
- Supported-platform CI and Miri were not run.
