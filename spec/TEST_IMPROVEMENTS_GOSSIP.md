# Gossip Test Improvements

This file is a planning note, not a statement of current repository status.

## Current state in this checkout

- The repository already contains active gossip-focused integration tests such as:
  - `tests/gossip_matrix_e2e.rs`
  - `tests/gossip_e2e_tests.rs`
  - `tests/gossip_removal_e2e.rs`
  - `tests/gossip_partition_e2e.rs`
  - `tests/gossip_partition_invariant_e2e.rs`
  - `tests/gossip_chaos_scheduler.rs`
- `tests/unit/*` is already wired through `tests/unit_tests.rs`, so the earlier “Cargo does not compile these yet” note is no longer accurate.
- The repo path for this checkout is `/Users/greg/Dev/git/icanact-remote`.

## What still makes sense from the plan

The broad goals in the original draft remain reasonable:

- prove deterministic convergence under reorder and duplication
- avoid tests that accidentally create connections and invalidate partition assertions
- verify safe fallback behavior when history is insufficient
- keep chaos-style tests deterministic and low-flake

## Historical note

Earlier drafts of this spec referenced future tasks such as adding a top-level `tests/unit.rs` entrypoint and additional scheduler-based harnessing. Some of that work has already landed in a different shape, and some proposed files or tasks are no longer present in this checkout. Treat the old task list as historical planning context, not an implementation checklist for the current tree.
