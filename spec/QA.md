# QA Plan

This file is a high-level QA planning artifact. It is not a live inventory of the current codebase.

## Current repository facts

- Repo path in this checkout: `/Users/greg/Dev/git/icanact-remote`
- Rust edition in `Cargo.toml`: `2024`
- `ask()` is implemented through the connection/correlation machinery in the current tree.
- `tests/unit/*` is already wired via `tests/unit_tests.rs`.

## How to read this document

Use it as a set of QA themes:

- deterministic gossip behavior
- bounded resources and shutdown behavior
- regression evidence for hot-path changes
- explicit scans for synchronization primitives and task lifecycle concerns

Do not read the older phase/task language here as a guaranteed description of unfinished work in the current checkout. Several references in the earlier draft targeted files, reports, or future cleanup steps that are not present anymore.

## Practical current checks

For the current repository, the most concrete verification surfaces are:

- `cargo test`
- `scripts/full_validation.sh`
- focused integration tests under `tests/`
- targeted source scans with `rg`

If this plan is revived for active QA work, it should be regenerated from the current tree before being used as a checklist.
