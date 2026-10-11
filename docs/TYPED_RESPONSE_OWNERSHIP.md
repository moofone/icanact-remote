# Native typed response ownership

Typed connection asks retain the native aligned receive owner from correlation
completion through decoding. `ConnectionHandle::ask_typed_archived` and its
custom-timeout variant return `ArchivedBytes<T>` without promoting that owner to
shared `Bytes` metadata. `ask_typed` and its custom-timeout variant deserialize
directly from the native response slice; deserializing an allocating value can
still allocate its result.

`decode_typed_archived_owned::<T>(AlignedBytes)` is the native archived decoder.
Its wrapper preserves the original pointer, owner, subview and complete retained
allocation capacity. `ArchivedBytes::retained_capacity()` reports that capacity
when known. A pooled receive checkout remains held until the archived wrapper is
dropped or explicitly transferred. Moving the wrapper does not acquire another
pool-owner reference or allocate a per-response owner box.

`decode_typed_archived::<T>(Bytes)` remains a compatibility decoder. It preserves
caller-owned shared bytes and their existing archive-alignment checks; arbitrary
`Bytes` cannot prove the underlying allocation capacity, so its wrapper reports
`None`. `ArchivedBytes::into_bytes()` is an explicit compatibility boundary which
can allocate shared-owner metadata for native backing. It returns the wire bytes,
including their type prefix. `as_bytes()` returns only the archived body.

Both decoders require the same unconditional wire type hash, supported archive
alignment and valid rkyv archive in debug and release builds. The native path does
not copy or realign malformed input to bypass validation. Failure releases its
owner, including receive-pool admission. These changes implement REMOTE-3 without
changing wire framing, correlation identity, deadlines or transport-liveness rules.

The tests `native_archived_response_retains_pointer_capacity_and_pool_checkout`,
`native_archived_response_rejects_bad_hash_and_body_and_returns_owner`, and
`typed_asks_deliver_native_archives_without_response_promotion` cover unique and
pooled owners, spare capacity, nonzero subviews, exact pointer preservation,
rejection cleanup, both public archived ask variants and actual writer-frame
progress. Allocation counters have explicit `Bytes` promotion positive controls.
They are authored for the unchanged host feature matrix; static authorship alone
is not evidence of an executed allocation or performance result. Whole-path
architecture acceptance remains UNPROVED until the required host evidence exists.
