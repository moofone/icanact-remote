# Packed V5 data frames over authenticated V6 sessions

The filename and framing constants retain the **V5 packed data-layout** name.
They do not describe the current connection-negotiation version. Current TLS
configurations advertise and accept only **`icanact-remote-v6`**, and the Hello
exchange requires protocol version **6**. V5 ALPN/Hello negotiation is rejected;
there is no downgrade promise. See `src/tls/mod.rs` and `src/handshake.rs`.

## Control word and canonical frame table

Every packed data frame starts with a big-endian `u32` control word:

```text
31                         27 26                              0
+---------------------------+----------------------------------+
| dense WireKind (5 bits)   | body length after control (27)   |
+---------------------------+----------------------------------+
```

`body_len` counts every byte after the control word, including fixed metadata.
Writers reject bodies above `134,217,727`; readers also enforce the configured
`max_message_size`, valid kinds, nonzero body length and each kind's minimum
metadata length. These dense discriminants are **not** the legacy `MessageType`
repr values. All integers below are big-endian; header sizes include the
four-byte control word. Reserved bytes are emitted as zero.

| ID | WireKind | Header bytes | Body fields before payload |
|---:|---|---:|---|
| 0 | Gossip | 16 | reserved:12 |
| 1 | Ask | 16 | correlation_id:4, reserved:8 |
| 2 | Response | 16 | correlation_id:4, nack_marker:1, nack_reason:1, reserved:6 |
| 3 | ActorTell | 16 | actor_id:8, type_hash:4 |
| 4 | ActorAsk | 32 | correlation_id:4, actor_id:8, type_hash:4, optional_request_id:8, reserved:4 |
| 5 | StreamStart | 28 | stream_id:4, correlation_id:4, total_size:4, actor_id:8, type_hash:4 |
| 6 | StreamData | 12 | stream_id:4, chunk_index:4 |
| 7 | StreamResponseStart | 16 | stream_id:4, correlation_id:4, total_size:4 |
| 8 | StreamResponseData | 12 | stream_id:4, chunk_index:4 |
| 9 | DirectAsk | 16 | correlation_id:4, request_id:8 |
| 10 | DirectResponse | 16 | correlation_id:4, reserved:8 |
| 11 | PubSub | 16 | reserved:12 |
| 12 | StreamAbort | 12 | stream_id:4, reason:4 |
| 13 | RouteBind | 24 | route_slot:4, actor_id:8, type_hash:4, reserved:4; no payload |
| 14 | RoutedActorAsk | 16 | correlation_id:4, route_slot:4, reserved:4 |

`src/framing.rs::tests::wire_spec_table_matches_canonical_kinds_and_sizes`
checks this table against the canonical enum and header constants. Header
contents remain covered by the linked golden-byte tests below; this document
is not a second implementation of the codec.

## Request identity and responses

- `correlation_id` identifies the outstanding response slot; it is distinct from
  a caller-controlled stable `request_id`. Multiple asks may be in flight.
- ActorAsk request identity occupies full-frame bytes **20..28**, preserving the
  32-byte header and aligned payload offset. Zero encodes the absent/unmarked
  form; explicitly supplying `Some(0)` to its marked writer is rejected.
- DirectAsk request identity occupies full-frame bytes **8..16**. Its receive
  parser rejects zero or truncated identity metadata. DirectAsk framing/client
  and DirectResponse receive/correlation support remain active contracts even
  when the local production path answers an ask with a `NoDispatcher` NACK.
- Compact RoutedActorAsk refers to a connection-local RouteBind established
  before use; the route slot is not a globally portable actor identifier. It
  does not carry the full ActorAsk request-id field. Marked actor asks use the
  full header. Slots/state must not be reused across a fresh connection.
- A NACK uses **Response kind 2**, zero payload bytes, marker **1** at full-frame
  byte **8**, and reason at byte **9**. Reasons are `UnknownActor=1`,
  `HandlerError=2`, `NoDispatcher=3`, and `Backpressure=4`. An unknown reason with
  a set marker resolves as `Unsupported`, not as a successful empty response.
  An absent marker is an ordinary response, including a valid empty response.

Golden-byte/semantic tests in `src/framing.rs`:
`actor_ask_request_id_uses_reserved_bytes_without_changing_frame_size`,
`direct_ask_request_id_round_trips_through_the_headers_reserved_bytes`,
`routed_ask_is_sixteen_bytes_and_route_bind_is_exact`,
`ask_nack_round_trips_through_the_response_headers_reserved_bytes`, and
`a_nack_whose_reason_this_build_does_not_know_is_still_a_nack`.

## Alignment and streaming

Inline archived payload offsets are 16 bytes for ActorTell/RoutedActorAsk and
32 bytes for ActorAsk. Receivers must supply initialized, correctly aligned
storage before exposing ordinary byte slices or archived references; eventual
socket writes do not legitimize prior uninitialized slice exposure.

Stream start frames contain chunk zero. Subsequent chunks begin at index one;
successful completion is bitmap-driven. There is no packed StreamEnd kind.
Request/response streaming shares the connection's reassembly domain, so the
implementation's stream-ID ownership/partition rules must be preserved.
StreamAbort is a real control frame, not a synthetic end-of-stream payload.

## Session negotiation and compatibility

The Hello exchange is carried inside mutually authenticated TLS before data
framing. It conveys version, supported features, optional schema hash, and the
16-byte boot identity for the running process. The durable peer identity comes
from the verified certificate; the boot identifier is not independent identity
proof. Client-only role negotiation affects dial-back/supervisor behavior.

Schema compatibility requires exact equality of the peers' `Option<u64>`
values: `None` matches `None`, equal `Some(hash)` matches, mismatched hashes or
`None` versus `Some` fail. **Both hashes absent is not proof of ABI compatibility**
between independently evolved applications. No packed data frame carries a
schema hash. Typed payload type-hash validation and negotiated capability rules
still apply; do not infer support for an unknown kind from a shared ALPN alone.

`src/handshake.rs` tests cover accepted V6 negotiation, version/ALPN rejection,
schema equality/mismatch, role features and per-kind capability policy. Update
those tests and the frame-table/golden checks with any intentional wire change;
do not mechanically rename the packed V5 layout or weaken fail-closed checks.
