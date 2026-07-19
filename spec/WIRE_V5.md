# icanact-remote wire V5

Every V5 frame starts with a big-endian `u32` control word:

```text
31                         27 26                              0
+---------------------------+----------------------------------+
| dense WireKind (5 bits)   | body length after control (27)   |
+---------------------------+----------------------------------+
```

Unknown kinds and bodies larger than `134,217,727` are rejected. All integer
fields below are big-endian. Header sizes include the four-byte control word.

| Kind | Header | Body fields before payload |
|---|---:|---|
| Gossip | 16 | reserved:12 |
| Ask / Response | 16 | correlation_id:4, reserved:8 |
| ActorTell | 16 | actor_id:8, type_hash:4 |
| ActorAsk | 32 | correlation_id:4, actor_id:8, type_hash:4, reserved:12 |
| DirectAsk / DirectResponse | 16 | correlation_id:4, reserved:8 |
| PubSub | 16 | reserved:12 |
| StreamStartData | 28 | stream_id:4, correlation_id:4, total_size:4, actor_id:8, type_hash:4 |
| StreamResponseStartData | 16 | stream_id:4, correlation_id:4, total_size:4 |
| StreamData / StreamResponseData | 12 | stream_id:4, chunk_index:4 |
| StreamAbort | 12 | stream_id:4, reason:4 |

Inline archived payload offsets are 16 bytes for ActorTell and 32 bytes for
ActorAsk. Stream start frames contain chunk zero. Subsequent chunks begin at
index one; successful completion is bitmap-driven and there is no StreamEnd.

Schema compatibility is negotiated once in the authenticated V5 Hello. Both
`Option<u64>` values must match exactly. No V5 data frame carries a schema
hash. TLS negotiation advertises and accepts only V5 ALPN.
