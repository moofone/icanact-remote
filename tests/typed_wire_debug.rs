// The type-hash prefix (and its verification) is unconditional in every
// build mode -- see typed::encode_typed -- so these round-trip checks are no
// longer debug-only and must also run under `cargo test --release`.
mod tests {
    use bytes::Buf;
    use icanact_remote::{decode_typed, typed, wire_type};

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Ping {
        id: u64,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct Pong {
        id: u64,
    }

    wire_type!(Ping, "icanact.remote.Ping");
    wire_type!(Pong, "icanact.remote.Pong");

    fn encode_wire_bytes<T>(msg: &T) -> bytes::Bytes
    where
        T: typed::WireEncode,
    {
        let payload = typed::encode_typed_pooled(msg).expect("encode_typed_pooled should succeed");
        let (mut payload, prefix, payload_len) = typed::typed_payload_parts::<T>(payload);
        let mut wire = bytes::BytesMut::with_capacity(payload_len);
        if let Some(prefix) = prefix {
            wire.extend_from_slice(&prefix);
        }
        let payload_bytes = payload.copy_to_bytes(payload.remaining());
        wire.extend_from_slice(payload_bytes.as_ref());
        wire.freeze()
    }

    #[test]
    fn typed_roundtrip_ok() {
        let msg = Ping { id: 7 };
        let payload = encode_wire_bytes(&msg);
        let decoded: Ping = decode_typed(payload.as_ref()).expect("decode_typed should succeed");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn typed_hash_mismatch_errors() {
        let msg = Ping { id: 42 };
        let payload = encode_wire_bytes(&msg);
        let err = decode_typed::<Pong>(payload.as_ref()).unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("hash mismatch"),
            "expected hash mismatch error, got: {err_str}"
        );
    }
}
