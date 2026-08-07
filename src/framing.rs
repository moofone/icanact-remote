use crate::MessageType;

/// Every V5 frame begins with one big-endian control word: kind:5 | body_len:27.
/// body_len counts all bytes after the word.
pub const LENGTH_PREFIX_LEN: usize = 4;
pub const CONTROL_KIND_BITS: u32 = 5;
pub const CONTROL_BODY_LEN_BITS: u32 = 32 - CONTROL_KIND_BITS;
pub const CONTROL_BODY_LEN_MASK: u32 = (1 << CONTROL_BODY_LEN_BITS) - 1;

/// Fixed bytes after the control word.
pub const ASK_RESPONSE_HEADER_LEN: usize = 12;
pub const GOSSIP_HEADER_LEN: usize = 12;
pub const ACTOR_TELL_HEADER_LEN: usize = 12;
pub const ACTOR_ASK_HEADER_LEN: usize = 28;
pub const ROUTED_ACTOR_ASK_HEADER_LEN: usize = 12;
pub const ROUTE_BIND_HEADER_LEN: usize = 20;
pub const DIRECT_ASK_HEADER_LEN: usize = 12;
pub const DIRECT_RESPONSE_HEADER_LEN: usize = 12;
pub const PUBSUB_HEADER_LEN: usize = 12;
pub const STREAM_REQUEST_START_HEADER_LEN: usize = 24;
pub const STREAM_RESPONSE_START_HEADER_LEN: usize = 12;
pub const STREAM_DATA_HEADER_LEN: usize = 8;
pub const STREAM_REQUEST_START_FRAME_HEADER_LEN: usize =
    LENGTH_PREFIX_LEN + STREAM_REQUEST_START_HEADER_LEN;
pub const STREAM_RESPONSE_START_FRAME_HEADER_LEN: usize =
    LENGTH_PREFIX_LEN + STREAM_RESPONSE_START_HEADER_LEN;
pub const STREAM_DATA_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + STREAM_DATA_HEADER_LEN;

pub const ASK_RESPONSE_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN;
pub const GOSSIP_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN;
pub const ACTOR_TELL_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ACTOR_TELL_HEADER_LEN;
pub const ACTOR_ASK_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ACTOR_ASK_HEADER_LEN;
pub const ROUTED_ACTOR_ASK_FRAME_HEADER_LEN: usize =
    LENGTH_PREFIX_LEN + ROUTED_ACTOR_ASK_HEADER_LEN;
pub const ROUTE_BIND_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ROUTE_BIND_HEADER_LEN;
pub const DIRECT_ASK_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + DIRECT_ASK_HEADER_LEN;
pub const DIRECT_RESPONSE_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + DIRECT_RESPONSE_HEADER_LEN;
pub const PUBSUB_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + PUBSUB_HEADER_LEN;

const _: () = assert!(ACTOR_TELL_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(ACTOR_ASK_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(ROUTED_ACTOR_ASK_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(ASK_RESPONSE_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(GOSSIP_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(DIRECT_ASK_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(DIRECT_RESPONSE_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(PUBSUB_FRAME_HEADER_LEN % 16 == 0);

/// Dense V5 kinds deliberately do not reuse MessageType's legacy repr(u8).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireKind {
    Gossip = 0,
    Ask = 1,
    Response = 2,
    ActorTell = 3,
    ActorAsk = 4,
    StreamStart = 5,
    StreamData = 6,
    StreamResponseStart = 7,
    StreamResponseData = 8,
    DirectAsk = 9,
    DirectResponse = 10,
    PubSub = 11,
    StreamAbort = 12,
    RouteBind = 13,
    RoutedActorAsk = 14,
}

impl WireKind {
    /// Every dense V5 kind, in discriminant order. Single source of truth so
    /// tests that must cover "every `WireKind`" (control-encoding coverage,
    /// the capability-gating guard in `handshake.rs`) can't drift from each
    /// other by editing one list and forgetting another.
    ///
    /// Only referenced from `#[cfg(test)]` code today (no production call
    /// site iterates every kind), which a non-test `cargo clippy` build
    /// reports as dead code.
    #[allow(dead_code)]
    pub(crate) const ALL: [WireKind; 15] = [
        Self::Gossip,
        Self::Ask,
        Self::Response,
        Self::ActorTell,
        Self::ActorAsk,
        Self::StreamStart,
        Self::StreamData,
        Self::StreamResponseStart,
        Self::StreamResponseData,
        Self::DirectAsk,
        Self::DirectResponse,
        Self::PubSub,
        Self::StreamAbort,
        Self::RouteBind,
        Self::RoutedActorAsk,
    ];

    pub const fn from_message_type(msg_type: MessageType) -> Option<Self> {
        match msg_type {
            MessageType::Gossip => Some(Self::Gossip),
            MessageType::Ask => Some(Self::Ask),
            MessageType::Response => Some(Self::Response),
            MessageType::ActorTell => Some(Self::ActorTell),
            MessageType::ActorAsk => Some(Self::ActorAsk),
            MessageType::StreamStart => Some(Self::StreamStart),
            MessageType::StreamData => Some(Self::StreamData),
            MessageType::StreamResponseStart => Some(Self::StreamResponseStart),
            MessageType::StreamResponseData => Some(Self::StreamResponseData),
            MessageType::DirectAsk => Some(Self::DirectAsk),
            MessageType::DirectResponse => Some(Self::DirectResponse),
            MessageType::PubSub => Some(Self::PubSub),
            MessageType::StreamEnd | MessageType::StreamResponseEnd => None,
        }
    }

    pub const fn message_type(self) -> MessageType {
        match self {
            Self::Gossip => MessageType::Gossip,
            Self::Ask => MessageType::Ask,
            Self::Response => MessageType::Response,
            Self::ActorTell => MessageType::ActorTell,
            Self::ActorAsk => MessageType::ActorAsk,
            Self::StreamStart => MessageType::StreamStart,
            Self::StreamData => MessageType::StreamData,
            Self::StreamResponseStart => MessageType::StreamResponseStart,
            Self::StreamResponseData => MessageType::StreamResponseData,
            Self::DirectAsk => MessageType::DirectAsk,
            Self::DirectResponse => MessageType::DirectResponse,
            Self::PubSub => MessageType::PubSub,
            Self::StreamAbort => MessageType::StreamEnd,
            Self::RouteBind | Self::RoutedActorAsk => MessageType::ActorAsk,
        }
    }

    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Gossip),
            1 => Some(Self::Ask),
            2 => Some(Self::Response),
            3 => Some(Self::ActorTell),
            4 => Some(Self::ActorAsk),
            5 => Some(Self::StreamStart),
            6 => Some(Self::StreamData),
            7 => Some(Self::StreamResponseStart),
            8 => Some(Self::StreamResponseData),
            9 => Some(Self::DirectAsk),
            10 => Some(Self::DirectResponse),
            11 => Some(Self::PubSub),
            12 => Some(Self::StreamAbort),
            13 => Some(Self::RouteBind),
            14 => Some(Self::RoutedActorAsk),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Control {
    pub kind: WireKind,
    pub body_len: usize,
}

#[inline]
pub fn checked_body_len(fixed_len: usize, payload_len: usize) -> usize {
    fixed_len
        .checked_add(payload_len)
        .filter(|len| *len <= CONTROL_BODY_LEN_MASK as usize)
        .expect("frame body length exceeds V5 27-bit limit")
}

#[inline]
pub fn encode_control(kind: WireKind, body_len: usize) -> [u8; LENGTH_PREFIX_LEN] {
    assert!(
        body_len <= CONTROL_BODY_LEN_MASK as usize,
        "frame body length exceeds V5 27-bit limit"
    );
    let word = ((kind as u32) << CONTROL_BODY_LEN_BITS) | body_len as u32;
    word.to_be_bytes()
}

#[inline]
pub fn decode_control(bytes: [u8; LENGTH_PREFIX_LEN]) -> Option<Control> {
    let word = u32::from_be_bytes(bytes);
    let kind = WireKind::from_u8((word >> CONTROL_BODY_LEN_BITS) as u8)?;
    Some(Control {
        kind,
        body_len: (word & CONTROL_BODY_LEN_MASK) as usize,
    })
}

#[inline]
fn init_header<const N: usize>(kind: WireKind, body_len: usize) -> [u8; N] {
    let mut header = [0u8; N];
    header[..LENGTH_PREFIX_LEN].copy_from_slice(&encode_control(kind, body_len));
    header
}

/// V5 actor tell header. Payload begins at offset 16 for every inline size.
pub fn write_actor_tell_header(
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
) -> [u8; ACTOR_TELL_FRAME_HEADER_LEN] {
    let body_len = checked_body_len(ACTOR_TELL_HEADER_LEN, payload_len);
    let mut header = init_header(WireKind::ActorTell, body_len);
    header[4..12].copy_from_slice(&actor_id.to_be_bytes());
    header[12..16].copy_from_slice(&type_hash.to_be_bytes());
    header
}

/// V5 actor ask header. The trailing pad preserves a 16-byte payload offset.
pub fn write_actor_ask_header(
    correlation_id: u32,
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
) -> [u8; ACTOR_ASK_FRAME_HEADER_LEN] {
    let body_len = checked_body_len(ACTOR_ASK_HEADER_LEN, payload_len);
    let mut header = init_header(WireKind::ActorAsk, body_len);
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[8..16].copy_from_slice(&actor_id.to_be_bytes());
    header[16..20].copy_from_slice(&type_hash.to_be_bytes());
    header
}

/// V5 compact ask after its route was bound on this connection.
pub fn write_routed_actor_ask_header(
    correlation_id: u32,
    route_slot: u32,
    payload_len: usize,
) -> [u8; ROUTED_ACTOR_ASK_FRAME_HEADER_LEN] {
    let mut header = init_header(
        WireKind::RoutedActorAsk,
        checked_body_len(ROUTED_ACTOR_ASK_HEADER_LEN, payload_len),
    );
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[8..12].copy_from_slice(&route_slot.to_be_bytes());
    header
}

/// Establishes a connection-scoped slot before a routed ask uses it.
pub fn write_route_bind_header(
    route_slot: u32,
    actor_id: u64,
    type_hash: u32,
) -> [u8; ROUTE_BIND_FRAME_HEADER_LEN] {
    let mut header = init_header(WireKind::RouteBind, ROUTE_BIND_HEADER_LEN);
    header[4..8].copy_from_slice(&route_slot.to_be_bytes());
    header[8..16].copy_from_slice(&actor_id.to_be_bytes());
    header[16..20].copy_from_slice(&type_hash.to_be_bytes());
    header
}

pub fn write_ask_response_header(
    msg_type: MessageType,
    correlation_id: u32,
    payload_len: usize,
) -> [u8; ASK_RESPONSE_FRAME_HEADER_LEN] {
    let kind = match msg_type {
        MessageType::Ask => WireKind::Ask,
        MessageType::Response => WireKind::Response,
        _ => panic!("ask/response header requires Ask or Response"),
    };
    let body_len = checked_body_len(ASK_RESPONSE_HEADER_LEN, payload_len);
    let mut header = init_header(kind, body_len);
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header
}

/// Machine-readable reason an ask could not be answered with data. Carried in
/// a Response frame's `ASK_RESPONSE_HEADER_LEN` fixed region, which has
/// always been zero-padded past the correlation id (no application payload
/// has ever been placed there), so a NACK costs no extra frame or round trip.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskNackReason {
    /// No actor/handler is wired up to answer this ask at all.
    UnknownActor = 1,
    /// A handler was invoked and returned an application error.
    HandlerError = 2,
    /// This connection/build has no dispatcher for the ask's wire path
    /// (e.g. a raw or direct ask with no registered handler concept).
    NoDispatcher = 3,
    /// The peer set the NACK marker with a reason byte this build does not
    /// recognize -- a newer peer's reason, or a corrupted frame. Decode-only:
    /// never written to the wire, so the reserved 0 discriminant cannot
    /// collide with a reason a future version assigns.
    Unsupported = 0,
}

impl AskNackReason {
    /// Total by construction: every byte maps to a reason, so a frame whose
    /// NACK marker is set can never decode as an ordinary response. An
    /// unrecognized byte degrades to `Unsupported` rather than falling
    /// through to success, and stays forward-compatible -- a newer peer's
    /// reason still resolves the waiter with an error instead of tearing
    /// down the connection.
    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::UnknownActor,
            2 => Self::HandlerError,
            3 => Self::NoDispatcher,
            _ => Self::Unsupported,
        }
    }
}

impl std::fmt::Display for AskNackReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::UnknownActor => "unknown actor",
            Self::HandlerError => "handler error",
            Self::NoDispatcher => "no dispatcher for this ask path",
            Self::Unsupported => "refused with a reason this build does not recognize",
        };
        f.write_str(text)
    }
}

/// Byte offset, within the Response frame's fixed region (i.e. relative to
/// `body[0]`, right after the control word), of the NACK marker. Offset 0..4
/// is the correlation id; this is the first previously-unused padding byte.
const ASK_NACK_FLAG_BODY_OFFSET: usize = 4;
const ASK_NACK_REASON_BODY_OFFSET: usize = 5;
const ASK_NACK_FLAG_SET: u8 = 1;

/// Build a Response frame that NACKs an ask instead of answering it: same
/// kind and header shape as a normal response, zero-length payload, with the
/// reason packed into the header's reserved bytes.
pub fn write_ask_nack_header(
    correlation_id: u32,
    reason: AskNackReason,
) -> [u8; ASK_RESPONSE_FRAME_HEADER_LEN] {
    let mut header = init_header(WireKind::Response, ASK_RESPONSE_HEADER_LEN);
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[LENGTH_PREFIX_LEN + ASK_NACK_FLAG_BODY_OFFSET] = ASK_NACK_FLAG_SET;
    header[LENGTH_PREFIX_LEN + ASK_NACK_REASON_BODY_OFFSET] = reason as u8;
    header
}

/// Inspect a Response frame's body (the bytes after the control word,
/// starting at the correlation id) for the NACK marker. Returns `None` for
/// an ordinary response, including every response written before this
/// marker existed (that padding was always zeroed).
///
/// `None` means exactly one thing -- the marker is absent -- so a set marker
/// always yields a NACK. An unrecognized reason byte resolves to
/// `Unsupported` instead of `None`; conflating the two would deliver a
/// rejection to the caller as a successful empty response.
pub fn ask_nack_reason(response_fixed_region: &[u8]) -> Option<AskNackReason> {
    if response_fixed_region.len() < ASK_RESPONSE_HEADER_LEN {
        return None;
    }
    if response_fixed_region[ASK_NACK_FLAG_BODY_OFFSET] != ASK_NACK_FLAG_SET {
        return None;
    }
    Some(AskNackReason::from_u8(
        response_fixed_region[ASK_NACK_REASON_BODY_OFFSET],
    ))
}

pub fn write_gossip_frame_prefix(payload_len: usize) -> [u8; GOSSIP_FRAME_HEADER_LEN] {
    init_header(
        WireKind::Gossip,
        checked_body_len(GOSSIP_HEADER_LEN, payload_len),
    )
}

pub fn write_pubsub_frame_prefix(payload_len: usize) -> [u8; PUBSUB_FRAME_HEADER_LEN] {
    init_header(
        WireKind::PubSub,
        checked_body_len(PUBSUB_HEADER_LEN, payload_len),
    )
}

/// Build a DirectAsk frame header. `request_id` is a stable identifier the
/// caller controls, unlike `correlation_id`, which is a connection-local slot
/// index recycled on every reconnect. `(peer_id, request_id)` is the identity
/// a receiver would need to recognize "this is the same logical ask retried
/// after a transport reset" rather than treating the retry as a new request
/// and executing it twice.
///
/// **No receiver does that today, and none can.** DirectAsk has no registered
/// application handler in any build mode -- every read path answers it with
/// `AskNackReason::NoDispatcher` -- so nothing reaches a point where this id
/// could be consulted. It is wire surface kept ready for a future DirectAsk
/// dispatcher, not a capability in use.
///
/// In particular this is *not* what makes the actor-ask path idempotent.
/// `WireKind::ActorAsk` carries no `request_id` (see
/// `write_actor_ask_header`), and that is the frame real ask traffic uses.
/// Dedupe on that path keys on an identity carried inside the payload
/// instead, so it needs nothing from this header.
///
/// Occupies bytes the frame has always reserved and zeroed after
/// `correlation_id`, so it costs no extra frame. Fail-closed: `request_id`
/// must be nonzero (see `direct_ask_request_id` on the read side); 0 is
/// reserved to mean "absent" and is rejected rather than silently accepted
/// as a valid-looking id that could collide across independent asks.
pub fn write_direct_ask_header(
    correlation_id: u32,
    request_id: u64,
    payload_len: usize,
) -> [u8; DIRECT_ASK_FRAME_HEADER_LEN] {
    let body_len = checked_body_len(DIRECT_ASK_HEADER_LEN, payload_len);
    let mut header = init_header(WireKind::DirectAsk, body_len);
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[8..16].copy_from_slice(&request_id.to_be_bytes());
    header
}

/// Read back the stable request id `write_direct_ask_header` wrote, from a
/// DirectAsk frame's body (the bytes after the control word, starting at the
/// correlation id). Fail-closed: `None` both for a truncated body and for
/// the reserved-zero sentinel (`request_id == 0`), so a caller can't
/// mistake "absent" for a valid id.
pub fn direct_ask_request_id(body: &[u8]) -> Option<u64> {
    if body.len() < DIRECT_ASK_HEADER_LEN {
        return None;
    }
    let request_id = u64::from_be_bytes(body[4..12].try_into().unwrap());
    if request_id == 0 {
        None
    } else {
        Some(request_id)
    }
}

pub fn write_direct_response_header(
    correlation_id: u32,
    payload_len: usize,
) -> [u8; DIRECT_RESPONSE_FRAME_HEADER_LEN] {
    let body_len = checked_body_len(DIRECT_RESPONSE_HEADER_LEN, payload_len);
    let mut header = init_header(WireKind::DirectResponse, body_len);
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header
}

pub fn write_stream_request_start_header(
    stream_id: u32,
    correlation_id: u32,
    total_size: u32,
    actor_id: u64,
    type_hash: u32,
    first_chunk_len: usize,
) -> [u8; STREAM_REQUEST_START_FRAME_HEADER_LEN] {
    let mut header = init_header(
        WireKind::StreamStart,
        checked_body_len(STREAM_REQUEST_START_HEADER_LEN, first_chunk_len),
    );
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&correlation_id.to_be_bytes());
    header[12..16].copy_from_slice(&total_size.to_be_bytes());
    header[16..24].copy_from_slice(&actor_id.to_be_bytes());
    header[24..28].copy_from_slice(&type_hash.to_be_bytes());
    header
}

pub fn write_stream_response_start_header(
    stream_id: u32,
    correlation_id: u32,
    total_size: u32,
    first_chunk_len: usize,
) -> [u8; STREAM_RESPONSE_START_FRAME_HEADER_LEN] {
    let mut header = init_header(
        WireKind::StreamResponseStart,
        checked_body_len(STREAM_RESPONSE_START_HEADER_LEN, first_chunk_len),
    );
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&correlation_id.to_be_bytes());
    header[12..16].copy_from_slice(&total_size.to_be_bytes());
    header
}

pub fn write_stream_data_header(
    response: bool,
    stream_id: u32,
    chunk_index: u32,
    payload_len: usize,
) -> [u8; STREAM_DATA_FRAME_HEADER_LEN] {
    let kind = if response {
        WireKind::StreamResponseData
    } else {
        WireKind::StreamData
    };
    let mut header = init_header(kind, checked_body_len(STREAM_DATA_HEADER_LEN, payload_len));
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&chunk_index.to_be_bytes());
    header
}

pub fn write_stream_abort_header(
    stream_id: u32,
    reason: u32,
) -> [u8; STREAM_DATA_FRAME_HEADER_LEN] {
    let mut header = init_header(WireKind::StreamAbort, STREAM_DATA_HEADER_LEN);
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&reason.to_be_bytes());
    header
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_round_trip_and_rejects_unknown_kind() {
        let bytes = encode_control(WireKind::ActorTell, 123);
        assert_eq!(
            decode_control(bytes),
            Some(Control {
                kind: WireKind::ActorTell,
                body_len: 123
            })
        );
        assert!(decode_control((31u32 << CONTROL_BODY_LEN_BITS).to_be_bytes()).is_none());
    }

    #[test]
    fn every_v5_wire_kind_has_a_pinned_dense_control_encoding() {
        for (expected_value, kind) in WireKind::ALL.into_iter().enumerate() {
            let bytes = encode_control(kind, 17);
            assert_eq!(
                u32::from_be_bytes(bytes) >> CONTROL_BODY_LEN_BITS,
                expected_value as u32
            );
            assert_eq!(decode_control(bytes), Some(Control { kind, body_len: 17 }));
        }
    }

    #[test]
    fn control_codec_preserves_boundary_lengths_for_every_kind() {
        // Was `0..=WireKind::StreamAbort as u8`, silently excluding
        // RouteBind/RoutedActorAsk (13, 14) from coverage. WireKind::ALL is
        // the single source of truth so this can't drift again.
        let lengths = [0, 1, 15, 16, 17, CONTROL_BODY_LEN_MASK as usize];
        for kind in WireKind::ALL {
            for body_len in lengths {
                let encoded = encode_control(kind, body_len);
                assert_eq!(decode_control(encoded), Some(Control { kind, body_len }));
            }
        }
    }

    #[test]
    fn actor_tell_is_sixteen_bytes_for_any_inline_payload_size() {
        for payload_len in [
            0,
            1,
            64,
            64 * 1024,
            10 * 1024 * 1024,
            CONTROL_BODY_LEN_MASK as usize - ACTOR_TELL_HEADER_LEN,
        ] {
            let header = write_actor_tell_header(0x0102_0304_0506_0708, 0x1122_3344, payload_len);
            assert_eq!(header.len(), 16);
            assert_eq!(
                u64::from_be_bytes(header[4..12].try_into().unwrap()),
                0x0102_0304_0506_0708
            );
            assert_eq!(
                u32::from_be_bytes(header[12..16].try_into().unwrap()),
                0x1122_3344
            );
            assert_eq!(
                decode_control(header[..4].try_into().unwrap())
                    .unwrap()
                    .body_len,
                12 + payload_len
            );
        }
    }

    #[test]
    fn all_inline_payload_offsets_are_sixteen_aligned() {
        for offset in [
            ACTOR_TELL_FRAME_HEADER_LEN,
            ACTOR_ASK_FRAME_HEADER_LEN,
            ASK_RESPONSE_FRAME_HEADER_LEN,
            GOSSIP_FRAME_HEADER_LEN,
            DIRECT_ASK_FRAME_HEADER_LEN,
            DIRECT_RESPONSE_FRAME_HEADER_LEN,
            PUBSUB_FRAME_HEADER_LEN,
        ] {
            assert_eq!(offset % 16, 0);
        }
    }

    #[test]
    fn direct_frames_do_not_encode_redundant_payload_length() {
        let header = write_direct_ask_header(0x1234_5678, 0xdead_beef_cafe_1234, 9);
        let control = decode_control(header[..4].try_into().unwrap()).unwrap();
        assert_eq!(control.kind, WireKind::DirectAsk);
        assert_eq!(control.body_len, DIRECT_ASK_HEADER_LEN + 9);
        assert_eq!(
            u32::from_be_bytes(header[4..8].try_into().unwrap()),
            0x1234_5678
        );
        assert_eq!(
            u64::from_be_bytes(header[8..16].try_into().unwrap()),
            0xdead_beef_cafe_1234
        );
    }

    /// The stable request id occupies bytes the frame has always reserved
    /// (and zeroed) after the connection-local correlation id, so it costs
    /// no extra frame -- same trick as the ask-NACK marker in the Response
    /// header.
    #[test]
    fn direct_ask_request_id_round_trips_through_the_headers_reserved_bytes() {
        let header = write_direct_ask_header(1, 42, 0);
        assert_eq!(
            u64::from_be_bytes(header[8..16].try_into().unwrap()),
            42,
            "request_id must occupy the frame's previously-zeroed trailing bytes"
        );
    }

    #[test]
    fn routed_ask_is_sixteen_bytes_and_route_bind_is_exact() {
        let ask = write_routed_actor_ask_header(7, 11, 99);
        assert_eq!(ask.len(), 16);
        assert_eq!(
            decode_control(ask[..4].try_into().unwrap()).unwrap().kind,
            WireKind::RoutedActorAsk
        );
        assert_eq!(u32::from_be_bytes(ask[4..8].try_into().unwrap()), 7);
        assert_eq!(u32::from_be_bytes(ask[8..12].try_into().unwrap()), 11);
        let bind = write_route_bind_header(11, 0x0102_0304_0506_0708, 0x1122_3344);
        assert_eq!(bind.len(), 24);
        assert_eq!(
            decode_control(bind[..4].try_into().unwrap()).unwrap().kind,
            WireKind::RouteBind
        );
        assert_eq!(u32::from_be_bytes(bind[4..8].try_into().unwrap()), 11);
    }

    #[test]
    fn oversize_body_panics() {
        assert!(
            std::panic::catch_unwind(|| {
                write_actor_tell_header(0, 0, CONTROL_BODY_LEN_MASK as usize)
            })
            .is_err()
        );
    }

    #[test]
    fn ask_nack_round_trips_through_the_response_headers_reserved_bytes() {
        // The NACK marker rides in bytes the wire has always reserved (and
        // zeroed) after the correlation id in a Response frame, so it costs
        // no extra frame and old-shaped Response parsing that never looked
        // past the correlation id keeps working.
        let header = write_ask_nack_header(0x0102_0304, AskNackReason::UnknownActor);
        let control = decode_control(header[..4].try_into().unwrap()).unwrap();
        assert_eq!(control.kind, WireKind::Response);
        assert_eq!(control.body_len, ASK_RESPONSE_HEADER_LEN);
        assert_eq!(
            u32::from_be_bytes(header[4..8].try_into().unwrap()),
            0x0102_0304
        );
        assert_eq!(
            ask_nack_reason(&header[4..]),
            Some(AskNackReason::UnknownActor)
        );
    }

    #[test]
    fn a_nack_whose_reason_this_build_does_not_know_is_still_a_nack() {
        // A newer peer can NACK with a reason byte we have never heard of,
        // and a corrupted frame can produce one by accident. Either way the
        // marker is set, so the ask was refused. Decoding that as an
        // ordinary response would hand the caller a successful empty payload
        // for a request nobody answered -- fabricated success, the failure
        // mode this whole NACK path exists to remove.
        for unknown in [0u8, 4, 9, 200, 255] {
            let mut header = write_ask_nack_header(7, AskNackReason::UnknownActor);
            header[LENGTH_PREFIX_LEN + ASK_NACK_REASON_BODY_OFFSET] = unknown;
            assert_eq!(
                ask_nack_reason(&header[4..]),
                Some(AskNackReason::Unsupported),
                "reason byte {unknown} left the marker set, so it must not decode as a response"
            );
        }

        // The marker itself still distinguishes the two cases: an ordinary
        // response is unaffected no matter what the reason byte holds.
        let mut ordinary = write_ask_response_header(MessageType::Response, 7, 0);
        ordinary[LENGTH_PREFIX_LEN + ASK_NACK_REASON_BODY_OFFSET] = 200;
        assert_eq!(ask_nack_reason(&ordinary[4..]), None);
    }

    #[test]
    fn a_normal_response_header_never_parses_as_a_nack() {
        let header = write_ask_response_header(MessageType::Response, 7, 5);
        assert_eq!(ask_nack_reason(&header[4..]), None);
    }

    #[test]
    fn stream_abort_is_a_fixed_twelve_byte_frame() {
        let header = write_stream_abort_header(7, 9);
        assert_eq!(header.len(), STREAM_DATA_FRAME_HEADER_LEN);
        assert_eq!(
            decode_control(header[..4].try_into().unwrap()),
            Some(Control {
                kind: WireKind::StreamAbort,
                body_len: STREAM_DATA_HEADER_LEN,
            })
        );
        assert_eq!(u32::from_be_bytes(header[4..8].try_into().unwrap()), 7);
        assert_eq!(u32::from_be_bytes(header[8..12].try_into().unwrap()), 9);
    }
}
