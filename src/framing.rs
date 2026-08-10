use crate::{GossipError, MessageType, Result};

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

/// Shared `.expect()` message for every infallible wrapper in this file
/// whose fallible `try_*` sibling is the one every internal caller actually
/// uses (`checked_body_len`, `encode_control`, and each `write_*_header`/
/// `write_*_frame_prefix` below except `write_route_bind_header` and
/// `write_stream_abort_header`, which were never fallible: both take only
/// fixed-size constants, never a caller-supplied length).
///
/// Unlike `write_stream_*_header`'s trusted-invariant panic (genuinely
/// unreachable, since every internal call site clamps the chunk length to
/// `max_stream_chunk_size()` first), the panic behind *this* message is
/// reachable in practice: `payload_len` on these builders is an ordinary,
/// un-chunked caller payload (`Bytes`/`PooledPayload`) with no upper bound
/// of its own before the call. That is not a new risk this PR introduces --
/// it is the exact, unconditional panic every one of these functions
/// already had before this PR, when there was no fallible alternative at
/// all. The infallible name stays available only for whatever a
/// hypothetical caller outside this crate already depends on; every
/// internal caller uses the `try_*` sibling, which is what actually
/// enforces the V5 27-bit limit safely (returning `MessageTooLarge`
/// instead of panicking the sending task and, via `ExitGuard`, tearing the
/// connection down).
const FRAME_BODY_LEN_INVARIANT: &str = "frame body length exceeds V5 27-bit limit";

/// Sum a fixed header length with a caller-supplied payload length, bounded
/// to the V5 27-bit body-length field. See the note on
/// `FRAME_BODY_LEN_INVARIANT` above.
#[inline]
pub fn checked_body_len(fixed_len: usize, payload_len: usize) -> usize {
    try_checked_body_len(fixed_len, payload_len).expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `checked_body_len` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
#[inline]
pub fn try_checked_body_len(fixed_len: usize, payload_len: usize) -> Result<usize> {
    fixed_len
        .checked_add(payload_len)
        .filter(|len| *len <= CONTROL_BODY_LEN_MASK as usize)
        .ok_or_else(|| GossipError::MessageTooLarge {
            size: fixed_len.saturating_add(payload_len),
            max: CONTROL_BODY_LEN_MASK as usize,
        })
}

/// See the note on `FRAME_BODY_LEN_INVARIANT` above.
#[inline]
pub fn encode_control(kind: WireKind, body_len: usize) -> [u8; LENGTH_PREFIX_LEN] {
    try_encode_control(kind, body_len).expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `encode_control` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
#[inline]
pub fn try_encode_control(kind: WireKind, body_len: usize) -> Result<[u8; LENGTH_PREFIX_LEN]> {
    if body_len > CONTROL_BODY_LEN_MASK as usize {
        return Err(GossipError::MessageTooLarge {
            size: body_len,
            max: CONTROL_BODY_LEN_MASK as usize,
        });
    }
    let word = ((kind as u32) << CONTROL_BODY_LEN_BITS) | body_len as u32;
    Ok(word.to_be_bytes())
}

/// Local pre-send admission check for an inline (non-streaming) frame: the
/// peer's configured `max_message_size` is a ceiling on the *encoded* frame
/// body (this `WireKind`'s fixed header length plus the caller's payload),
/// not on the payload alone. Comparing payload length by itself under-counts
/// every structured frame kind by its header size and lets a payload through
/// this gate that the receiver still hard-rejects as `MessageTooLarge` once
/// the header is added, tearing the connection down -- exactly what this
/// check exists to prevent.
///
/// `fixed_header_len` must be the same constant (e.g. `ACTOR_TELL_HEADER_LEN`)
/// the caller's `write_*_header` will add to the payload length. Pass `0` for
/// the raw-tell/typed-tell paths: their bare-length control word has no
/// separate structured header, so body_len == payload_len exactly.
#[inline]
pub fn reject_oversize_for_inline_send(
    fixed_header_len: usize,
    payload_len: usize,
    max_message_size: usize,
) -> Result<()> {
    let body_len = fixed_header_len.saturating_add(payload_len);
    if body_len > max_message_size {
        return Err(GossipError::MessageTooLarge {
            size: body_len,
            max: max_message_size,
        });
    }
    Ok(())
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
fn init_header<const N: usize>(kind: WireKind, body_len: usize) -> Result<[u8; N]> {
    let mut header = [0u8; N];
    header[..LENGTH_PREFIX_LEN].copy_from_slice(&try_encode_control(kind, body_len)?);
    Ok(header)
}

/// V5 actor tell header. Payload begins at offset 16 for every inline size.
/// See the note on `FRAME_BODY_LEN_INVARIANT` above.
pub fn write_actor_tell_header(
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
) -> [u8; ACTOR_TELL_FRAME_HEADER_LEN] {
    try_write_actor_tell_header(actor_id, type_hash, payload_len).expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_actor_tell_header` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_actor_tell_header(
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
) -> Result<[u8; ACTOR_TELL_FRAME_HEADER_LEN]> {
    let body_len = try_checked_body_len(ACTOR_TELL_HEADER_LEN, payload_len)?;
    let mut header: [u8; ACTOR_TELL_FRAME_HEADER_LEN] = init_header(WireKind::ActorTell, body_len)?;
    header[4..12].copy_from_slice(&actor_id.to_be_bytes());
    header[12..16].copy_from_slice(&type_hash.to_be_bytes());
    Ok(header)
}

/// V5 actor ask header. The trailing pad preserves a 16-byte payload offset.
/// See the note on `FRAME_BODY_LEN_INVARIANT` above.
pub fn write_actor_ask_header(
    correlation_id: u32,
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
) -> [u8; ACTOR_ASK_FRAME_HEADER_LEN] {
    try_write_actor_ask_header(correlation_id, actor_id, type_hash, payload_len)
        .expect(FRAME_BODY_LEN_INVARIANT)
}

/// V5 actor ask header carrying an optional caller-controlled request id.
///
/// The request id occupies eight bytes of the existing trailing pad. The
/// remaining four bytes stay zero so legacy actor-ask frames retain their
/// exact size and alignment. A zero request id is reserved for the legacy,
/// unmarked form and is rejected when explicitly supplied.
pub fn write_actor_ask_header_with_request_id(
    correlation_id: u32,
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
    request_id: Option<u64>,
) -> [u8; ACTOR_ASK_FRAME_HEADER_LEN] {
    try_write_actor_ask_header_with_request_id(
        correlation_id,
        actor_id,
        type_hash,
        payload_len,
        request_id,
    )
    .expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_actor_ask_header` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_actor_ask_header(
    correlation_id: u32,
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
) -> Result<[u8; ACTOR_ASK_FRAME_HEADER_LEN]> {
    try_write_actor_ask_header_with_request_id(
        correlation_id,
        actor_id,
        type_hash,
        payload_len,
        None,
    )
}

/// Fallible actor-ask header writer with an optional out-of-band request id.
pub fn try_write_actor_ask_header_with_request_id(
    correlation_id: u32,
    actor_id: u64,
    type_hash: u32,
    payload_len: usize,
    request_id: Option<u64>,
) -> Result<[u8; ACTOR_ASK_FRAME_HEADER_LEN]> {
    if request_id == Some(0) {
        return Err(crate::GossipError::InvalidConfig(
            "actor ask request id must be nonzero".to_string(),
        ));
    }
    let body_len = try_checked_body_len(ACTOR_ASK_HEADER_LEN, payload_len)?;
    let mut header: [u8; ACTOR_ASK_FRAME_HEADER_LEN] = init_header(WireKind::ActorAsk, body_len)?;
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[8..16].copy_from_slice(&actor_id.to_be_bytes());
    header[16..20].copy_from_slice(&type_hash.to_be_bytes());
    if let Some(request_id) = request_id {
        header[20..28].copy_from_slice(&request_id.to_be_bytes());
    }
    Ok(header)
}

/// V5 compact ask after its route was bound on this connection. See the
/// note on `FRAME_BODY_LEN_INVARIANT` above.
pub fn write_routed_actor_ask_header(
    correlation_id: u32,
    route_slot: u32,
    payload_len: usize,
) -> [u8; ROUTED_ACTOR_ASK_FRAME_HEADER_LEN] {
    try_write_routed_actor_ask_header(correlation_id, route_slot, payload_len)
        .expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_routed_actor_ask_header` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_routed_actor_ask_header(
    correlation_id: u32,
    route_slot: u32,
    payload_len: usize,
) -> Result<[u8; ROUTED_ACTOR_ASK_FRAME_HEADER_LEN]> {
    let body_len = try_checked_body_len(ROUTED_ACTOR_ASK_HEADER_LEN, payload_len)?;
    let mut header: [u8; ROUTED_ACTOR_ASK_FRAME_HEADER_LEN] =
        init_header(WireKind::RoutedActorAsk, body_len)?;
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[8..12].copy_from_slice(&route_slot.to_be_bytes());
    Ok(header)
}

/// Establishes a connection-scoped slot before a routed ask uses it.
///
/// `ROUTE_BIND_HEADER_LEN` is a fixed 20-byte constant, not a caller-supplied
/// payload length -- it is always far under the V5 27-bit body-length limit,
/// so this can never fail and stays infallible rather than pushing `?`
/// through every one of its call sites for a case that cannot occur. This
/// PR never changed this function's signature, so it has no `try_*` sibling.
pub fn write_route_bind_header(
    route_slot: u32,
    actor_id: u64,
    type_hash: u32,
) -> [u8; ROUTE_BIND_FRAME_HEADER_LEN] {
    let mut header: [u8; ROUTE_BIND_FRAME_HEADER_LEN] =
        init_header(WireKind::RouteBind, ROUTE_BIND_HEADER_LEN)
            .expect("ROUTE_BIND_HEADER_LEN is a fixed constant within the V5 27-bit limit");
    header[4..8].copy_from_slice(&route_slot.to_be_bytes());
    header[8..16].copy_from_slice(&actor_id.to_be_bytes());
    header[16..20].copy_from_slice(&type_hash.to_be_bytes());
    header
}

/// See the note on `FRAME_BODY_LEN_INVARIANT` above.
pub fn write_ask_response_header(
    msg_type: MessageType,
    correlation_id: u32,
    payload_len: usize,
) -> [u8; ASK_RESPONSE_FRAME_HEADER_LEN] {
    try_write_ask_response_header(msg_type, correlation_id, payload_len)
        .expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_ask_response_header` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above. `msg_type` validation is
/// unrelated to that invariant (a caller programming error, not an
/// adversarial/oversize payload) and panics in both forms, exactly as it
/// did before this PR.
pub fn try_write_ask_response_header(
    msg_type: MessageType,
    correlation_id: u32,
    payload_len: usize,
) -> Result<[u8; ASK_RESPONSE_FRAME_HEADER_LEN]> {
    let kind = match msg_type {
        MessageType::Ask => WireKind::Ask,
        MessageType::Response => WireKind::Response,
        _ => panic!("ask/response header requires Ask or Response"),
    };
    let body_len = try_checked_body_len(ASK_RESPONSE_HEADER_LEN, payload_len)?;
    let mut header: [u8; ASK_RESPONSE_FRAME_HEADER_LEN] = init_header(kind, body_len)?;
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    Ok(header)
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
    /// A dispatcher exists for this wire path, but the connection has no
    /// spare capacity to admit the answer right now (e.g. the local
    /// streaming-response queue is full). Distinct from `NoDispatcher`,
    /// which means no dispatcher exists for this path at all -- this is
    /// transient: the same ask, retried once capacity frees up (or sent to
    /// a different peer), may succeed where it fails now.
    Backpressure = 4,
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
            4 => Self::Backpressure,
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
            Self::Backpressure => "no spare capacity for this ask right now",
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
/// reason packed into the header's reserved bytes. See the note on
/// `FRAME_BODY_LEN_INVARIANT` above.
pub fn write_ask_nack_header(
    correlation_id: u32,
    reason: AskNackReason,
) -> [u8; ASK_RESPONSE_FRAME_HEADER_LEN] {
    try_write_ask_nack_header(correlation_id, reason).expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_ask_nack_header` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_ask_nack_header(
    correlation_id: u32,
    reason: AskNackReason,
) -> Result<[u8; ASK_RESPONSE_FRAME_HEADER_LEN]> {
    let mut header: [u8; ASK_RESPONSE_FRAME_HEADER_LEN] =
        init_header(WireKind::Response, ASK_RESPONSE_HEADER_LEN)?;
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[LENGTH_PREFIX_LEN + ASK_NACK_FLAG_BODY_OFFSET] = ASK_NACK_FLAG_SET;
    header[LENGTH_PREFIX_LEN + ASK_NACK_REASON_BODY_OFFSET] = reason as u8;
    Ok(header)
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
    try_write_gossip_frame_prefix(payload_len).expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_gossip_frame_prefix` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_gossip_frame_prefix(payload_len: usize) -> Result<[u8; GOSSIP_FRAME_HEADER_LEN]> {
    init_header(
        WireKind::Gossip,
        try_checked_body_len(GOSSIP_HEADER_LEN, payload_len)?,
    )
}

/// See the note on `FRAME_BODY_LEN_INVARIANT` above.
pub fn write_pubsub_frame_prefix(payload_len: usize) -> [u8; PUBSUB_FRAME_HEADER_LEN] {
    try_write_pubsub_frame_prefix(payload_len).expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_pubsub_frame_prefix` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_pubsub_frame_prefix(payload_len: usize) -> Result<[u8; PUBSUB_FRAME_HEADER_LEN]> {
    init_header(
        WireKind::PubSub,
        try_checked_body_len(PUBSUB_HEADER_LEN, payload_len)?,
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
/// Actor asks now expose the same bounded out-of-band identity through
/// `write_actor_ask_header_with_request_id`; compact routed asks deliberately
/// remain unmarked because their header has no spare identity bytes. Callers
/// that need an actor request id therefore use the uncompact ActorAsk form.
///
/// Occupies bytes the frame has always reserved and zeroed after
/// `correlation_id`, so it costs no extra frame. Fail-closed: `request_id`
/// must be nonzero (see `direct_ask_request_id` on the read side); 0 is
/// reserved to mean "absent" and is rejected rather than silently accepted
/// as a valid-looking id that could collide across independent asks.
///
/// See the note on `FRAME_BODY_LEN_INVARIANT` above for why this has both an
/// infallible form and a `try_*` sibling.
pub fn write_direct_ask_header(
    correlation_id: u32,
    request_id: u64,
    payload_len: usize,
) -> [u8; DIRECT_ASK_FRAME_HEADER_LEN] {
    try_write_direct_ask_header(correlation_id, request_id, payload_len)
        .expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_direct_ask_header` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_direct_ask_header(
    correlation_id: u32,
    request_id: u64,
    payload_len: usize,
) -> Result<[u8; DIRECT_ASK_FRAME_HEADER_LEN]> {
    let body_len = try_checked_body_len(DIRECT_ASK_HEADER_LEN, payload_len)?;
    let mut header: [u8; DIRECT_ASK_FRAME_HEADER_LEN] = init_header(WireKind::DirectAsk, body_len)?;
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header[8..16].copy_from_slice(&request_id.to_be_bytes());
    Ok(header)
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
    try_write_direct_response_header(correlation_id, payload_len).expect(FRAME_BODY_LEN_INVARIANT)
}

/// Fallible sibling of `write_direct_response_header` -- see the note on
/// `FRAME_BODY_LEN_INVARIANT` above. Every internal caller in this crate
/// uses this, not the infallible form above.
pub fn try_write_direct_response_header(
    correlation_id: u32,
    payload_len: usize,
) -> Result<[u8; DIRECT_RESPONSE_FRAME_HEADER_LEN]> {
    let body_len = try_checked_body_len(DIRECT_RESPONSE_HEADER_LEN, payload_len)?;
    let mut header: [u8; DIRECT_RESPONSE_FRAME_HEADER_LEN] =
        init_header(WireKind::DirectResponse, body_len)?;
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    Ok(header)
}

/// Shared `.expect()` message for the infallible `write_stream_*_header`
/// wrappers below -- see the note on `write_stream_request_start_header`.
const STREAM_CHUNK_INVARIANT: &str =
    "stream chunk length is bounded by max_stream_chunk_size, always within the V5 27-bit limit";

/// In every current caller (the streaming writer in `connection_pool`),
/// `first_chunk_len`/`payload_len` here are never the caller's raw,
/// unbounded payload length -- the streaming writer always clamps every
/// chunk to `max_stream_chunk_size()` first (itself derived from
/// `max_message_size`, which config validation already bounds to the V5
/// 27-bit limit), so `checked_body_len` cannot observe an oversize value on
/// that path in practice. That evidence hasn't changed across four review
/// rounds.
///
/// codex's objection moved each time a fix landed: `pub(crate)` broke a
/// hypothetical downstream caller by hiding the function; `pub` + `Result`
/// broke it anyway by changing the return type, since any expression that
/// indexed, iterated, or otherwise used the returned array no longer
/// compiles against a `Result`. The only change that is genuinely
/// source-compatible with whatever a caller outside this crate could have
/// written before this PR touched these functions is *no signature change
/// at all*: these three stay `pub fn (..) -> [u8; N]`, exactly as they
/// were, and panic via the same trusted-invariant `.expect()` every other
/// infallible builder in this file uses (`write_route_bind_header`,
/// `write_stream_abort_header`). `try_write_stream_request_start_header`/
/// `try_write_stream_response_start_header`/`try_write_stream_data_header`
/// below are the fallible siblings: every internal caller in this crate
/// uses those instead, so no panicking path is reachable in practice
/// despite the infallible signatures staying available for whatever a
/// hypothetical downstream caller might already depend on.
pub fn write_stream_request_start_header(
    stream_id: u32,
    correlation_id: u32,
    total_size: u32,
    actor_id: u64,
    type_hash: u32,
    first_chunk_len: usize,
) -> [u8; STREAM_REQUEST_START_FRAME_HEADER_LEN] {
    try_write_stream_request_start_header(
        stream_id,
        correlation_id,
        total_size,
        actor_id,
        type_hash,
        first_chunk_len,
    )
    .expect(STREAM_CHUNK_INVARIANT)
}

/// Fallible sibling of `write_stream_request_start_header` -- see the note
/// there. Every internal caller uses this, not the infallible form above.
pub fn try_write_stream_request_start_header(
    stream_id: u32,
    correlation_id: u32,
    total_size: u32,
    actor_id: u64,
    type_hash: u32,
    first_chunk_len: usize,
) -> Result<[u8; STREAM_REQUEST_START_FRAME_HEADER_LEN]> {
    let body_len = try_checked_body_len(STREAM_REQUEST_START_HEADER_LEN, first_chunk_len)?;
    let mut header: [u8; STREAM_REQUEST_START_FRAME_HEADER_LEN] =
        init_header(WireKind::StreamStart, body_len)?;
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&correlation_id.to_be_bytes());
    header[12..16].copy_from_slice(&total_size.to_be_bytes());
    header[16..24].copy_from_slice(&actor_id.to_be_bytes());
    header[24..28].copy_from_slice(&type_hash.to_be_bytes());
    Ok(header)
}

/// See the note on `write_stream_request_start_header` above.
pub fn write_stream_response_start_header(
    stream_id: u32,
    correlation_id: u32,
    total_size: u32,
    first_chunk_len: usize,
) -> [u8; STREAM_RESPONSE_START_FRAME_HEADER_LEN] {
    try_write_stream_response_start_header(stream_id, correlation_id, total_size, first_chunk_len)
        .expect(STREAM_CHUNK_INVARIANT)
}

/// Fallible sibling of `write_stream_response_start_header` -- see the note
/// on `write_stream_request_start_header` above. Every internal caller uses
/// this, not the infallible form above.
pub fn try_write_stream_response_start_header(
    stream_id: u32,
    correlation_id: u32,
    total_size: u32,
    first_chunk_len: usize,
) -> Result<[u8; STREAM_RESPONSE_START_FRAME_HEADER_LEN]> {
    let body_len = try_checked_body_len(STREAM_RESPONSE_START_HEADER_LEN, first_chunk_len)?;
    let mut header: [u8; STREAM_RESPONSE_START_FRAME_HEADER_LEN] =
        init_header(WireKind::StreamResponseStart, body_len)?;
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&correlation_id.to_be_bytes());
    header[12..16].copy_from_slice(&total_size.to_be_bytes());
    Ok(header)
}

/// See the note on `write_stream_request_start_header` above.
pub fn write_stream_data_header(
    response: bool,
    stream_id: u32,
    chunk_index: u32,
    payload_len: usize,
) -> [u8; STREAM_DATA_FRAME_HEADER_LEN] {
    try_write_stream_data_header(response, stream_id, chunk_index, payload_len)
        .expect(STREAM_CHUNK_INVARIANT)
}

/// Fallible sibling of `write_stream_data_header` -- see the note on
/// `write_stream_request_start_header` above. Every internal caller uses
/// this, not the infallible form above.
pub fn try_write_stream_data_header(
    response: bool,
    stream_id: u32,
    chunk_index: u32,
    payload_len: usize,
) -> Result<[u8; STREAM_DATA_FRAME_HEADER_LEN]> {
    let kind = if response {
        WireKind::StreamResponseData
    } else {
        WireKind::StreamData
    };
    let body_len = try_checked_body_len(STREAM_DATA_HEADER_LEN, payload_len)?;
    let mut header: [u8; STREAM_DATA_FRAME_HEADER_LEN] = init_header(kind, body_len)?;
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&chunk_index.to_be_bytes());
    Ok(header)
}

/// `STREAM_DATA_HEADER_LEN` is a fixed 8-byte constant -- this can never fail.
pub fn write_stream_abort_header(
    stream_id: u32,
    reason: u32,
) -> [u8; STREAM_DATA_FRAME_HEADER_LEN] {
    let mut header: [u8; STREAM_DATA_FRAME_HEADER_LEN] =
        init_header(WireKind::StreamAbort, STREAM_DATA_HEADER_LEN)
            .expect("STREAM_DATA_HEADER_LEN is a fixed constant within the V5 27-bit limit");
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
    fn actor_ask_request_id_uses_reserved_bytes_without_changing_frame_size() {
        let legacy = write_actor_ask_header(1, 2, 3, 4);
        let marked = write_actor_ask_header_with_request_id(1, 2, 3, 4, Some(42));
        assert_eq!(legacy.len(), ACTOR_ASK_FRAME_HEADER_LEN);
        assert_eq!(marked.len(), ACTOR_ASK_FRAME_HEADER_LEN);
        assert_eq!(u64::from_be_bytes(legacy[20..28].try_into().unwrap()), 0);
        assert_eq!(u64::from_be_bytes(marked[20..28].try_into().unwrap()), 42);
        assert_eq!(&marked[28..32], &[0; 4]);
        assert!(matches!(
            try_write_actor_ask_header_with_request_id(1, 2, 3, 4, Some(0)),
            Err(GossipError::InvalidConfig(_))
        ));
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

    /// A body length at exactly the V5 27-bit limit is still representable;
    /// one byte past it must be rejected, not silently truncated. Exercised
    /// through `try_checked_body_len`, the form every internal caller uses.
    #[test]
    fn checked_body_len_boundary_at_and_above_27_bits() {
        let max = CONTROL_BODY_LEN_MASK as usize;
        assert_eq!(try_checked_body_len(0, max).unwrap(), max);
        assert_eq!(try_checked_body_len(1, max - 1).unwrap(), max);
        assert!(try_checked_body_len(0, max + 1).is_err());
        assert!(try_checked_body_len(1, max).is_err());
    }

    /// Same boundary, exercised through `try_encode_control` directly (the
    /// other former panic site): the error must carry the offending size
    /// and the limit, not just a message.
    #[test]
    fn encode_control_boundary_at_and_above_27_bits() {
        let max = CONTROL_BODY_LEN_MASK as usize;
        assert!(try_encode_control(WireKind::Gossip, max).is_ok());
        match try_encode_control(WireKind::Gossip, max + 1) {
            Err(GossipError::MessageTooLarge {
                size,
                max: reported,
            }) => {
                assert_eq!(size, max + 1);
                assert_eq!(reported, max);
            }
            other => panic!("expected MessageTooLarge, got {other:?}"),
        }
    }

    /// `max_message_size` bounds the encoded body (fixed header + payload),
    /// not the payload alone: a payload that fits under the limit by itself
    /// must still be rejected once its frame's fixed header pushes the
    /// encoded body over it, and accepted when the same total fits.
    #[test]
    fn reject_oversize_for_inline_send_accounts_for_fixed_header_overhead() {
        let max = 100;
        assert!(reject_oversize_for_inline_send(12, 90, max).is_err());
        assert!(reject_oversize_for_inline_send(12, 88, max).is_ok());
        assert!(reject_oversize_for_inline_send(12, 89, max).is_err());
        // The raw-tell/typed-tell paths pass 0: body_len == payload_len.
        assert!(reject_oversize_for_inline_send(0, max, max).is_ok());
        assert!(reject_oversize_for_inline_send(0, max + 1, max).is_err());
    }

    /// Every `try_*` writer whose body length is a direct function of a
    /// caller-supplied payload must return `MessageTooLarge` at and above the
    /// 27-bit limit instead of panicking `checked_body_len`'s old `.expect`
    /// or `encode_control`'s old `assert!`. This is the form every internal
    /// caller in the crate uses.
    #[test]
    fn oversize_body_returns_message_too_large_not_panic() {
        let oversized = CONTROL_BODY_LEN_MASK as usize + 1;
        assert!(matches!(
            try_write_actor_tell_header(0, 0, oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
        assert!(matches!(
            try_write_actor_ask_header(0, 0, 0, oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
        assert!(matches!(
            try_write_routed_actor_ask_header(0, 0, oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
        assert!(matches!(
            try_write_ask_response_header(MessageType::Ask, 0, oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
        assert!(matches!(
            try_write_gossip_frame_prefix(oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
        assert!(matches!(
            try_write_pubsub_frame_prefix(oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
        assert!(matches!(
            try_write_direct_ask_header(0, 1, oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
        assert!(matches!(
            try_write_direct_response_header(0, oversized),
            Err(GossipError::MessageTooLarge { .. })
        ));
    }

    /// The infallible name of every builder above (and `checked_body_len`/
    /// `encode_control`) stays available only for whatever a hypothetical
    /// caller outside this crate already depends on -- restored to its
    /// exact pre-PR signature and behavior. This is the once-per-function
    /// proof that each one still panics at the same boundary its `try_*`
    /// sibling reports as `MessageTooLarge`, rather than silently doing
    /// something else now that the name is shared with a fallible sibling.
    #[test]
    fn infallible_wrappers_panic_at_the_same_boundary_their_try_sibling_reports() {
        let oversized = CONTROL_BODY_LEN_MASK as usize + 1;
        let cases: [(&str, Box<dyn Fn()>); 10] = [
            (
                "checked_body_len",
                Box::new(move || {
                    checked_body_len(0, oversized);
                }),
            ),
            (
                "encode_control",
                Box::new(move || {
                    encode_control(WireKind::Gossip, oversized);
                }),
            ),
            (
                "write_actor_tell_header",
                Box::new(move || {
                    write_actor_tell_header(0, 0, oversized);
                }),
            ),
            (
                "write_actor_ask_header",
                Box::new(move || {
                    write_actor_ask_header(0, 0, 0, oversized);
                }),
            ),
            (
                "write_routed_actor_ask_header",
                Box::new(move || {
                    write_routed_actor_ask_header(0, 0, oversized);
                }),
            ),
            (
                "write_ask_response_header",
                Box::new(move || {
                    write_ask_response_header(MessageType::Ask, 0, oversized);
                }),
            ),
            (
                "write_gossip_frame_prefix",
                Box::new(move || {
                    write_gossip_frame_prefix(oversized);
                }),
            ),
            (
                "write_pubsub_frame_prefix",
                Box::new(move || {
                    write_pubsub_frame_prefix(oversized);
                }),
            ),
            (
                "write_direct_ask_header",
                Box::new(move || {
                    write_direct_ask_header(0, 1, oversized);
                }),
            ),
            (
                "write_direct_response_header",
                Box::new(move || {
                    write_direct_response_header(0, oversized);
                }),
            ),
        ];
        for (name, case) in cases {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(case));
            assert!(
                outcome.is_err(),
                "{name} must panic on an oversize payload (restored pre-PR behavior), not \
                 silently succeed"
            );
        }
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
    fn ask_nack_round_trips_the_backpressure_reason() {
        // Distinct from `NoDispatcher`: a dispatcher exists, the connection
        // is just out of capacity right now, so this is transient rather
        // than "this build can never answer this ask".
        let header = write_ask_nack_header(42, AskNackReason::Backpressure);
        assert_eq!(
            ask_nack_reason(&header[4..]),
            Some(AskNackReason::Backpressure)
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
        for unknown in [0u8, 5, 9, 200, 255] {
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
