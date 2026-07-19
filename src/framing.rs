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
pub const DIRECT_ASK_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + DIRECT_ASK_HEADER_LEN;
pub const DIRECT_RESPONSE_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + DIRECT_RESPONSE_HEADER_LEN;
pub const PUBSUB_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + PUBSUB_HEADER_LEN;

const _: () = assert!(ACTOR_TELL_FRAME_HEADER_LEN % 16 == 0);
const _: () = assert!(ACTOR_ASK_FRAME_HEADER_LEN % 16 == 0);
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
}

impl WireKind {
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

pub fn write_gossip_frame_prefix(payload_len: usize) -> [u8; GOSSIP_FRAME_HEADER_LEN] {
    init_header(WireKind::Gossip, checked_body_len(GOSSIP_HEADER_LEN, payload_len))
}

pub fn write_pubsub_frame_prefix(payload_len: usize) -> [u8; PUBSUB_FRAME_HEADER_LEN] {
    init_header(WireKind::PubSub, checked_body_len(PUBSUB_HEADER_LEN, payload_len))
}

pub fn write_direct_ask_header(
    correlation_id: u32,
    payload_len: usize,
) -> [u8; DIRECT_ASK_FRAME_HEADER_LEN] {
    let body_len = checked_body_len(DIRECT_ASK_HEADER_LEN, payload_len);
    let mut header = init_header(WireKind::DirectAsk, body_len);
    header[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    header
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
    let mut header = init_header(
        kind,
        checked_body_len(STREAM_DATA_HEADER_LEN, payload_len),
    );
    header[4..8].copy_from_slice(&stream_id.to_be_bytes());
    header[8..12].copy_from_slice(&chunk_index.to_be_bytes());
    header
}

pub fn write_stream_abort_header(stream_id: u32, reason: u32) -> [u8; STREAM_DATA_FRAME_HEADER_LEN] {
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
        let kinds = [
            WireKind::Gossip,
            WireKind::Ask,
            WireKind::Response,
            WireKind::ActorTell,
            WireKind::ActorAsk,
            WireKind::StreamStart,
            WireKind::StreamData,
            WireKind::StreamResponseStart,
            WireKind::StreamResponseData,
            WireKind::DirectAsk,
            WireKind::DirectResponse,
            WireKind::PubSub,
            WireKind::StreamAbort,
        ];
        for (expected_value, kind) in kinds.into_iter().enumerate() {
            let bytes = encode_control(kind, 17);
            assert_eq!(u32::from_be_bytes(bytes) >> CONTROL_BODY_LEN_BITS, expected_value as u32);
            assert_eq!(decode_control(bytes), Some(Control { kind, body_len: 17 }));
        }
    }

    #[test]
    fn control_codec_preserves_boundary_lengths_for_every_kind() {
        let lengths = [0, 1, 15, 16, 17, CONTROL_BODY_LEN_MASK as usize];
        for raw_kind in 0..=WireKind::StreamAbort as u8 {
            let kind = WireKind::from_u8(raw_kind).expect("dense V5 kind");
            for body_len in lengths {
                let encoded = encode_control(kind, body_len);
                assert_eq!(decode_control(encoded), Some(Control { kind, body_len }));
            }
        }
    }

    #[test]
    fn actor_tell_is_sixteen_bytes_for_any_inline_payload_size() {
        for payload_len in [0, 1, 64, 64 * 1024, 10 * 1024 * 1024] {
            let header =
                write_actor_tell_header(0x0102_0304_0506_0708, 0x1122_3344, payload_len);
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
        let header = write_direct_ask_header(0x1234_5678, 9);
        let control = decode_control(header[..4].try_into().unwrap()).unwrap();
        assert_eq!(control.kind, WireKind::DirectAsk);
        assert_eq!(control.body_len, DIRECT_ASK_HEADER_LEN + 9);
        assert_eq!(
            u32::from_be_bytes(header[4..8].try_into().unwrap()),
            0x1234_5678
        );
        assert_eq!(&header[8..], &[0; 8]);
    }

    #[test]
    fn oversize_body_panics() {
        assert!(std::panic::catch_unwind(|| {
            write_actor_tell_header(0, 0, CONTROL_BODY_LEN_MASK as usize)
        })
        .is_err());
    }

    #[test]
    fn stream_abort_is_a_fixed_twelve_byte_frame() {
        let header = write_stream_abort_header(7, 9);
        assert_eq!(header.len(), STREAM_DATA_FRAME_HEADER_LEN);
        assert_eq!(decode_control(header[..4].try_into().unwrap()), Some(Control {
            kind: WireKind::StreamAbort,
            body_len: STREAM_DATA_HEADER_LEN,
        }));
        assert_eq!(u32::from_be_bytes(header[4..8].try_into().unwrap()), 7);
        assert_eq!(u32::from_be_bytes(header[8..12].try_into().unwrap()), 9);
    }
}
