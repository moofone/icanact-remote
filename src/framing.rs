use crate::MessageType;

pub const LENGTH_PREFIX_LEN: usize = 4;
pub const ASK_RESPONSE_HEADER_LEN: usize = 12; // type(1) + correlation_id(2) + pad(9)
pub const GOSSIP_HEADER_LEN: usize = 12; // type(1) + pad(11)
pub const ACTOR_HEADER_LEN: usize = 28; // type(1) + correlation_id(2) + reserved(9) + actor_id(8) + type_hash(4) + payload_len(4)
pub const STREAM_HEADER_PREFIX_LEN: usize = 12; // type(1) + correlation_id(2) + reserved(9)

pub const SCHEMA_HASH_LEN: usize = 8;

// DirectAsk fast path constants
pub const DIRECT_ASK_HEADER_LEN: usize = 12; // type(1) + correlation_id(2) + payload_len(4) + pad(5)
pub const DIRECT_RESPONSE_HEADER_LEN: usize = 12; // type(1) + correlation_id(2) + payload_len(4) + pad(5)

pub const ASK_RESPONSE_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN;
pub const GOSSIP_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN;
pub const ACTOR_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN;
pub const DIRECT_ASK_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + DIRECT_ASK_HEADER_LEN;
pub const DIRECT_RESPONSE_FRAME_HEADER_LEN: usize = LENGTH_PREFIX_LEN + DIRECT_RESPONSE_HEADER_LEN;

/// Write ActorTell/ActorAsk header with padded 16-byte alignment.
///
/// Wire format: [length:4][type:1][correlation_id:2][schema_hash:8][reserved:1][actor_id:8][type_hash:4][payload_len:4]
pub fn write_actor_frame_header(
    msg_type: MessageType,
    correlation_id: u16,
    actor_id: u64,
    type_hash: u32,
    schema_hash: Option<u64>,
    payload_len: usize,
) -> [u8; ACTOR_FRAME_HEADER_LEN] {
    debug_assert!(matches!(
        msg_type,
        MessageType::ActorTell | MessageType::ActorAsk
    ));

    let total_size = ACTOR_HEADER_LEN + payload_len;
    let mut header = [0u8; ACTOR_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = msg_type as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    write_schema_hash(&mut header[7..16], schema_hash);
    header[16..24].copy_from_slice(&actor_id.to_be_bytes());
    header[24..28].copy_from_slice(&type_hash.to_be_bytes());
    header[28..32].copy_from_slice(&(payload_len as u32).to_be_bytes());
    header
}

/// Write schema hash into a reserved header slice (first 8 bytes).
pub fn write_schema_hash(reserved: &mut [u8], schema_hash: Option<u64>) {
    debug_assert!(
        reserved.len() >= SCHEMA_HASH_LEN,
        "reserved header slice too short for schema hash"
    );
    reserved.fill(0);
    if let Some(hash) = schema_hash {
        reserved[..SCHEMA_HASH_LEN].copy_from_slice(&hash.to_be_bytes());
    }
}

/// Read schema hash from a reserved header slice (first 8 bytes).
pub fn read_schema_hash(reserved: &[u8]) -> Option<u64> {
    debug_assert!(
        reserved.len() >= SCHEMA_HASH_LEN,
        "reserved header slice too short for schema hash"
    );
    if reserved[..SCHEMA_HASH_LEN].iter().all(|b| *b == 0) {
        None
    } else {
        Some(u64::from_be_bytes(
            reserved[..SCHEMA_HASH_LEN]
                .try_into()
                .expect("schema hash slice must be 8 bytes"),
        ))
    }
}

pub fn write_ask_response_header(
    msg_type: MessageType,
    correlation_id: u16,
    payload_len: usize,
) -> [u8; ASK_RESPONSE_FRAME_HEADER_LEN] {
    debug_assert!(matches!(msg_type, MessageType::Ask | MessageType::Response));

    let total_size = ASK_RESPONSE_HEADER_LEN + payload_len;
    let mut header = [0u8; ASK_RESPONSE_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = msg_type as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    header[7..16].fill(0); // pad for 16-byte alignment
    header
}

pub fn write_gossip_frame_prefix(payload_len: usize) -> [u8; GOSSIP_FRAME_HEADER_LEN] {
    let total_size = GOSSIP_HEADER_LEN + payload_len;
    let mut header = [0u8; GOSSIP_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = MessageType::Gossip as u8;
    header[5..16].fill(0);
    header
}

/// Write DirectAsk header - fast path for direct ask without actor message handler
/// Wire format: [length:4][type:1][correlation_id:2][payload_len:4][payload:N]
pub fn write_direct_ask_header(
    correlation_id: u16,
    payload_len: usize,
) -> [u8; DIRECT_ASK_FRAME_HEADER_LEN] {
    let total_size = DIRECT_ASK_HEADER_LEN + payload_len;
    let mut header = [0u8; DIRECT_ASK_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = MessageType::DirectAsk as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    header[7..11].copy_from_slice(&(payload_len as u32).to_be_bytes());
    header[11..16].fill(0);
    header
}

/// Write DirectResponse header - fast path for direct response
/// Wire format: [length:4][type:1][correlation_id:2][payload_len:4][payload:N]
pub fn write_direct_response_header(
    correlation_id: u16,
    payload_len: usize,
) -> [u8; DIRECT_RESPONSE_FRAME_HEADER_LEN] {
    let total_size = DIRECT_RESPONSE_HEADER_LEN + payload_len;
    let mut header = [0u8; DIRECT_RESPONSE_FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&(total_size as u32).to_be_bytes());
    header[4] = MessageType::DirectResponse as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    header[7..11].copy_from_slice(&(payload_len as u32).to_be_bytes());
    header[11..16].fill(0);
    header
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALIGNMENT: usize = 16;

    fn is_aligned(offset: usize) -> bool {
        offset % ALIGNMENT == 0
    }

    #[test]
    fn ask_response_payload_offset_aligned_with_length_prefix() {
        let offset = LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN;
        assert!(is_aligned(offset));
    }

    #[test]
    fn gossip_payload_offset_aligned_with_length_prefix() {
        let offset = LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN;
        assert!(is_aligned(offset));
    }

    #[test]
    fn actor_payload_offset_with_length_prefix() {
        // Wire format: [len:4][type:1][corr:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
        // Payload offset = 4 + 28 = 32, and 32 % 8 = 0 (8-byte aligned!)
        let offset = LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN;
        assert_eq!(offset, 32);
        assert_eq!(offset % ALIGNMENT, 0); // Now 16-byte aligned
    }

    #[test]
    fn codec_trap_length_prefix_stripped_requires_more_padding() {
        let ask_header_without_pad = 1 + 2; // type + correlation_id
        let ask_pad_needed = (ALIGNMENT - (ask_header_without_pad % ALIGNMENT)) % ALIGNMENT;
        assert_eq!(ask_pad_needed, 13);

        let gossip_header_without_pad = 1; // type
        let gossip_pad_needed = (ALIGNMENT - (gossip_header_without_pad % ALIGNMENT)) % ALIGNMENT;
        assert_eq!(gossip_pad_needed, 15);

        // Actor header now includes an 8-byte schema hash plus 1 byte of padding.
        let actor_header_without_pad = 1 + 2 + SCHEMA_HASH_LEN + 8 + 4 + 4;
        let actor_header_with_length_prefix = LENGTH_PREFIX_LEN + actor_header_without_pad; // 4 + 27 = 31
        let actor_pad_needed_for_32 = 32 - actor_header_with_length_prefix; // 32 - 31 = 1
        assert_eq!(actor_pad_needed_for_32, 1); // 1 padding byte (schema hash occupies the rest)
    }

    #[test]
    fn write_ask_response_header_sets_length_and_pad() {
        let payload_len = 3;
        let header = write_ask_response_header(MessageType::Ask, 0x1234, payload_len);
        let total = (ASK_RESPONSE_HEADER_LEN + payload_len) as u32;
        assert_eq!(u32::from_be_bytes(header[0..4].try_into().unwrap()), total);
        assert_eq!(header[4], MessageType::Ask as u8);
        assert_eq!(u16::from_be_bytes(header[5..7].try_into().unwrap()), 0x1234);
        assert_eq!(&header[7..16], &[0u8; 9]);
    }

    #[test]
    fn actor_header_encodes_schema_hash() {
        let payload_len = 0;
        let schema_hash = 0xAABBCCDDEEFF0011u64;
        let header = write_actor_frame_header(
            MessageType::ActorTell,
            0xBEEF,
            0x0102030405060708u64,
            0x11223344u32,
            Some(schema_hash),
            payload_len,
        );

        let reserved = &header[7..16];
        assert_eq!(&reserved[..SCHEMA_HASH_LEN], &schema_hash.to_be_bytes());
        assert_eq!(reserved[SCHEMA_HASH_LEN], 0);
        assert_eq!(read_schema_hash(reserved), Some(schema_hash));
    }

    #[test]
    fn write_gossip_frame_prefix_sets_padding() {
        let payload_len = 10;
        let header = write_gossip_frame_prefix(payload_len);
        let total = (GOSSIP_HEADER_LEN + payload_len) as u32;
        assert_eq!(u32::from_be_bytes(header[0..4].try_into().unwrap()), total);
        assert_eq!(header[4], MessageType::Gossip as u8);
        assert_eq!(&header[5..16], &[0u8; 11]);
    }

    #[test]
    fn header_lengths_hold_for_varied_payload_sizes() {
        for payload_len in 0..256 {
            let ask_header = write_ask_response_header(MessageType::Ask, 0, payload_len);
            let ask_total = (ASK_RESPONSE_HEADER_LEN + payload_len) as u32;
            assert_eq!(
                u32::from_be_bytes(ask_header[0..4].try_into().unwrap()),
                ask_total
            );

            let gossip_header = write_gossip_frame_prefix(payload_len);
            let gossip_total = (GOSSIP_HEADER_LEN + payload_len) as u32;
            assert_eq!(
                u32::from_be_bytes(gossip_header[0..4].try_into().unwrap()),
                gossip_total
            );

            assert!(is_aligned(LENGTH_PREFIX_LEN + ASK_RESPONSE_HEADER_LEN));
            assert!(is_aligned(LENGTH_PREFIX_LEN + GOSSIP_HEADER_LEN));
            // Actor header is now 8-byte aligned (32 % 8 = 0)
            assert!(is_aligned(LENGTH_PREFIX_LEN + ACTOR_HEADER_LEN));
        }
    }
}
