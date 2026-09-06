use crate::{GossipError, Result};
use bytes::{Buf, Bytes};
use crossbeam_queue::ArrayQueue;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::OnceLock;

const SERIALIZER_POOL_SIZE: usize = 64;
const MAX_POOLED_BUFFER_CAPACITY: usize = 1024 * 1024; // 1MB
const MAX_POOLED_ARENA_CAPACITY: usize = 1024 * 1024; // 1MB
const BYTE_PAYLOAD_POOL_SIZE: usize = 4096;
const MAX_POOLED_BYTE_CAPACITY: usize = 1024 * 1024; // 1MB

fn validate_archive_alignment<A>(payload: &[u8], type_name: &str) -> Result<()> {
    let required = std::mem::align_of::<A>();
    if required > crate::aligned::PAYLOAD_ALIGNMENT {
        return Err(GossipError::InvalidConfig(format!(
            "typed archive alignment for {type_name} requires {required} bytes, exceeding the {}-byte payload guarantee",
            crate::aligned::PAYLOAD_ALIGNMENT
        )));
    }
    if (payload.as_ptr() as usize) % required != 0 {
        return Err(GossipError::InvalidConfig(format!(
            "typed archive alignment for {type_name} requires a {required}-byte-aligned payload"
        )));
    }
    Ok(())
}

struct SerializerCtx {
    writer: rkyv::util::AlignedVec,
    arena: rkyv::ser::allocator::Arena,
}

impl SerializerCtx {
    fn new() -> Self {
        Self {
            writer: rkyv::util::AlignedVec::new(),
            arena: rkyv::ser::allocator::Arena::new(),
        }
    }
}

thread_local! {
    static SERIALIZER_POOL: RefCell<Vec<Box<SerializerCtx>>> = RefCell::new({
        let mut pool = Vec::with_capacity(SERIALIZER_POOL_SIZE);
        for _ in 0..SERIALIZER_POOL_SIZE {
            pool.push(Box::new(SerializerCtx::new()));
        }
        pool
    });
}

static BYTE_PAYLOAD_POOL: OnceLock<ArrayQueue<Vec<u8>>> = OnceLock::new();

fn byte_payload_pool() -> &'static ArrayQueue<Vec<u8>> {
    BYTE_PAYLOAD_POOL.get_or_init(|| ArrayQueue::new(BYTE_PAYLOAD_POOL_SIZE))
}

fn acquire_ctx() -> Box<SerializerCtx> {
    SERIALIZER_POOL.with(|pool| {
        pool.borrow_mut()
            .pop()
            .unwrap_or_else(|| Box::new(SerializerCtx::new()))
    })
}

fn release_ctx(mut ctx: Box<SerializerCtx>) {
    ctx.writer.clear();
    if ctx.writer.capacity() > MAX_POOLED_BUFFER_CAPACITY {
        return;
    }
    if ctx.arena.capacity() > MAX_POOLED_ARENA_CAPACITY {
        ctx.arena = rkyv::ser::allocator::Arena::new();
    } else {
        ctx.arena.shrink();
    }

    SERIALIZER_POOL.with(|pool| {
        let mut guard = pool.borrow_mut();
        if guard.len() < SERIALIZER_POOL_SIZE {
            guard.push(ctx);
        }
    });
}

fn try_acquire_byte_buffer(min_capacity: usize) -> Option<Vec<u8>> {
    if min_capacity > MAX_POOLED_BYTE_CAPACITY {
        return None;
    }
    match byte_payload_pool().pop() {
        Some(buffer) if buffer.capacity() >= min_capacity => Some(buffer),
        Some(mut buffer) => {
            // Grow the undersized buffer so later equal-sized payloads reuse it.
            // Returning it unchanged left the pool full of small buffers and forced
            // a fresh allocation on every larger message.
            // `Vec::reserve` is relative to `len`, not `capacity`; clear first so
            // `reserve(min_capacity)` actually raises capacity to the new size.
            buffer.clear();
            if buffer.capacity() < min_capacity {
                buffer.reserve(min_capacity);
            }
            Some(buffer)
        }
        None => Some(Vec::with_capacity(min_capacity)),
    }
}

fn release_byte_buffer(mut buffer: Vec<u8>) {
    buffer.clear();
    if buffer.capacity() > MAX_POOLED_BYTE_CAPACITY {
        return;
    }
    let _ = byte_payload_pool().push(buffer);
}

pub(crate) fn prewarm_pooled_byte_buffers(count: usize, capacity: usize) {
    let pool = byte_payload_pool();
    for _ in 0..count {
        if pool.is_full() {
            return;
        }
        if pool.push(Vec::with_capacity(capacity)).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod qa_pool_review {
    use super::*;

    #[test]
    fn qa_prewarmed_pool_adapts_to_larger_steady_state_payloads() {
        while byte_payload_pool().pop().is_some() {}
        prewarm_pooled_byte_buffers(BYTE_PAYLOAD_POOL_SIZE, 4096);
        for _ in 0..100 {
            let buffer = try_acquire_byte_buffer(8192).unwrap();
            release_byte_buffer(buffer);
        }
        let mut suitable = 0;
        let mut undersized = 0;
        while let Some(buffer) = byte_payload_pool().pop() {
            if buffer.capacity() >= 8192 {
                suitable += 1;
            } else {
                undersized += 1;
            }
        }
        assert!(
            suitable > 0,
            "larger steady-state messages must become reusable instead of allocating forever (suitable={suitable}, undersized={undersized})"
        );
    }
}

fn encode_typed_in<T>(value: &T, ctx: &mut SerializerCtx) -> Result<usize>
where
    T: WireEncode,
{
    let writer = std::mem::take(&mut ctx.writer);
    let writer = rkyv::api::high::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Error>(
        value,
        writer,
        ctx.arena.acquire(),
    )
    .map_err(GossipError::Serialization)?;
    let len = writer.len();
    ctx.writer = writer;
    Ok(len)
}

/// Pooled payload that implements bytes::Buf without copying.
pub struct PooledPayload {
    inner: Option<PooledPayloadInner>,
    len: usize,
    pos: usize,
}

enum PooledPayloadInner {
    Serializer(Box<SerializerCtx>),
    Bytes(Vec<u8>),
}

impl PooledPayload {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn try_from_pooled_bytes(
        min_capacity: usize,
        fill: impl FnOnce(&mut Vec<u8>),
    ) -> Option<Self> {
        let mut buffer = try_acquire_byte_buffer(min_capacity)?;
        buffer.clear();
        fill(&mut buffer);
        let len = buffer.len();
        Some(Self {
            inner: Some(PooledPayloadInner::Bytes(buffer)),
            len,
            pos: 0,
        })
    }
}

impl Buf for PooledPayload {
    fn remaining(&self) -> usize {
        self.len.saturating_sub(self.pos)
    }

    fn chunk(&self) -> &[u8] {
        match self.inner.as_ref() {
            Some(PooledPayloadInner::Serializer(ctx)) => &ctx.writer[self.pos..self.len],
            Some(PooledPayloadInner::Bytes(buffer)) => &buffer[self.pos..self.len],
            None => &[],
        }
    }

    fn advance(&mut self, cnt: usize) {
        let remaining = self.remaining();
        assert!(
            cnt <= remaining,
            "cannot advance past remaining bytes: requested {cnt}, remaining {remaining}"
        );
        self.pos += cnt;
    }
}

impl Drop for PooledPayload {
    fn drop(&mut self) {
        match self.inner.take() {
            Some(PooledPayloadInner::Serializer(ctx)) => release_ctx(ctx),
            Some(PooledPayloadInner::Bytes(buffer)) => release_byte_buffer(buffer),
            None => {}
        }
    }
}

/// Compile-time wire type marker with a stable hash identifier.
///
/// The hash should be derived from a stable, shared identifier (e.g. a protocol name)
/// so different binaries can agree on the same type mapping.
pub trait WireType {
    const TYPE_HASH: u64;
    const TYPE_NAME: &'static str;
}

/// Helper trait for rkyv-serializable wire types.
pub trait WireEncode:
    WireType
    + for<'a> rkyv::Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >
{
}

impl<T> WireEncode for T where
    T: WireType
        + for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >
{
}

/// Helper trait for rkyv-deserializable wire types.
pub trait WireDecode: WireType + rkyv::Archive + Sized
where
    for<'a> <Self as rkyv::Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<Self, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
}

impl<T> WireDecode for T
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
}

/// FNV-1a 64-bit hash for stable compile-time hashing of string literals.
pub const fn fnv1a_hash(input: &str) -> u64 {
    let bytes = input.as_bytes();
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

/// Encode a typed message for the wire.
///
/// Prefixes the payload with the type hash for validation. This is
/// unconditional in every build mode: debug and release must speak the same
/// wire format for typed payloads, or a mixed debug/release deployment would
/// silently corrupt or misinterpret them.
///
/// Important: the prefix is padded to preserve alignment for total zero-copy archived access
/// on the receiver. (See `PAYLOAD_ALIGNMENT` in `aligned.rs`.)
pub fn encode_typed<T>(value: &T) -> Result<Bytes>
where
    T: WireEncode,
{
    crate::reject_non_zero_copy_path("typed::encode_typed (copying encoder)")?;
    let payload =
        rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(GossipError::Serialization)?;

    const PREFIX_LEN: usize = 16;
    let mut buf = Vec::with_capacity(PREFIX_LEN + payload.len());
    buf.extend_from_slice(&T::TYPE_HASH.to_be_bytes());
    // Pad to 16 bytes so the archive body stays 16-aligned when the underlying buffer is.
    buf.extend_from_slice(&[0u8; PREFIX_LEN - 8]);
    buf.extend_from_slice(payload.as_ref());
    Ok(Bytes::from(buf))
}

/// Encode a typed payload using the pooled serializer context.
pub fn encode_typed_pooled<T>(value: &T) -> Result<PooledPayload>
where
    T: WireEncode,
{
    let mut ctx = acquire_ctx();
    let len = encode_typed_in(value, &mut ctx)?;

    Ok(PooledPayload {
        inner: Some(PooledPayloadInner::Serializer(ctx)),
        len,
        pos: 0,
    })
}

/// Wrap a pooled payload with the type hash prefix. Unconditional in every
/// build mode -- see `encode_typed`.
pub fn typed_payload_parts<T: WireType>(
    payload: PooledPayload,
) -> (PooledPayload, Option<[u8; 16]>, usize) {
    const PREFIX_LEN: usize = 16;
    let mut prefix = [0u8; PREFIX_LEN];
    prefix[..8].copy_from_slice(&T::TYPE_HASH.to_be_bytes());
    let total_len = prefix.len() + payload.len();
    (payload, Some(prefix), total_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aligned::{AlignedBytes, AlignedBytesPool};
    use crate::wire_type;
    use std::sync::Arc;

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct TestMsg {
        value: u64,
    }

    wire_type!(TestMsg, "typed::TestMsg");

    /// The type-hash prefix and its verification must be unconditional: a
    /// debug binary and a release binary must speak the exact same wire
    /// format for every typed payload. `cargo test` always runs in the dev
    /// profile (that attribute is true), so a behavioral test cannot observe
    /// the release path directly; this asserts the *source* no longer
    /// branches on it, which is the only way the two build modes can be
    /// guaranteed to agree. The needles are built by concatenation (never
    /// written as a contiguous literal anywhere in this file) so this test
    /// cannot trip over its own source text.
    #[test]
    fn typed_prefix_logic_does_not_vary_by_cfg_debug_assertions() {
        let source = include_str!("typed.rs");
        let positive: String = ["cfg", "(", "debug_assertions", ")"].concat();
        let negative: String = ["cfg", "(", "not", "(", "debug_assertions", ")", ")"].concat();
        assert!(
            !source.contains(&positive) && !source.contains(&negative),
            "typed.rs must not gate the type-hash prefix (encode/decode, typed_payload_parts) \
             on debug_assertions -- debug and release must be wire-compatible for every typed payload"
        );
    }

    #[test]
    fn pooled_payload_buf_semantics() {
        let msg = TestMsg { value: 42 };
        let mut payload = encode_typed_pooled(&msg).unwrap();
        let remaining = payload.remaining();
        assert!(remaining > 0);
        assert_eq!(payload.chunk().len(), remaining);

        let advance_by = 1.min(remaining);
        payload.advance(advance_by);
        assert_eq!(payload.remaining(), remaining - advance_by);
    }

    #[test]
    #[should_panic(expected = "cannot advance past remaining bytes")]
    fn pooled_payload_rejects_advance_past_remaining() {
        let mut payload = PooledPayload::try_from_pooled_bytes(3, |out| {
            out.extend_from_slice(b"abc");
        })
        .expect("small pooled payload");

        payload.advance(4);
    }

    #[test]
    fn pool_reuse_and_cap_behavior() {
        let msg = TestMsg { value: 7 };
        let payload = encode_typed_pooled(&msg).unwrap();
        drop(payload);

        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
        struct BigMsg {
            data: Vec<u8>,
        }
        wire_type!(BigMsg, "typed::BigMsg");

        let big = BigMsg {
            data: vec![0u8; MAX_POOLED_BUFFER_CAPACITY + 1024],
        };
        let big_payload = encode_typed_pooled(&big).unwrap();
        drop(big_payload);
    }

    #[test]
    fn pooled_byte_payload_allocates_bounded_buffer_when_warm_pool_too_small() {
        let payload = PooledPayload::try_from_pooled_bytes(65_507, |out| {
            out.extend_from_slice(&vec![0u8; 65_507]);
        })
        .expect("bounded on-demand byte payload");
        assert_eq!(payload.len(), 65_507);

        assert!(
            PooledPayload::try_from_pooled_bytes(MAX_POOLED_BYTE_CAPACITY + 1, |_| {}).is_none()
        );
    }

    #[test]
    fn typed_payload_parts_includes_hash_unconditionally() {
        let msg = TestMsg { value: 1 };
        let payload = encode_typed_pooled(&msg).unwrap();
        let (_payload, prefix, total_len) = typed_payload_parts::<TestMsg>(payload);

        assert!(total_len >= 16);
        assert!(prefix.is_some());
    }

    #[test]
    fn archived_body_is_validated_alongside_the_prefix() {
        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
        struct AlignedTest {
            v: u128,
        }
        wire_type!(AlignedTest, "typed::AlignedTest");

        // Encode (prefix included), then copy into an aligned receive buffer to
        // simulate the real receive path.
        let pooled = encode_typed_pooled(&AlignedTest { v: 7 }).unwrap();
        let (payload, prefix, total_len) = typed_payload_parts::<AlignedTest>(pooled);
        let mut encoded = Vec::with_capacity(total_len);
        if let Some(prefix) = prefix {
            encoded.extend_from_slice(&prefix);
        }
        encoded.extend_from_slice(payload.chunk());
        let pool = Arc::new(AlignedBytesPool::new(1));
        let aligned = AlignedBytes::from_pooled_slice(&encoded, Arc::clone(&pool));
        let bytes: Bytes = aligned.into();

        let archived = decode_typed_archived::<AlignedTest>(bytes).unwrap();
        let a = archived.archived().unwrap();
        assert_eq!(a.v, 7);

        let mut malformed = encoded;
        malformed.pop();
        assert!(
            decode_typed_archived::<AlignedTest>(Bytes::from(malformed)).is_err(),
            "a matching type hash must not bypass archived byte validation"
        );
    }

    #[test]
    fn archived_decode_rejects_misaligned_caller_bytes() {
        let payload = encode_typed_pooled(&TestMsg { value: 9 }).unwrap();
        let (payload, prefix, total_len) = typed_payload_parts::<TestMsg>(payload);
        let mut encoded = Vec::with_capacity(total_len);
        if let Some(prefix) = prefix {
            encoded.extend_from_slice(&prefix);
        }
        encoded.extend_from_slice(payload.chunk());
        let archive_alignment = std::mem::align_of::<<TestMsg as rkyv::Archive>::Archived>();
        assert!(archive_alignment > 1);
        let body_offset = 16;
        let mut storage = vec![0_u8; encoded.len() + archive_alignment];
        let storage_address = storage.as_ptr() as usize;
        let offset = (0..archive_alignment)
            .find(|offset| (storage_address + offset + body_offset) % archive_alignment != 0)
            .expect("an over-allocation must contain a misaligned offset");
        storage[offset..offset + encoded.len()].copy_from_slice(&encoded);
        let misaligned = Bytes::from(storage).slice(offset..offset + encoded.len());

        let Err(error) = decode_typed_archived::<TestMsg>(misaligned) else {
            panic!("misaligned caller bytes must be rejected");
        };
        assert!(matches!(
            error,
            GossipError::InvalidConfig(message)
                if message.contains("typed archive alignment")
        ));
    }

    #[test]
    fn archive_alignment_cannot_exceed_transport_guarantee() {
        #[repr(align(32))]
        struct OverAligned;

        let error = validate_archive_alignment::<OverAligned>(&[], "OverAligned")
            .expect_err("over-aligned archives must fail closed");
        assert!(matches!(
            error,
            GossipError::InvalidConfig(message)
                if message.contains("exceeding the 16-byte payload guarantee")
        ));
    }
}

/// Zero-copy wrapper for archived payloads that keeps the underlying bytes alive.
pub struct ArchivedBytes<T> {
    bytes: Bytes,
    offset: usize,
    _marker: PhantomData<T>,
}

impl<T> ArchivedBytes<T> {
    /// Access the raw payload bytes (without the debug type hash prefix).
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[self.offset..]
    }

    /// Return the underlying buffer (includes debug prefix if present).
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl<T> ArchivedBytes<T>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
        + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    /// Access the archived payload with validation.
    pub fn archived(&self) -> Result<&<T as rkyv::Archive>::Archived> {
        Ok(rkyv::access::<
            <T as rkyv::Archive>::Archived,
            rkyv::rancor::Error,
        >(self.as_bytes())?)
    }
}

/// Decode a typed message from the wire.
///
/// Verifies and strips the type hash prefix. Unconditional in every build
/// mode -- see `encode_typed`.
pub fn decode_typed<T>(payload: &[u8]) -> Result<T>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
    const PREFIX_LEN: usize = 16;
    if payload.len() < PREFIX_LEN {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload too short for type hash ({})",
            T::TYPE_NAME
        )));
    }
    let mut hash_bytes = [0u8; 8];
    hash_bytes.copy_from_slice(&payload[..8]);
    let hash = u64::from_be_bytes(hash_bytes);
    if hash != T::TYPE_HASH {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload hash mismatch for {}: expected {:016x}, got {:016x}",
            T::TYPE_NAME,
            T::TYPE_HASH,
            hash
        )));
    }
    let body = &payload[PREFIX_LEN..];
    let archived = rkyv::access::<T::Archived, rkyv::rancor::Error>(body)?;
    let mut pool = rkyv::de::Pool::new();
    let mut deserializer = rkyv::rancor::Strategy::wrap(&mut pool);
    Ok(rkyv::Deserialize::deserialize(archived, &mut deserializer)?)
}

/// Decode a typed message into an archived view (zero-copy).
///
/// Verifies and strips the type hash prefix without copying. Unconditional
/// in every build mode -- see `encode_typed`.
pub fn decode_typed_archived<T>(payload: Bytes) -> Result<ArchivedBytes<T>>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
        + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    const PREFIX_LEN: usize = 16;
    if payload.len() < PREFIX_LEN {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload too short for type hash ({})",
            T::TYPE_NAME
        )));
    }
    let mut hash_bytes = [0u8; 8];
    hash_bytes.copy_from_slice(&payload[..8]);
    let hash = u64::from_be_bytes(hash_bytes);
    if hash != T::TYPE_HASH {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload hash mismatch for {}: expected {:016x}, got {:016x}",
            T::TYPE_NAME,
            T::TYPE_HASH,
            hash
        )));
    }
    validate_archive_alignment::<T::Archived>(&payload[PREFIX_LEN..], T::TYPE_NAME)?;
    // Validate before exposing the zero-copy wrapper. The wrapper owns the
    // bytes, so the validated archive remains stable for its lifetime.
    rkyv::access::<T::Archived, rkyv::rancor::Error>(&payload[PREFIX_LEN..])?;
    Ok(ArchivedBytes {
        bytes: payload,
        offset: PREFIX_LEN,
        _marker: PhantomData,
    })
}

/// Implement WireType with a stable, shared string identifier.
#[macro_export]
macro_rules! wire_type {
    ($ty:ty, $name:expr) => {
        impl $crate::typed::WireType for $ty {
            const TYPE_HASH: u64 = $crate::typed::fnv1a_hash($name);
            const TYPE_NAME: &'static str = $name;
        }
    };
}
