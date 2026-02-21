use crate::{GossipError, Result};
use bytes::{Buf, Bytes};
use std::cell::RefCell;
use std::marker::PhantomData;

const SERIALIZER_POOL_SIZE: usize = 64;
const MAX_POOLED_BUFFER_CAPACITY: usize = 1024 * 1024; // 1MB
const MAX_POOLED_ARENA_CAPACITY: usize = 1024 * 1024; // 1MB

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
    ctx: Option<Box<SerializerCtx>>,
    len: usize,
    pos: usize,
}

impl PooledPayload {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Buf for PooledPayload {
    fn remaining(&self) -> usize {
        self.len.saturating_sub(self.pos)
    }

    fn chunk(&self) -> &[u8] {
        if let Some(ctx) = self.ctx.as_ref() {
            &ctx.writer[self.pos..self.len]
        } else {
            &[]
        }
    }

    fn advance(&mut self, cnt: usize) {
        let remaining = self.remaining();
        let to_advance = cnt.min(remaining);
        self.pos += to_advance;
    }
}

impl Drop for PooledPayload {
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.take() {
            release_ctx(ctx);
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
/// In debug builds, prefixes the payload with the type hash for validation.
///
/// Important: the debug prefix is padded to preserve alignment for total zero-copy archived access
/// on the receiver. (See `PAYLOAD_ALIGNMENT` in `aligned.rs`.)
pub fn encode_typed<T>(value: &T) -> Result<Bytes>
where
    T: WireEncode,
{
    let payload =
        rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(GossipError::Serialization)?;

    #[cfg(debug_assertions)]
    {
        const DEBUG_PREFIX_LEN: usize = 16;
        let mut buf = Vec::with_capacity(DEBUG_PREFIX_LEN + payload.len());
        buf.extend_from_slice(&T::TYPE_HASH.to_be_bytes());
        // Pad to 16 bytes so the archive body stays 16-aligned when the underlying buffer is.
        buf.extend_from_slice(&[0u8; DEBUG_PREFIX_LEN - 8]);
        buf.extend_from_slice(payload.as_ref());
        Ok(Bytes::from(buf))
    }

    #[cfg(not(debug_assertions))]
    {
        Ok(Bytes::copy_from_slice(payload.as_ref()))
    }
}

/// Encode a typed payload using the pooled serializer context.
pub fn encode_typed_pooled<T>(value: &T) -> Result<PooledPayload>
where
    T: WireEncode,
{
    let mut ctx = acquire_ctx();
    let len = encode_typed_in(value, &mut ctx)?;

    Ok(PooledPayload {
        ctx: Some(ctx),
        len,
        pos: 0,
    })
}

/// Wrap a pooled payload with the debug type hash prefix when enabled.
pub fn typed_payload_parts<T: WireType>(
    payload: PooledPayload,
) -> (PooledPayload, Option<[u8; 16]>, usize) {
    #[cfg(debug_assertions)]
    {
        const DEBUG_PREFIX_LEN: usize = 16;
        let mut prefix = [0u8; DEBUG_PREFIX_LEN];
        prefix[..8].copy_from_slice(&T::TYPE_HASH.to_be_bytes());
        let total_len = prefix.len() + payload.len();
        // Note: this widens the prefix compared to the previous 8-byte debug format.
        // Debug builds are not wire-compatible with older debug builds across this change.
        (payload, Some(prefix), total_len)
    }

    #[cfg(not(debug_assertions))]
    {
        let total_len = payload.len();
        return (payload, None, total_len);
    }
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
    fn typed_payload_parts_includes_hash_in_debug() {
        let msg = TestMsg { value: 1 };
        let payload = encode_typed_pooled(&msg).unwrap();
        let (_payload, prefix, total_len) = typed_payload_parts::<TestMsg>(payload);

        #[cfg(debug_assertions)]
        {
            assert!(total_len >= 16);
            assert!(prefix.is_some());
        }
    }

    #[test]
    fn archived_unchecked_body_is_aligned_in_debug_prefix_mode() {
        #[cfg(debug_assertions)]
        {
            #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
            struct AlignedTest {
                v: u128,
            }
            wire_type!(AlignedTest, "typed::AlignedTest");

            // Encode (debug prefix included), then copy into an aligned receive buffer to
            // simulate the real receive path.
            let payload = encode_typed(&AlignedTest { v: 7 }).unwrap();
            let pool = Arc::new(AlignedBytesPool::new(1));
            let aligned = AlignedBytes::from_pooled_slice(payload.as_ref(), Arc::clone(&pool));
            let bytes: Bytes = aligned.into();

            let archived = decode_typed_archived::<AlignedTest>(bytes).unwrap();
            let a = unsafe { archived.archived_unchecked() };
            assert_eq!(a.v, 7);
        }
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

    /// Access the archived payload without validation (total zero-copy).
    ///
    /// This is the fastest path, but it is unsafe: the caller must guarantee that
    /// `self.as_bytes()` contains a valid archived `T` at the root position.
    ///
    /// Safety is generally achieved by:
    /// - only accepting bytes produced by `rkyv::to_bytes` on trusted peers, and
    /// - enforcing alignment at the transport boundary (see `AlignedBytes`).
    pub unsafe fn archived_unchecked(&self) -> &<T as rkyv::Archive>::Archived {
        debug_assert_eq!(
            (self.as_bytes().as_ptr() as usize)
                % std::mem::align_of::<<T as rkyv::Archive>::Archived>(),
            0,
            "misaligned archived root for {} (align={})",
            T::TYPE_NAME,
            std::mem::align_of::<<T as rkyv::Archive>::Archived>()
        );
        // SAFETY: caller guarantees bytes represent a valid archived `T`.
        unsafe { rkyv::access_unchecked::<<T as rkyv::Archive>::Archived>(self.as_bytes()) }
    }
}

/// Decode a typed message from the wire.
///
/// In debug builds, verifies and strips the type hash prefix.
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
    #[cfg(debug_assertions)]
    {
        const DEBUG_PREFIX_LEN: usize = 16;
        if payload.len() < DEBUG_PREFIX_LEN {
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
        let body = &payload[DEBUG_PREFIX_LEN..];
        let archived = rkyv::access::<T::Archived, rkyv::rancor::Error>(body)?;
        let mut pool = rkyv::de::Pool::new();
        let mut deserializer = rkyv::rancor::Strategy::wrap(&mut pool);
        Ok(rkyv::Deserialize::deserialize(archived, &mut deserializer)?)
    }

    #[cfg(not(debug_assertions))]
    {
        let archived = rkyv::access::<T::Archived, rkyv::rancor::Error>(payload)?;
        let mut pool = rkyv::de::Pool::new();
        let mut deserializer = rkyv::rancor::Strategy::wrap(&mut pool);
        Ok(rkyv::Deserialize::deserialize(archived, &mut deserializer)?)
    }
}

/// Decode a typed message into an archived view (zero-copy).
///
/// In debug builds, verifies and strips the type hash prefix without copying.
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
    #[cfg(debug_assertions)]
    {
        const DEBUG_PREFIX_LEN: usize = 16;
        if payload.len() < DEBUG_PREFIX_LEN {
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
        Ok(ArchivedBytes {
            bytes: payload,
            offset: DEBUG_PREFIX_LEN,
            _marker: PhantomData,
        })
    }

    #[cfg(not(debug_assertions))]
    {
        Ok(ArchivedBytes {
            bytes: payload,
            offset: 0,
            _marker: PhantomData,
        })
    }
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
