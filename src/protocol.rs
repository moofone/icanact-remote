use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::{Buf, Bytes};
use tracing::{info, warn};

use crate::{
    GossipError, PeerId, Result,
    handle::{
        MessageReadResult, handle_raw_ask_request, handle_response_message, send_ask_nack,
        send_inline_response, send_inline_response_aligned, send_pooled_response,
        send_streaming_response,
    },
    registry::{ActorResponse, GossipRegistry, RegistryMessage},
};

/// How long an in-progress stream may go with ZERO byte progress before it
/// is reaped. Measured from the last time bytes actually advanced -- never
/// from stream start, and never merely from the last time the reader was
/// polled -- so a stream that is genuinely still receiving data is never
/// reaped by this check alone. A peer that stops advancing entirely
/// (including one that never sends a single byte after `StreamStart`) loses
/// its slot and reserved bytes within this window.
///
/// This bound alone is not sufficient to keep a slot from being pinned
/// indefinitely: see `MIN_SUSTAINED_RATE_WINDOW` / `MIN_SUSTAINED_BYTES_PER_WINDOW`
/// for the bound that catches a peer trickling just enough to dodge this one.
///
/// Also doubles as the base tombstone TTL for a reaped/rejected stream id
/// (see `reject_stream` / `tombstone_reaped_stream`): a tombstone hit
/// refreshes its own timer the same way genuine stream activity does, so a
/// sender still trickling into a dead id keeps being silently discarded
/// instead of eventually falling through to a fatal "unknown stream_id".
const STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Width of the tumbling window over which a minimum sustained transfer
/// rate is enforced (see `MIN_SUSTAINED_BYTES_PER_WINDOW`). Deliberately
/// much wider than `STREAM_IDLE_TIMEOUT`: this check exists to catch a
/// pattern the idle check cannot -- nonzero progress on every poll, just
/// under the idle bound -- so it must tolerate the bursty-then-quiet
/// delivery pattern of a real, honest, low-bandwidth link without
/// mistaking a lull for starvation.
const MIN_SUSTAINED_RATE_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);

/// The minimum number of bytes a stream must receive within any one
/// `MIN_SUSTAINED_RATE_WINDOW` to avoid being reaped for insufficient
/// throughput.
///
/// This is the bound that keeps "any progress at all resets the clock"
/// from being exploitable: without it, a peer that sends a single byte just
/// under `STREAM_IDLE_TIMEOUT` forever holds its stream slot and its share
/// of `MAX_INFLIGHT_STREAM_BYTES` for free, which is exactly the slow-drip
/// resource pin an absolute lifetime cap used to prevent. 8 KiB per 5
/// minutes (~27 B/s sustained) is far below any real network transfer --
/// including a link too slow to be worth transferring over at all -- while
/// being thousands of times more than a peer can get away with sending
/// merely to keep the idle timer from firing. A peer that wants to hold a
/// slot open now has to pay for it in real, continuous bandwidth rather
/// than one byte a minute; that does not bound the worst case to a fixed
/// wall-clock ceiling the way the old absolute cap did, but it converts an
/// unbounded *free* resource pin into a bounded-cost one, on top of the
/// existing per-connection `max_concurrent_streams` / `MAX_INFLIGHT_STREAM_BYTES`
/// budgets that already limit the blast radius of any single connection.
const MIN_SUSTAINED_BYTES_PER_WINDOW: usize = 8 * 1024;

/// Per-connection streaming state for managing partial streams
#[derive(Debug)]
pub struct StreamingState {
    active_streams: HashMap<u64, InProgressStream>,
    /// Rejected/reaped stream ids are quarantined so their trailing frames
    /// can be discarded without allocating or tearing down unrelated
    /// traffic. See `RejectedStreamTombstone` for the invariant governing
    /// how long an entry survives, and `REJECTED_STREAMS_BITMAP_WORD_BUDGET`
    /// for the aggregate bound on how much of this table can exist at once.
    rejected_streams: HashMap<u64, RejectedStreamTombstone>,
    /// Running total of `received_chunks.len()` (bitmap words) across every
    /// entry in `rejected_streams`, kept in sync by `try_insert_tombstone`/
    /// `remove_tombstone` so the aggregate budget can be checked in O(1)
    /// rather than re-summing the whole table on every insertion.
    rejected_streams_bitmap_words: usize,
    max_concurrent_streams: usize,
}

/// Aggregate cap, in bitmap words (8 bytes each; see
/// `RejectedStreamTombstone::received_chunks`), on how much
/// chunk-completion-tracking memory `rejected_streams` may hold across *all*
/// its tombstones combined. `131_072` words is 1 MiB -- generous for
/// ordinary operation (a realistically-sized stride needs only a handful of
/// words per tombstone regardless of declared size) but a hard ceiling on
/// the worst case (a peer pairing a huge declared size with a
/// one-byte-stride first chunk on every rejected/reaped generation).
///
/// This is a *budget*, not a capacity-eviction policy: exceeding it never
/// evicts an existing tombstone to make room (see `RejectedStreamTombstone`'s
/// invariant -- that is exactly the bug this table's redesign removed, since
/// any existing entry might still have a frame in flight). Instead,
/// `try_insert_tombstone` refuses the *new* entry that would cross the
/// budget, and its caller turns that refusal into a hard, connection-fatal
/// error (see `begin_v5_stream_or_discard` and `cleanup_stale_with`'s
/// return values). The peer whose traffic caused the pressure bears the
/// cost -- its connection closes -- rather than an unrelated stream losing
/// the tombstone it still needs.
const REJECTED_STREAMS_BITMAP_WORD_BUDGET: usize = 131_072;

/// Hard ceiling on the number of *entries* `rejected_streams` may hold,
/// independent of `REJECTED_STREAMS_BITMAP_WORD_BUDGET`. The byte budget
/// alone cannot bound an entry whose bitmap costs zero words -- two such
/// paths have been found in this table's history (an unvalidated empty
/// first V5 chunk, and a legacy stream reaped before its stride was ever
/// established; see `begin_v5_stream`'s `first_chunk_len == 0` check and
/// `tombstone_reaped_stream`'s `chunk_stride.is_none()` check) -- so every
/// tombstone, regardless of its own bitmap size, also costs exactly one
/// unit against this budget. `4096` is far beyond any legitimate backlog
/// (`max_concurrent_streams` bounds how many streams can be active at once,
/// and a rejected/reaped generation's tombstone is removed as soon as its
/// own completion is observed) while still capping the table's fixed
/// per-entry overhead -- the `RejectedStreamTombstone` struct plus
/// `HashMap` bookkeeping, independent of `received_chunks`'s length -- at a
/// small, constant worst case, exactly the property a *count* budget adds
/// that a *byte* budget cannot: it does not depend on every entry
/// continuing to cost a nonzero number of words, which is a property of
/// today's two insertion paths, not a guarantee this table can enforce
/// against a third one.
const REJECTED_STREAMS_ENTRY_BUDGET: usize = 4096;

#[cfg(test)]
thread_local! {
    /// Test-only instrumentation: the word count of the last bitmap
    /// `RejectedStreamTombstone::establish_stride` actually allocated on
    /// this thread, or `None` if it has not allocated one on this thread
    /// since the test last reset it. Thread-local (not a shared counter) so
    /// this stays reliable under `cargo test`'s default parallelism -- each
    /// `#[test]` runs to completion on one thread, so a shared counter
    /// would otherwise be contaminated by unrelated tests allocating
    /// bitmaps concurrently on other threads. Lets a test assert that a
    /// budget-exhausted rejection prevented the allocation itself, not
    /// merely that the tombstone was not inserted afterward -- see
    /// `reject_stream`.
    static LAST_ESTABLISHED_BITMAP_WORDS: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Chunk-completion state for a rejected or reaped stream generation,
/// tracked precisely enough to know the instant no protocol-compliant
/// sender can have any more bytes left to send for it.
///
/// **Invariant:** a tombstone is removed only when it is *proven* done --
/// either every chunk index its declared size implies has been observed
/// (`is_complete`), or a new `StreamStart` for the same `stream_id` arrives
/// (an unambiguous generation boundary on an ordered transport; handled by
/// `begin_v5_stream_or_discard`'s unconditional `remove_tombstone` before it
/// decides whether to admit or re-reject the retry). No existing tombstone
/// is ever evicted to make room for another -- that is the one property
/// every earlier attempt got wrong (see below) -- but a *new* tombstone can
/// be refused outright by `REJECTED_STREAMS_BITMAP_WORD_BUDGET`, which is a
/// bound on the table's aggregate size, not on any individual entry's
/// lifetime.
///
/// This is fundamentally a *lifetime* property, which is why it replaces
/// three earlier attempts that were all fighting the wrong dimension:
/// - A wall-clock TTL pruned tombstones on a timer that could fire moments
///   before the very chunk that would have refreshed it was read.
/// - Removing that TTL with no replacement left `reject_stream` needing to
///   fail once the table filled, turning ordinary sustained overload into a
///   connection-fatal error.
/// - Evicting the least-recently-touched entry at a fixed 32-entry capacity
///   (the previous fix) always has a safe-looking answer, but "which
///   tombstone to sacrifice" has no correct answer at all: streaming frames
///   interleave, so nothing bounds how many *other* streams can be
///   rejected while one specific rejected generation still has trailing
///   chunks in flight. Any fixed count can be exceeded, and evicting
///   *anything* to make room risks evicting a tombstone still needed.
///
/// Tracking exact per-generation completion sidesteps that question for
/// every *existing* entry: an entry's own lifetime is driven entirely by
/// its own generation's completion, never by how many other rejections
/// happen around it. The aggregate word budget above is a separate,
/// orthogonal bound -- on how much the table may grow *at all* -- with an
/// explicit, non-silent failure mode (connection teardown) rather than a
/// silent eviction, so the two concerns (an entry's lifetime vs. the
/// table's aggregate size) are never conflated the way the LRU attempt
/// conflated them.
#[derive(Debug, Clone)]
struct RejectedStreamTombstone {
    /// Total size declared by this generation's `StreamStart`, in bytes.
    total_size: u64,
    /// Chunk length established from the first chunk this tombstone ever
    /// saw (the inline payload on `StreamStart` for a fresh rejection, or
    /// whatever the stream had already determined before being reaped).
    /// `None` only for a degenerate generation whose stride could never be
    /// established (an empty first chunk) -- such a tombstone is cleared
    /// only by a retry, never by completion tracking; see `is_complete`.
    chunk_stride: Option<usize>,
    expected_chunks: Option<usize>,
    /// One bit per chunk index, exactly mirroring
    /// `InProgressStream::received_chunks`. A byte total alone cannot
    /// safely detect completion -- a duplicated chunk could stand in for
    /// one that never arrived -- so distinct indices are tracked instead.
    received_chunks: Vec<u64>,
}

impl RejectedStreamTombstone {
    /// A brand-new resource-pressure rejection. `first_chunk_len` bytes are
    /// the `StreamStart` frame's own inline payload -- already fully
    /// consumed off the wire as part of that one frame, and chunk index 0 in
    /// the same numbering `reserve_v5_chunk` uses -- so this establishes the
    /// stride from it immediately and marks chunk 0 received; the sender
    /// will not send it again.
    fn rejected(total_size: u64, first_chunk_len: usize) -> Self {
        let mut tombstone = Self {
            total_size,
            chunk_stride: None,
            expected_chunks: None,
            received_chunks: Vec::new(),
        };
        tombstone.establish_stride(first_chunk_len);
        tombstone.mark_chunk_received(0);
        tombstone
    }

    /// A stream reaped out of `active_streams` mid-transfer: carries over
    /// exactly the chunk-completion state it had already validated while
    /// active (see `commit_v5_chunk`), so nothing already confirmed
    /// received has to be re-derived, and nothing merely *reserved* but not
    /// yet committed is ever counted as received before it actually is.
    fn reaped(
        total_size: u64,
        chunk_stride: Option<usize>,
        expected_chunks: Option<usize>,
        received_chunks: Vec<u64>,
    ) -> Self {
        Self {
            total_size,
            chunk_stride,
            expected_chunks,
            received_chunks,
        }
    }

    /// Establishes the stride once, from the first chunk length observed.
    /// A zero length leaves the stride unknown -- there is no valid stride
    /// to derive from an empty chunk (mirrors `reserve_v5_chunk`, which
    /// rejects a zero-length chunk before ever reaching stride derivation).
    fn establish_stride(&mut self, chunk_len: usize) {
        if self.chunk_stride.is_some() || chunk_len == 0 {
            return;
        }
        self.chunk_stride = Some(chunk_len);
        let expected = Self::expected_chunk_count(self.total_size, chunk_len);
        self.expected_chunks = Some(expected);
        let words = Self::bitmap_words_for(expected);
        #[cfg(test)]
        LAST_ESTABLISHED_BITMAP_WORDS.with(|cell| cell.set(Some(words)));
        self.received_chunks = vec![0u64; words];
    }

    /// The chunk count `establish_stride` would derive for a stride of
    /// `chunk_len` against a declared `total_size` -- pure arithmetic, no
    /// allocation. `chunk_len` must be nonzero (see `establish_stride`).
    /// Shared with `StreamingState::reject_stream` so it can size (and
    /// budget-check) the bitmap `establish_stride` would allocate *before*
    /// ever constructing the tombstone that allocates it, using the exact
    /// same formula rather than a hand-derived copy that could drift.
    fn expected_chunk_count(total_size: u64, chunk_len: usize) -> usize {
        (total_size as usize).div_ceil(chunk_len)
    }

    /// The chunk-completion bitmap word count for `expected_chunks` chunks
    /// (one bit per chunk, packed into `u64` words) -- pure arithmetic, no
    /// allocation. See `expected_chunk_count`.
    fn bitmap_words_for(expected_chunks: usize) -> usize {
        expected_chunks.div_ceil(64)
    }

    /// Records `chunk_index` as received. Silently ignores an index outside
    /// the established range (e.g. a malformed/out-of-range chunk on a
    /// stream we are discarding anyway, or the stride was never
    /// established) rather than panicking -- this is best-effort completion
    /// bookkeeping for a stream we are not otherwise validating, not a
    /// source of truth that must reject bad input.
    fn mark_chunk_received(&mut self, chunk_index: usize) {
        let word = chunk_index / 64;
        let bit = 1u64 << (chunk_index % 64);
        if let Some(slot) = self.received_chunks.get_mut(word) {
            *slot |= bit;
        }
    }

    /// True once every expected chunk index has been observed -- the exact
    /// point at which a protocol-compliant sender has no more bytes left to
    /// send for this generation. Mirrors `InProgressStream::all_chunks_received`.
    fn is_complete(&self) -> bool {
        let Some(expected) = self.expected_chunks else {
            return self.total_size == 0;
        };
        if expected == 0 {
            return true;
        }
        let full_words = expected / 64;
        if self.received_chunks[..full_words]
            .iter()
            .any(|w| *w != u64::MAX)
        {
            return false;
        }
        let remainder = expected % 64;
        if remainder == 0 {
            return true;
        }
        let mask = (1u64 << remainder) - 1;
        self.received_chunks[full_words] & mask == mask
    }
}

/// A validated, not-yet-committed V5 chunk destination. The IO task receives
/// plaintext directly into this range and commits it only after every byte was
/// read, so a torn frame can never advance the integrity bitmap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamChunkReservation {
    stream_id: u64,
    chunk_index: usize,
    offset: usize,
    len: usize,
}

impl StreamChunkReservation {
    pub(crate) fn len(self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(self) -> bool {
        self.len == 0
    }

    pub(crate) fn stream_id(self) -> u64 {
        self.stream_id
    }

    pub(crate) fn chunk_index(self) -> usize {
        self.chunk_index
    }
}

/// Completed direct-read V5 stream. Its payload owns the same pooled aligned
/// allocation that was reserved at StreamStart.
#[derive(Debug)]
pub(crate) struct CompletedV5Stream {
    pub actor_id: u64,
    pub type_hash: u32,
    pub correlation_id: u32,
    pub is_response: bool,
    pub payload: crate::AlignedBytes,
}

/// A stream that is currently being assembled
#[derive(Debug)]
struct InProgressStream {
    stream_id: u64,
    total_size: u64,
    type_hash: u32,
    actor_id: u64,
    correlation_id: u32,
    is_response: bool,
    schema_hash: Option<u64>,
    received_size: usize,
    /// Pre-allocated aligned buffer for final message assembly.
    buffer: crate::PooledAlignedBuffer,
    /// Chunk stride used to calculate offsets.
    chunk_stride: Option<usize>,
    /// Bitmap of chunk indices already written, one bit per chunk.
    ///
    /// Sized once the stride is known (from the first chunk). Without it,
    /// completion was decided purely on the sum of received bytes, so a
    /// duplicated chunk could stand in for one that never arrived and the
    /// stream would assemble with an unwritten (zero-filled) hole.
    received_chunks: Vec<u64>,
    /// Number of chunks this stream expects, known once the stride is.
    expected_chunks: Option<usize>,
    /// Count of chunk indices seen more than once, for diagnostics.
    duplicate_chunks: u32,
    /// Timestamp when stream started. Kept for diagnostics only -- neither
    /// reap check is measured from this; see `last_activity` and
    /// `rate_window_started_at`.
    started_at: std::time::Instant,
    /// Timestamp of the most recent genuine byte progress (drives the
    /// zero-progress idle reap).
    last_activity: std::time::Instant,
    /// Total bytes physically read for this stream so far, including bytes
    /// already read into a chunk reservation that has not committed yet.
    ///
    /// Deliberately separate from `received_size`, which only advances on a
    /// full chunk commit: crediting the rate window off `received_size`
    /// alone means a single large chunk that is genuinely, steadily
    /// progressing looks like zero progress until it fully commits, and
    /// gets reaped mid-transfer for insufficient rate -- reintroducing,
    /// through the rate bound, the exact failure this whole change exists
    /// to fix. This field is bumped by the exact number of new bytes a real
    /// socket read returns, so it can only advance at the cost of the peer
    /// actually sending that many bytes; there is no path that credits it
    /// without a matching read.
    total_bytes_progressed: usize,
    /// Start of the current minimum-sustained-rate tumbling window.
    rate_window_started_at: std::time::Instant,
    /// `total_bytes_progressed` as of `rate_window_started_at`, so the
    /// reaper can tell how many bytes landed during the current window
    /// without a separate running counter.
    progressed_bytes_at_window_start: usize,
}

impl InProgressStream {
    /// Records `chunk_index` as received. Returns `false` if it was already
    /// recorded (a duplicate), in which case the caller must not write the
    /// payload or advance `received_size` again.
    fn mark_chunk_received(&mut self, chunk_index: usize) -> bool {
        let word = chunk_index / 64;
        let bit = 1u64 << (chunk_index % 64);
        if word >= self.received_chunks.len() {
            return false;
        }
        if self.received_chunks[word] & bit != 0 {
            self.duplicate_chunks = self.duplicate_chunks.saturating_add(1);
            return false;
        }
        self.received_chunks[word] |= bit;
        true
    }

    /// True once every expected chunk index has been written.
    ///
    /// A stream whose stride is not yet known has received no chunks, so it is
    /// complete only if it declared no payload at all.
    fn all_chunks_received(&self) -> bool {
        let Some(expected) = self.expected_chunks else {
            return self.total_size == 0;
        };
        if expected == 0 {
            return true;
        }
        let full_words = expected / 64;
        if self.received_chunks[..full_words]
            .iter()
            .any(|w| *w != u64::MAX)
        {
            return false;
        }
        let remainder = expected % 64;
        if remainder == 0 {
            return true;
        }
        let mask = (1u64 << remainder) - 1;
        self.received_chunks[full_words] & mask == mask
    }
}

impl StreamingState {
    pub fn new() -> Self {
        Self {
            active_streams: HashMap::new(),
            rejected_streams: HashMap::new(),
            rejected_streams_bitmap_words: 0,
            max_concurrent_streams: 16, // Reasonable limit
        }
    }

    pub fn start_stream_with_correlation(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        schema_hash: Option<u64>,
    ) -> Result<()> {
        self.start_stream_with_correlation_and_kind(
            header,
            correlation_id,
            pool,
            schema_hash,
            false,
        )
    }

    pub fn start_stream_with_correlation_and_kind(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        schema_hash: Option<u64>,
        is_response: bool,
    ) -> Result<()> {
        // A repeated StreamStart must never be reclassified as resource
        // pressure merely because the connection is already at capacity. The
        // subsequent duplicate chunk remains a fatal protocol error.
        if self.active_streams.contains_key(&header.stream_id) {
            return Ok(());
        }

        // Malformed metadata is validated *before* the capacity check below,
        // deliberately -- not merely for tidiness. `begin_v5_stream_or_discard`
        // reclassifies a `ResourceBusy` from this function into a clean,
        // stream-local discard and builds a `RejectedStreamTombstone` from
        // the *same* `total_size`/`first_chunk_len` this call received. If
        // an out-of-range `total_size` could reach that path unvalidated
        // (which it could, when capacity happened to be full first), a peer
        // could pair a one-byte first chunk with an enormous declared size
        // and have the tombstone's chunk-completion bitmap attempt an
        // allocation sized off that declared value -- a discard path is
        // exactly the wrong place to trust unvalidated attacker input for an
        // allocation size. Validating here first means capacity pressure can
        // never mask a malformed declaration: it is always the fatal
        // `InvalidData` a bogus `total_size` deserves, never `ResourceBusy`.
        let total_size = usize::try_from(header.total_size).map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stream size overflows usize",
            ))
        })?;

        if total_size > crate::MAX_STREAM_SIZE {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stream size {} exceeds MAX_STREAM_SIZE {}",
                    total_size,
                    crate::MAX_STREAM_SIZE
                ),
            )));
        }

        if self.active_streams.len() >= self.max_concurrent_streams {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "Too many concurrent streams",
            )));
        }

        // Bound aggregate eager allocation across all in-flight streams on
        // this connection. A StreamStart pre-allocates its full declared
        // size, so cap the sum to keep a peer from opening many max-size
        // streams and forcing ~1 GiB of eager allocation (DoS). Summed on
        // demand over the (<= max_concurrent_streams) active entries.
        let inflight_bytes: usize = self
            .active_streams
            .values()
            .map(|s| s.total_size as usize)
            .sum();
        if inflight_bytes.saturating_add(total_size) > crate::MAX_INFLIGHT_STREAM_BYTES {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                format!(
                    "per-connection in-flight stream budget exceeded: {} in flight + {} requested > {}",
                    inflight_bytes,
                    total_size,
                    crate::MAX_INFLIGHT_STREAM_BYTES
                ),
            )));
        }
        let buffer = crate::PooledAlignedBuffer::with_len(total_size, pool);
        let stream = InProgressStream {
            stream_id: header.stream_id,
            total_size: header.total_size,
            type_hash: header.type_hash,
            actor_id: header.actor_id,
            correlation_id,
            is_response,
            schema_hash,
            received_size: 0,
            buffer,
            chunk_stride: None,
            received_chunks: Vec::new(),
            expected_chunks: None,
            duplicate_chunks: 0,
            started_at: std::time::Instant::now(),
            last_activity: std::time::Instant::now(),
            total_bytes_progressed: 0,
            rate_window_started_at: std::time::Instant::now(),
            progressed_bytes_at_window_start: 0,
        };
        self.active_streams.insert(header.stream_id, stream);
        Ok(())
    }

    /// Starts a V5 stream and reserves its first data-bearing Start frame.
    pub(crate) fn begin_v5_stream(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        is_response: bool,
        first_chunk_len: usize,
    ) -> Result<StreamChunkReservation> {
        // Validated *before* `start_stream_with_correlation_and_kind`'s
        // capacity check, for the same reason `total_size` is (see that
        // call's comment below): `begin_v5_stream_or_discard` reclassifies
        // a `ResourceBusy` from that check into a stream-local discard and
        // builds a `RejectedStreamTombstone` from this same
        // `first_chunk_len`. `reserve_v5_chunk` (the only other place a
        // zero-length chunk is normally rejected, with "V5 stream chunk is
        // empty") is never reached in that path -- capacity fails first --
        // so an empty first chunk previously reached tombstone
        // construction unvalidated. `RejectedStreamTombstone::establish_stride`
        // treats a zero length as "stride unknown" and leaves
        // `received_chunks` empty, so that tombstone charges *zero* words
        // against the aggregate budget: a peer at capacity could send
        // unlimited distinct header-only `StreamStart`s and grow
        // `rejected_streams` without bound, straight past the budget that
        // exists to prevent exactly that. A legitimate V5 stream never has
        // a zero-length first chunk regardless of `total_size` (a
        // genuinely empty message uses `start_stream_with_correlation`,
        // not this path), so this is unconditionally fatal, matching
        // `reserve_v5_chunk`'s existing rejection for the same shape.
        if first_chunk_len == 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk is empty",
            )));
        }
        if first_chunk_len > header.total_size as usize {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 StreamStartData has invalid first chunk length",
            )));
        }
        self.start_stream_with_correlation_and_kind(
            header,
            correlation_id,
            pool,
            None,
            is_response,
        )?;
        self.reserve_v5_chunk(header.stream_id, 0, first_chunk_len)
    }

    /// Convert only bounded resource-pressure rejections into a stream-local
    /// discard. Malformed frames stay fatal protocol errors -- and so does a
    /// rejection that cannot be tombstoned because
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET` is already exhausted: closing
    /// this connection is the explicit, non-silent failure mode for that
    /// case (see `reject_stream`), not silently admitting a stream this
    /// connection has no room for, and not evicting some other, unrelated
    /// tombstone to make room.
    pub(crate) fn begin_v5_stream_or_discard(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u32,
        pool: Arc<crate::AlignedBytesPool>,
        is_response: bool,
        first_chunk_len: usize,
    ) -> Result<Option<StreamChunkReservation>> {
        // TCP preserves frame order: every trailing frame for the rejected
        // generation arrives before a later StreamStart. A new start is
        // therefore an unambiguous generation boundary, not a reason to
        // suppress a legitimate retry until the tombstone expires.
        self.remove_tombstone(header.stream_id);
        let total_size = header.total_size;
        match self.begin_v5_stream(header, correlation_id, pool, is_response, first_chunk_len) {
            Ok(reservation) => Ok(Some(reservation)),
            Err(error) if is_resource_busy(&error) => {
                if self.reject_stream(header.stream_id, total_size, first_chunk_len) {
                    Ok(None)
                } else {
                    Err(GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::QuotaExceeded,
                        "rejected-stream tombstone budget exhausted",
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Validates a V5 chunk before the reader receives it. This does not mark
    /// the bitmap; callers must call `commit_v5_chunk` after a complete read.
    pub(crate) fn reserve_v5_chunk(
        &mut self,
        stream_id: u64,
        chunk_index: u32,
        chunk_len: usize,
    ) -> Result<StreamChunkReservation> {
        let stream = self.active_streams.get_mut(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk for unknown stream_id={stream_id}"),
            ))
        })?;
        if chunk_len == 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk is empty",
            )));
        }
        let chunk_index = chunk_index as usize;
        let stride = match stream.chunk_stride {
            Some(stride) => stride,
            None => {
                stream.chunk_stride = Some(chunk_len);
                let expected = (stream.total_size as usize).div_ceil(chunk_len);
                stream.expected_chunks = Some(expected);
                stream.received_chunks = vec![0u64; expected.div_ceil(64)];
                chunk_len
            }
        };
        let expected = stream.expected_chunks.expect("stride sets expected chunks");
        if chunk_index >= expected {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk index out of range",
            )));
        }
        let offset = chunk_index.checked_mul(stride).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk offset overflow",
            ))
        })?;
        let remaining = stream.total_size as usize - offset;
        let expected_len = remaining.min(stride);
        if chunk_len != expected_len {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream chunk length does not match its index",
            )));
        }
        let word = chunk_index / 64;
        let bit = 1u64 << (chunk_index % 64);
        if stream.received_chunks[word] & bit != 0 {
            stream.duplicate_chunks = stream.duplicate_chunks.saturating_add(1);
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "duplicate V5 stream chunk",
            )));
        }
        Ok(StreamChunkReservation {
            stream_id,
            chunk_index,
            offset,
            len: chunk_len,
        })
    }

    pub(crate) fn reserve_v5_chunk_or_discard(
        &mut self,
        stream_id: u64,
        chunk_index: u32,
        chunk_len: usize,
    ) -> Result<Option<StreamChunkReservation>> {
        if let Some(tombstone) = self.rejected_streams.get_mut(&stream_id) {
            // A sender still trickling chunks into a reaped/rejected id is
            // making a good-faith delivery attempt; record it toward this
            // generation's completion instead of merely refreshing a
            // timestamp. Once every chunk the generation declared has been
            // observed, no compliant sender has anything left to send for
            // it, so the tombstone can be (and is) removed right here --
            // see `RejectedStreamTombstone`'s invariant.
            tombstone.mark_chunk_received(chunk_index as usize);
            if tombstone.is_complete() {
                self.remove_tombstone(stream_id);
            }
            return Ok(None);
        }
        self.reserve_v5_chunk(stream_id, chunk_index, chunk_len)
            .map(Some)
    }

    /// Marks `chunk_index` received for `stream_id`'s tombstone, if one
    /// exists, removing the tombstone if this was its last outstanding
    /// chunk. The completion counterpart for a chunk that was already
    /// *reserved* (mid-read, via `ReadStreamPayload`) when its owning
    /// stream was reaped out from under it: that chunk's `StreamData`
    /// header was already parsed and its reservation already established
    /// before the reap happened, so it can never re-enter through
    /// `reserve_v5_chunk_or_discard`'s own "already tombstoned" lookup --
    /// there is no second frame for the same chunk coming. Completion has
    /// to be recorded here instead, once the read side (see
    /// `connection_pool::read_pipeline::discard_remainder_of_reservation`)
    /// finishes draining the chunk's remaining bytes off the wire. A no-op
    /// if no tombstone exists for `stream_id` (e.g. already superseded by
    /// a retry).
    pub(crate) fn mark_reap_discarded_chunk_received(
        &mut self,
        stream_id: u64,
        chunk_index: usize,
    ) {
        if let Some(tombstone) = self.rejected_streams.get_mut(&stream_id) {
            tombstone.mark_chunk_received(chunk_index);
            if tombstone.is_complete() {
                self.remove_tombstone(stream_id);
            }
        }
    }

    /// Tombstones a resource-pressure-rejected `StreamStart`. Never evicts
    /// an existing tombstone to make room -- that is exactly the bug this
    /// table's redesign removed, since any existing entry might still have
    /// a frame in flight -- so it can fail outright when
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET` or `REJECTED_STREAMS_ENTRY_BUDGET`
    /// is already exhausted. Returns `true` if the tombstone was recorded
    /// (or none was needed -- see below), `false` if either budget refused it; the caller
    /// (`begin_v5_stream_or_discard`) turns `false` into a hard,
    /// connection-fatal error rather than silently discarding the stream
    /// with no way to recognize its own trailing chunks later.
    ///
    /// When the `StreamStart`'s own inline first chunk already covers the
    /// entire declared size, `RejectedStreamTombstone::rejected` produces
    /// an already-complete tombstone: nothing more will ever arrive for
    /// this generation, so there is nothing left to quarantine. Skipping
    /// insertion in that case (mirroring `tombstone_reaped_stream`'s
    /// identical check) matters because a complete tombstone can *only*
    /// ever be removed by completion (already true, so it will never
    /// trigger again) or by a retry of this exact `stream_id` -- which,
    /// with a peer that never reuses ids, may never happen. Inserting it
    /// anyway would leak one map entry per single-frame rejection forever.
    ///
    /// A single expected chunk (`first_chunk_len >= total_size`) is exactly
    /// the condition under which the tombstone `RejectedStreamTombstone::
    /// rejected` would build is already complete (see
    /// `RejectedStreamTombstone::expected_chunk_count`/`is_complete`): chunk
    /// 0 is marked received at construction, and one chunk is all there is.
    /// Checked here, before either the budget or the tombstone itself, using
    /// the same allocation-free arithmetic `establish_stride` uses, so this
    /// never-inserted case is also never charged bitmap-word accounting it
    /// would not actually need.
    ///
    /// Otherwise, the projected bitmap word count is checked against
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET` *before* constructing the
    /// tombstone that would allocate it. A valid `MAX_STREAM_SIZE`
    /// declaration paired with a one-byte first chunk needs ~1,048,576 words
    /// (8 MiB) against a 131,072-word (1 MiB) budget; building the tombstone
    /// first and only then consulting the budget (the previous shape of this
    /// function) pays for that allocation on every such rejection regardless
    /// of whether the budget would have refused it.
    fn reject_stream(&mut self, stream_id: u64, total_size: u64, first_chunk_len: usize) -> bool {
        let expected_chunks =
            RejectedStreamTombstone::expected_chunk_count(total_size, first_chunk_len);
        if expected_chunks <= 1 {
            self.remove_tombstone(stream_id);
            return true;
        }
        let new_words = RejectedStreamTombstone::bitmap_words_for(expected_chunks);
        if self.projected_bitmap_words(stream_id, new_words).is_none() {
            return false;
        }
        let tombstone = RejectedStreamTombstone::rejected(total_size, first_chunk_len);
        debug_assert!(
            !tombstone.is_complete(),
            "expected_chunks > 1 above must not agree with is_complete()"
        );
        self.try_insert_tombstone(stream_id, tombstone)
    }

    /// The aggregate bitmap word count `rejected_streams` would hold if
    /// `stream_id`'s tombstone were (re)inserted needing `new_words` words,
    /// or `None` if that would exceed `REJECTED_STREAMS_BITMAP_WORD_BUDGET`
    /// *or* `REJECTED_STREAMS_ENTRY_BUDGET`. An existing tombstone already
    /// charged against both budgets for the same `stream_id` is replaced,
    /// not added to, for either dimension -- replacing it can never grow
    /// the entry count, so only a genuinely new `stream_id` is subject to
    /// the entry-count check. Split out from `try_insert_tombstone` so
    /// `reject_stream` can run the same check *before* the (potentially
    /// large) bitmap `new_words` describes is ever allocated.
    fn projected_bitmap_words(&self, stream_id: u64, new_words: usize) -> Option<usize> {
        let is_new_entry = !self.rejected_streams.contains_key(&stream_id);
        if is_new_entry && self.rejected_streams.len() >= REJECTED_STREAMS_ENTRY_BUDGET {
            return None;
        }
        let previous_words = self
            .rejected_streams
            .get(&stream_id)
            .map_or(0, |existing| existing.received_chunks.len());
        let projected_words = self
            .rejected_streams_bitmap_words
            .saturating_sub(previous_words)
            .saturating_add(new_words);
        (projected_words <= REJECTED_STREAMS_BITMAP_WORD_BUDGET).then_some(projected_words)
    }

    /// Attempts to insert (or replace) a tombstone, respecting
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET` and
    /// `REJECTED_STREAMS_ENTRY_BUDGET`. Returns `false` -- without touching
    /// `rejected_streams` at all -- if doing so would exceed either budget;
    /// every existing entry is left exactly as it was. Keeps
    /// `rejected_streams_bitmap_words` in sync so the byte-budget check
    /// stays O(1) rather than re-summing the whole table on every call (the
    /// entry-count check is already O(1) via `HashMap::len`).
    fn try_insert_tombstone(&mut self, stream_id: u64, tombstone: RejectedStreamTombstone) -> bool {
        let new_words = tombstone.received_chunks.len();
        let Some(projected_words) = self.projected_bitmap_words(stream_id, new_words) else {
            return false;
        };
        self.rejected_streams_bitmap_words = projected_words;
        self.rejected_streams.insert(stream_id, tombstone);
        true
    }

    /// Removes a tombstone, if present, keeping
    /// `rejected_streams_bitmap_words` in sync.
    fn remove_tombstone(&mut self, stream_id: u64) {
        if let Some(removed) = self.rejected_streams.remove(&stream_id) {
            self.rejected_streams_bitmap_words = self
                .rejected_streams_bitmap_words
                .saturating_sub(removed.received_chunks.len());
        }
    }

    /// Returns the writable target range for the next read into
    /// `reservation`. This function is called on every poll of the
    /// underlying socket read -- including polls where nothing is actually
    /// transferred -- so it must stay free of any side effect on the
    /// stream's activity timer. Callers advance progress explicitly via
    /// `record_v5_chunk_progress` after a read that returns at least one
    /// byte.
    ///
    /// Returns a `NotFound` network error (checked by `is_stream_reaped_error`)
    /// if the owning stream was reaped while this reservation was still
    /// live. Callers must treat that as a per-stream discard, not a fatal
    /// connection error: the remainder of the in-flight chunk still has to
    /// be drained off the wire to keep framing intact, but the stream's
    /// backing buffer is already gone.
    pub(crate) fn v5_chunk_target(
        &mut self,
        reservation: StreamChunkReservation,
        read: usize,
    ) -> Result<&mut [u8]> {
        let stream = self
            .active_streams
            .get_mut(&reservation.stream_id)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "stream reaped while its chunk reservation was still live",
                ))
            })?;
        if read > reservation.len {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "V5 stream read exceeds reserved chunk",
            )));
        }
        let start = reservation.offset + read;
        let end = reservation.offset + reservation.len;
        Ok(&mut stream.buffer.as_mut_slice()[start..end])
    }

    /// Records that `new_bytes` actually landed in `reservation`'s target
    /// range, keeping the owning stream out of the idle reaper AND crediting
    /// it toward the minimum-sustained-rate window -- even though the chunk
    /// this reservation belongs to has not committed yet. Must only be
    /// called after a read that returned at least one byte, with exactly
    /// the number of bytes that read returned: crediting progress for a
    /// poll that transferred nothing is exactly what let a stalled peer's
    /// live reservation pin a slot indefinitely, and crediting more than
    /// was actually read would let a peer buy rate-window credit it never
    /// paid bandwidth for.
    pub(crate) fn record_v5_chunk_progress(
        &mut self,
        reservation: StreamChunkReservation,
        new_bytes: usize,
    ) {
        if let Some(stream) = self.active_streams.get_mut(&reservation.stream_id) {
            stream.last_activity = std::time::Instant::now();
            stream.total_bytes_progressed = stream.total_bytes_progressed.saturating_add(new_bytes);
        }
    }

    pub(crate) fn commit_v5_chunk(
        &mut self,
        reservation: StreamChunkReservation,
    ) -> Result<Option<CompletedV5Stream>> {
        let completed = {
            let stream = self
                .active_streams
                .get_mut(&reservation.stream_id)
                .ok_or_else(|| {
                    GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "stream vanished",
                    ))
                })?;
            let word = reservation.chunk_index / 64;
            let bit = 1u64 << (reservation.chunk_index % 64);
            if stream.received_chunks[word] & bit != 0 {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "duplicate V5 stream chunk commit",
                )));
            }
            stream.received_chunks[word] |= bit;
            stream.received_size += reservation.len;
            stream.last_activity = std::time::Instant::now();
            stream.all_chunks_received()
        };
        if !completed {
            return Ok(None);
        }
        let stream = self
            .active_streams
            .remove(&reservation.stream_id)
            .expect("completed stream exists");
        Ok(Some(CompletedV5Stream {
            actor_id: stream.actor_id,
            type_hash: stream.type_hash,
            correlation_id: stream.correlation_id,
            is_response: stream.is_response,
            payload: stream.buffer.into_aligned_bytes(),
        }))
    }

    pub fn add_chunk_with_correlation(
        &mut self,
        header: crate::StreamHeader,
        chunk_data: Bytes,
        _schema_hash: Option<u64>,
    ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
        // If stream doesn't exist, we might have missed the start frame or it was cleaned up
        if !self.active_streams.contains_key(&header.stream_id) {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk for unknown stream_id={}", header.stream_id),
            )));
        }

        let stream = self
            .active_streams
            .get_mut(&header.stream_id)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Received chunk for unknown stream_id={}", header.stream_id),
                ))
            })?;

        if header.total_size != 0 && header.total_size != stream.total_size {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stream total_size mismatch",
            )));
        }

        if stream.received_size + chunk_data.len() > stream.total_size as usize {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk overflow for stream_id={}", header.stream_id),
            )));
        }

        if header.chunk_size as usize != chunk_data.len() {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk_size does not match chunk_data length",
            )));
        }

        if stream.chunk_stride.is_none() && header.chunk_size > 0 {
            let stride = header.chunk_size as usize;
            stream.chunk_stride = Some(stride);
            // The stride fixes how many chunks this stream consists of, which
            // in turn sizes the received-chunk bitmap.
            let expected = (stream.total_size as usize).div_ceil(stride);
            stream.expected_chunks = Some(expected);
            stream.received_chunks = vec![0u64; expected.div_ceil(64)];
        }

        let stride = stream.chunk_stride.unwrap_or(header.chunk_size as usize);
        if header.chunk_size as usize > stride {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk_size exceeds stream stride",
            )));
        }

        let chunk_index = header.chunk_index as usize;
        let offset = chunk_index.checked_mul(stride).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("chunk offset overflow for stream_id={}", header.stream_id),
            ))
        })?;
        let end = offset + chunk_data.len();
        if end > stream.total_size as usize {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received chunk overflow for stream_id={}", header.stream_id),
            )));
        }

        if let Some(expected) = stream.expected_chunks {
            if chunk_index >= expected {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "chunk_index={} is past the {} chunks declared by stream_id={}",
                        chunk_index, expected, header.stream_id
                    ),
                )));
            }

            // Senders chunk with a fixed stride, so every chunk but the last is
            // exactly one stride and the last is the remainder. Pinning this
            // keeps the stride (and therefore the chunk count the completion
            // check relies on) consistent with what actually arrives.
            let want = if chunk_index + 1 == expected {
                stream.total_size as usize - offset
            } else {
                stride
            };
            if chunk_data.len() != want {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "chunk_index={} of stream_id={} has {} bytes, expected {}",
                        chunk_index,
                        header.stream_id,
                        chunk_data.len(),
                        want
                    ),
                )));
            }
        }

        // A repeated chunk index must not advance `received_size`: completion is
        // decided per chunk, and double-counting bytes is exactly what let a
        // retransmit stand in for a chunk that never arrived.
        if !stream.mark_chunk_received(chunk_index) {
            warn!(
                stream_id = header.stream_id,
                chunk_index = chunk_index,
                duplicates = stream.duplicate_chunks,
                "Ignoring duplicate stream chunk"
            );
            return Ok(None);
        }

        // CRITICAL_PATH: write chunk directly into final buffer.
        stream.buffer.as_mut_slice()[offset..end].copy_from_slice(&chunk_data);
        stream.received_size += chunk_data.len();
        // Progress, so the idle reaper must not treat this stream as stalled,
        // and so the minimum-sustained-rate window credits it (this path
        // delivers a whole chunk at once rather than incrementally, so
        // unlike the V5 direct-read path there is no separate partial-read
        // credit to double-count against).
        stream.last_activity = std::time::Instant::now();
        stream.total_bytes_progressed = stream
            .total_bytes_progressed
            .saturating_add(chunk_data.len());

        if stream.all_chunks_received() {
            self.assemble_complete_message_with_correlation(header.stream_id)
        } else {
            Ok(None)
        }
    }

    pub fn metadata_for(&self, stream_id: u64) -> Option<(u64, u32, u32, bool)> {
        self.active_streams.get(&stream_id).map(|stream| {
            (
                stream.actor_id,
                stream.type_hash,
                stream.correlation_id,
                stream.is_response,
            )
        })
    }

    /// Drop a partial stream on the V5 cold-path abort signal. Its final
    /// allocation is returned to the pool and no partial data is delivered.
    pub fn abort_stream(&mut self, stream_id: u64) -> bool {
        self.active_streams.remove(&stream_id).is_some()
    }

    pub fn finalize_stream_with_correlation(
        &mut self,
        stream_id: u64,
        schema_hash: Option<u64>,
    ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
        // StreamEnd received - assemble the message
        if let Some(stream) = self.active_streams.get(&stream_id) {
            if stream.schema_hash != schema_hash {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "stream schema hash mismatch",
                )));
            }
        }
        self.assemble_complete_message_with_correlation(stream_id)
    }

    fn assemble_complete_message_with_correlation(
        &mut self,
        stream_id: u64,
    ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
        let stream = self.active_streams.remove(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Cannot finalize unknown stream_id={}", stream_id),
            ))
        })?;

        // ACTOR_REM_2 R9: the buffer is pre-allocated (zero-filled) to the
        // declared `total_size`. A StreamEnd that arrives before every byte was
        // received must NOT deliver that partially-filled buffer as if it were a
        // complete message — a malicious peer (StreamStart(N) then an immediate
        // StreamEnd) or a sender that aborts mid-transfer would otherwise inject
        // zero-padded / truncated payloads. Reject the incomplete assembly.
        if (stream.received_size as u64) < stream.total_size {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stream_id={} finalized incomplete: {} of {} bytes received",
                    stream_id, stream.received_size, stream.total_size
                ),
            )));
        }

        // The byte count alone cannot prove the buffer is fully written -- that
        // was the gap a duplicated chunk exploited. Require every chunk index.
        if !stream.all_chunks_received() {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stream_id={} finalized with missing chunks ({} duplicates seen)",
                    stream_id, stream.duplicate_chunks
                ),
            )));
        }

        let correlation_id = stream.correlation_id;
        let schema_hash = stream.schema_hash;
        let complete_data = stream.buffer.into_aligned_bytes().into_bytes();

        info!(
            "✅ STREAMING: Assembled complete message for stream_id={} ({} bytes for actor={}, type_hash=0x{:x}, correlation_id={})",
            stream.stream_id,
            complete_data.len(),
            stream.actor_id,
            stream.type_hash,
            correlation_id
        );

        Ok(Some((complete_data, correlation_id, schema_hash)))
    }

    /// Number of streams currently in progress.
    pub fn active_stream_count(&self) -> usize {
        self.active_streams.len()
    }

    /// Number of tombstoned (rejected or reaped) stream ids currently
    /// quarantined. Exposed for tests (including `connection_pool`'s, which
    /// drive `read_message_step` directly) that verify the table stays
    /// bounded or that a tombstone is cleaned up.
    #[cfg(test)]
    pub(crate) fn rejected_stream_count(&self) -> usize {
        self.rejected_streams.len()
    }

    /// Clean up in-progress streams that have stopped making genuine byte
    /// progress, or that are not sustaining a minimum transfer rate. A
    /// stream is never reaped merely for its total age: as long as it keeps
    /// clearing both checks below, it survives no matter how long the
    /// transfer takes, so a large or slow-but-healthy transfer is never
    /// evicted mid-flight. Two independent bounds are enforced, because
    /// neither alone is sufficient:
    ///
    /// - Zero progress for `STREAM_IDLE_TIMEOUT`: catches a peer that stops
    ///   advancing bytes entirely, including one that never sends any after
    ///   `StreamStart`.
    /// - Fewer than `MIN_SUSTAINED_BYTES_PER_WINDOW` bytes across a
    ///   `MIN_SUSTAINED_RATE_WINDOW`-wide tumbling window: catches a peer
    ///   that keeps sending just enough to dodge the idle check (e.g. one
    ///   byte every `STREAM_IDLE_TIMEOUT` minus a second) without ever
    ///   sustaining a real transfer rate. Without this, "any progress
    ///   resets the idle clock" lets that peer pin a slot and its share of
    ///   `MAX_INFLIGHT_STREAM_BYTES` for free, forever.
    ///
    /// Returns `false` if the aggregate tombstone budget was exhausted while
    /// trying to quarantine a reaped stream -- see `cleanup_stale_with`.
    /// Production callers (`stream_writer.rs::io_task`) must treat that as
    /// connection-fatal.
    #[must_use]
    pub fn cleanup_stale(&mut self) -> bool {
        self.cleanup_stale_with(
            STREAM_IDLE_TIMEOUT,
            MIN_SUSTAINED_RATE_WINDOW,
            MIN_SUSTAINED_BYTES_PER_WINDOW,
        )
    }

    /// `cleanup_stale` with explicit bounds, so tests do not have to wait
    /// out the production timeouts.
    ///
    /// Does **not** prune `rejected_streams` by wall-clock age, and never
    /// evicts an entry to make room for another: see
    /// `RejectedStreamTombstone`'s invariant for why a tombstone is removed
    /// only once proven done. Returns `false` if
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET` refused to tombstone one or
    /// more of the streams reaped by this call -- their trailing chunks
    /// would otherwise hit the fatal "unknown stream_id" path with no
    /// tombstone to catch them, so the caller must treat `false` as
    /// connection-fatal rather than silently losing track of those streams.
    #[must_use]
    pub(crate) fn cleanup_stale_with(
        &mut self,
        idle_timeout: std::time::Duration,
        rate_window: std::time::Duration,
        min_bytes_per_window: usize,
    ) -> bool {
        let before_count = self.active_streams.len();

        // A stream the reaper drops is not necessarily done as far as the
        // sender is concerned -- its next in-flight chunk is already on the
        // wire. Collect enough of each reaped stream's chunk-completion
        // state to tombstone it into `rejected_streams` below: without
        // that, the late chunk hits "unknown stream_id", a fatal protocol
        // error that tears down the whole connection instead of just this
        // stream.
        let mut reaped = Vec::new();
        self.active_streams.retain(|stream_id, stream| {
            let idle = stream.last_activity.elapsed();
            if idle > idle_timeout {
                warn!(
                    stream_id = stream_id,
                    idle_secs = idle.as_secs(),
                    age_secs = stream.started_at.elapsed().as_secs(),
                    received_size = stream.received_size,
                    expected_size = stream.total_size,
                    "Cleaning up stream with no byte progress within the idle timeout"
                );
                reaped.push((
                    *stream_id,
                    stream.total_size,
                    stream.chunk_stride,
                    stream.expected_chunks,
                    stream.received_chunks.clone(),
                ));
                return false;
            }

            // Tumbling minimum-rate window: only evaluated once a full
            // window has elapsed since it last opened, so a stream still
            // inside its first window is judged by the idle check alone.
            //
            // Credited off `total_bytes_progressed`, not `received_size`:
            // the latter only advances on a full chunk commit, so a single
            // large chunk that is genuinely, steadily progressing would
            // look like zero progress -- and get reaped for insufficient
            // rate -- for as long as it takes to fill. `total_bytes_progressed`
            // advances on every real byte read, committed or not, so it
            // agrees with `last_activity` about what counts as progress.
            let window_elapsed = stream.rate_window_started_at.elapsed();
            if window_elapsed >= rate_window {
                let bytes_this_window = stream
                    .total_bytes_progressed
                    .saturating_sub(stream.progressed_bytes_at_window_start);
                if bytes_this_window < min_bytes_per_window {
                    warn!(
                        stream_id = stream_id,
                        bytes_this_window,
                        min_bytes_per_window,
                        window_secs = window_elapsed.as_secs(),
                        received_size = stream.received_size,
                        expected_size = stream.total_size,
                        "Cleaning up stream that is not sustaining the minimum transfer rate"
                    );
                    reaped.push((
                        *stream_id,
                        stream.total_size,
                        stream.chunk_stride,
                        stream.expected_chunks,
                        stream.received_chunks.clone(),
                    ));
                    return false;
                }
                // The window is cleared: slide it forward rather than
                // letting it grow unbounded, so the check keeps measuring a
                // *recent* rate rather than a lifetime average (a transfer
                // that starts slow and speeds up must not be penalized
                // forever for its opening window).
                stream.rate_window_started_at = std::time::Instant::now();
                stream.progressed_bytes_at_window_start = stream.total_bytes_progressed;
            }

            true
        });

        let mut budget_exhausted = false;
        for (stream_id, total_size, chunk_stride, expected_chunks, received_chunks) in reaped {
            if !self.tombstone_reaped_stream(
                stream_id,
                total_size,
                chunk_stride,
                expected_chunks,
                received_chunks,
            ) {
                budget_exhausted = true;
            }
        }

        let removed = before_count - self.active_streams.len();
        if removed > 0 {
            info!(
                removed_count = removed,
                remaining = self.active_streams.len(),
                "Cleaned up stale in-progress streams"
            );
        }

        !budget_exhausted
    }

    /// Quarantines a reaped stream id the same way `reject_stream`
    /// quarantines a resource-pressure rejection, carrying over the exact
    /// chunk-completion state `stream` had already validated while active
    /// (see `RejectedStreamTombstone::reaped`) instead of starting blind.
    /// If that state already shows the generation complete -- every chunk
    /// it declared was already committed before it was reaped -- there is
    /// nothing left a compliant sender could still send, so no tombstone is
    /// needed at all. Returns `false` if either `REJECTED_STREAMS_BITMAP_WORD_BUDGET`
    /// or `REJECTED_STREAMS_ENTRY_BUDGET` refused the (incomplete) tombstone;
    /// see `try_insert_tombstone`.
    ///
    /// **`chunk_stride.is_none()` means this stream was reaped before its
    /// stride was ever established, and is never tombstoned at all** -- see
    /// the reasoning inline below. This is the only shape that reaches this
    /// function with an empty `received_chunks` (a V5 stream always
    /// establishes its stride at creation, from the mandatory inline first
    /// chunk on its `StreamStart` -- see `begin_v5_stream`/`reserve_v5_chunk`
    /// -- so only a *legacy* stream, whose `StreamStart` can legitimately
    /// arrive with no inline chunk at all, can still have `chunk_stride ==
    /// None` by the time it is reaped).
    fn tombstone_reaped_stream(
        &mut self,
        stream_id: u64,
        total_size: u64,
        chunk_stride: Option<usize>,
        expected_chunks: Option<usize>,
        received_chunks: Vec<u64>,
    ) -> bool {
        if chunk_stride.is_none() {
            // A tombstone for this generation cannot serve the purpose
            // tombstones exist for: with no stride, `is_complete` can never
            // observe completion (there is no known chunk count to check
            // against), and -- more fundamentally -- nothing removes it.
            // Legacy chunk delivery gates on `active_streams` alone
            // (`metadata_for`, checked by the dispatcher before
            // `add_chunk_with_correlation` is ever called), never on
            // `rejected_streams`; and legacy's own `StreamStart` entry point
            // (`start_stream_with_correlation_and_kind`) never calls
            // `remove_tombstone` the way `begin_v5_stream_or_discard` does
            // for V5. Inserting one anyway would create an entry that costs
            // *zero* bitmap words -- invisible to
            // `REJECTED_STREAMS_BITMAP_WORD_BUDGET` -- and that nothing in
            // this table can ever reclaim: exactly the zero-cost,
            // never-removed hole this function exists to close, not one to
            // reopen with a budget check.
            //
            // Consequence for a late chunk of the reaped generation: a
            // legacy `StreamData` frame for an id no longer in
            // `active_streams` is already dropped as a harmless, warned
            // no-op by the dispatcher's `metadata_for` check, independent of
            // whether a tombstone exists -- so nothing changes for the case
            // tombstones actually protect. What does change: a chunk
            // referencing this exact id through the *V5* path
            // (`reserve_v5_chunk_or_discard`) after this point is no longer
            // silently absorbed by an inert tombstone and instead hits the
            // ordinary "unknown stream_id" fatal error -- the same fallback
            // a truly unrecognized id already gets, and the correct one for
            // an id collision across protocol variants rather than a
            // legitimate late chunk of an already-reaped generation.
            //
            // `remove_tombstone` clears any stale tombstone a *different*
            // earlier generation of this same id might have left behind
            // (legacy's `StreamStart` not clearing on start, per above, cuts
            // both ways); it is a no-op when there is nothing to remove.
            self.remove_tombstone(stream_id);
            return true;
        }
        let tombstone = RejectedStreamTombstone::reaped(
            total_size,
            chunk_stride,
            expected_chunks,
            received_chunks,
        );
        if tombstone.is_complete() {
            self.remove_tombstone(stream_id);
            return true;
        }
        self.try_insert_tombstone(stream_id, tombstone)
    }
}

impl Default for StreamingState {
    fn default() -> Self {
        Self::new()
    }
}

fn is_resource_busy(error: &GossipError) -> bool {
    matches!(error, GossipError::Network(io) if io.kind() == std::io::ErrorKind::ResourceBusy)
}

/// True if `error` is `v5_chunk_target`'s "reservation vanished" signal --
/// the owning stream was reaped while a chunk read into it was still in
/// flight. The caller must drain and discard the remainder of that one
/// chunk instead of treating this as a fatal protocol error, mirroring how
/// a resource-pressure rejection is drained rather than tearing down the
/// connection (see `DiscardStreamPayload` in `connection_pool::read_pipeline`).
pub(crate) fn is_stream_reaped_error(error: &GossipError) -> bool {
    matches!(error, GossipError::Network(io) if io.kind() == std::io::ErrorKind::NotFound)
}

fn registry_message_sender_peer_id(msg: &RegistryMessage) -> Option<&PeerId> {
    match msg {
        RegistryMessage::DeltaGossip { delta, .. }
        | RegistryMessage::DeltaGossipResponse { delta, .. } => Some(&delta.sender_peer_id),
        RegistryMessage::FullSyncRequest { sender_peer_id, .. }
        | RegistryMessage::FullSync { sender_peer_id, .. }
        | RegistryMessage::FullSyncResponse { sender_peer_id, .. } => Some(sender_peer_id),
        RegistryMessage::PeerListGossip { .. } => None,
    }
}

/// Process a single read result result using the shared protocol logic.
///
/// This handles:
/// - Gossip messages -> registry.handle_incoming_message
/// - Raw Asks -> handle_raw_ask_request
/// - Responses -> handle_response_message
/// - Actor messages -> registry.actor_message_handler
/// - Streaming messages -> state.streaming (assembly) -> handler
#[cfg(test)]
pub(crate) async fn process_read_result(
    result: MessageReadResult,
    streaming_state: &mut StreamingState,
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    session_source: SocketAddr,
    response_correlation: Option<&crate::connection_pool::CorrelationTracker>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    authenticated_peer_id: Option<&PeerId>,
) -> Result<()> {
    process_read_result_with_instance(
        result,
        streaming_state,
        registry,
        peer_addr,
        session_source,
        None,
        response_correlation,
        response_connection,
        authenticated_peer_id,
    )
    .await
}

/// Process a frame with the exact stream instance when the caller owns the
/// stream task. This identity is carried only through the transport call
/// path; synthetic/test callers use the legacy wrapper above.
pub(crate) async fn process_read_result_with_instance(
    result: MessageReadResult,
    streaming_state: &mut StreamingState,
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    // R-11: this connection's own session discriminator -- see
    // `ReadContext::session_source`. Threaded to
    // `handle_incoming_message` so the restart-sequence exemption is
    // scoped to the exact connection that armed it.
    session_source: SocketAddr,
    connection_instance_id: Option<u64>,
    response_correlation: Option<&crate::connection_pool::CorrelationTracker>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    authenticated_peer_id: Option<&PeerId>,
) -> Result<()> {
    match result {
        MessageReadResult::RouteBound => {}
        MessageReadResult::Gossip(msg, _correlation_id) => {
            let authenticated_peer_id = authenticated_peer_id
                .or_else(|| response_connection.and_then(|conn| conn.embedded_peer_id.as_ref()));
            // Fail-closed: any gossip frame that carries a claimed sender
            // identity must be attributable to this connection's authenticated
            // identity. Drop if it mismatches OR if there is no authenticated
            // identity to verify against — mirroring the fail-closed PubSub path
            // below. On the live mutually-authenticated TLS path
            // `authenticated_peer_id` is always present, so this only closes
            // non-TLS / pre-identification paths where a forged `sender_peer_id`
            // would otherwise be accepted unchecked.
            if let Some(claimed) = registry_message_sender_peer_id(&msg) {
                match authenticated_peer_id {
                    Some(authenticated) if authenticated == claimed => {}
                    Some(authenticated) => {
                        warn!(
                            peer = %peer_addr,
                            authenticated_peer_id = %authenticated,
                            claimed_peer_id = %claimed,
                            "Dropping gossip message with mismatched authenticated peer identity"
                        );
                        return Ok(());
                    }
                    None => {
                        warn!(
                            peer = %peer_addr,
                            claimed_peer_id = %claimed,
                            "Dropping gossip message with a claimed sender but no authenticated peer identity"
                        );
                        return Ok(());
                    }
                }
            }

            if let Err(e) = crate::connection_pool::handle_incoming_message_with_instance(
                registry.clone(),
                peer_addr,
                session_source,
                connection_instance_id,
                authenticated_peer_id.cloned(),
                msg,
            )
            .await
            {
                warn!(error = %e, "Failed to process gossip message");
            }
        }
        MessageReadResult::AskRaw {
            correlation_id,
            payload,
        } => {
            handle_raw_ask_request(registry, peer_addr, correlation_id, &payload).await;
        }
        MessageReadResult::Response {
            correlation_id,
            payload,
        } => {
            handle_response_message(
                registry,
                peer_addr,
                correlation_id,
                payload,
                response_correlation,
            )
            .await;
        }
        MessageReadResult::AskNack {
            correlation_id,
            reason,
        } => {
            crate::handle::handle_response_nack_message(
                registry,
                peer_addr,
                correlation_id,
                reason,
                response_correlation,
            )
            .await;
        }
        MessageReadResult::PubSub { payload } => {
            let authenticated_peer_id = authenticated_peer_id
                .or_else(|| response_connection.and_then(|conn| conn.embedded_peer_id.as_ref()));
            if let Some(authenticated_peer_id) = authenticated_peer_id {
                if let Some(handler) = registry.pubsub_ingress_handler.load().as_ref() {
                    if let Err(e) = handler.handle(authenticated_peer_id, payload) {
                        warn!(peer = %peer_addr, error = %e, "Failed to process PubSub frame");
                    }
                }
            } else {
                warn!(peer = %peer_addr, "Dropping PubSub frame without authenticated peer identity");
            }
        }
        MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            request_id,
            schema_hash,
            payload,
        } => {
            // Handle actor message directly
            let corr_id = if msg_type == crate::MessageType::ActorAsk as u8 {
                correlation_id
            } else {
                0
            };
            let response_mode = if msg_type == crate::MessageType::ActorAsk as u8 {
                ResponseMode::AutoStream
            } else {
                ResponseMode::InlineOnly
            };
            handle_assembled_message(
                registry,
                peer_addr,
                authenticated_peer_id.or_else(|| {
                    response_connection.and_then(|conn| conn.embedded_peer_id.as_ref())
                }),
                actor_id,
                type_hash,
                payload,
                corr_id,
                request_id,
                schema_hash,
                response_connection,
                response_mode,
            )
            .await;
        }
        MessageReadResult::Streaming {
            msg_type,
            correlation_id,
            schema_hash,
            stream_header,
            chunk_data,
        } => {
            // Handle streaming messages
            match msg_type {
                msg_type
                    if msg_type == crate::MessageType::StreamStart as u8
                        || msg_type == crate::MessageType::StreamResponseStart as u8 =>
                {
                    let pool = registry.connection_pool.aligned_bytes_pool();
                    let is_response = msg_type == crate::MessageType::StreamResponseStart as u8;
                    if let Err(e) = streaming_state.start_stream_with_correlation_and_kind(
                        stream_header,
                        correlation_id,
                        pool,
                        schema_hash,
                        is_response,
                    ) {
                        warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                        return Ok(());
                    }
                    if chunk_data.is_empty() {
                        return Ok(());
                    }
                    let metadata = streaming_state.metadata_for(stream_header.stream_id);
                    if let Ok(Some((complete_data, corr_id, schema_hash))) = streaming_state
                        .add_chunk_with_correlation(stream_header, chunk_data, schema_hash)
                    {
                        if metadata
                            .map(|(_, _, _, response)| response)
                            .unwrap_or(is_response)
                        {
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                response_correlation,
                            )
                            .await;
                        } else if let Some((actor_id, type_hash, _, _)) = metadata {
                            handle_assembled_message(
                                registry,
                                peer_addr,
                                authenticated_peer_id.or_else(|| {
                                    response_connection
                                        .and_then(|conn| conn.embedded_peer_id.as_ref())
                                }),
                                actor_id,
                                type_hash,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                corr_id,
                                None,
                                schema_hash,
                                response_connection,
                                ResponseMode::AutoStream,
                            )
                            .await;
                        }
                    }
                }
                msg_type
                    if msg_type == crate::MessageType::StreamData as u8
                        || msg_type == crate::MessageType::StreamResponseData as u8 =>
                {
                    let Some((actor_id, type_hash, _, is_response)) =
                        streaming_state.metadata_for(stream_header.stream_id)
                    else {
                        warn!(
                            stream_id = stream_header.stream_id,
                            "Dropping stream chunk without start"
                        );
                        return Ok(());
                    };
                    if let Ok(Some((complete_data, corr_id, schema_hash))) = streaming_state
                        .add_chunk_with_correlation(stream_header, chunk_data, schema_hash)
                    {
                        if is_response {
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                response_correlation,
                            )
                            .await;
                        } else {
                            handle_assembled_message(
                                registry,
                                peer_addr,
                                authenticated_peer_id.or_else(|| {
                                    response_connection
                                        .and_then(|conn| conn.embedded_peer_id.as_ref())
                                }),
                                actor_id,
                                type_hash,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                corr_id,
                                None,
                                schema_hash,
                                response_connection,
                                ResponseMode::AutoStream,
                            )
                            .await;
                        }
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamEnd as u8 => {
                    // StreamEnd indicates the end of an incoming REQUEST (streaming tell/ask)
                    if let Ok(Some((complete_data, corr_id, schema_hash))) = streaming_state
                        .finalize_stream_with_correlation(stream_header.stream_id, schema_hash)
                    {
                        handle_assembled_message(
                            registry,
                            peer_addr,
                            authenticated_peer_id.or_else(|| {
                                response_connection.and_then(|conn| conn.embedded_peer_id.as_ref())
                            }),
                            stream_header.actor_id,
                            stream_header.type_hash,
                            crate::AlignedBytes::from_bytes(complete_data)
                                .expect("stream buffer must be aligned"),
                            corr_id,
                            None,
                            schema_hash,
                            response_connection,
                            ResponseMode::AutoStream,
                        )
                        .await;
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamResponseEnd as u8 => {
                    // StreamResponseEnd indicates the end of an incoming RESPONSE (from a remote ask)
                    if let Ok(Some((complete_data, corr_id, _schema_hash))) = streaming_state
                        .finalize_stream_with_correlation(stream_header.stream_id, schema_hash)
                    {
                        if corr_id != 0 {
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                crate::AlignedBytes::from_bytes(complete_data)
                                    .expect("stream buffer must be aligned"),
                                response_correlation,
                            )
                            .await;
                        } else {
                            // Ignore streaming response with correlation_id=0
                        }
                    }
                }
                _ => {
                    warn!("Unknown streaming message type: 0x{:02x}", msg_type);
                }
            }
        }
        MessageReadResult::StreamAbort { stream_id, reason } => {
            if !streaming_state.abort_stream(stream_id) {
                warn!(
                    stream_id,
                    reason, "Ignoring V5 stream abort for unknown stream"
                );
            }
        }
        MessageReadResult::Raw(_payload) => {
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                if std::env::var("ICANACT_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
                    crate::test_helpers::record_raw_payload(_payload.clone());
                }
            }
        }
        MessageReadResult::DirectAsk {
            correlation_id,
            request_id,
            payload,
        } => {
            // Fast-path DirectAsk bypasses the handler and RegistryMessage
            // overhead entirely -- there is no registered application
            // handler for it in any build mode, so it must never fabricate
            // a response from the caller's own request bytes. This used to
            // echo the request back under cfg(any(test, feature =
            // "test-helpers", debug_assertions)) and only NACK in release,
            // so a debug binary and a release binary answered a DirectAsk
            // differently. Every build mode now NACKs identically.
            //
            // request_id isn't consumed yet -- no dispatcher exists for
            // DirectAsk to hand it to -- but it was already fail-closed
            // validated (nonzero) by the parser.
            let _ = (payload, request_id);
            send_ask_nack(
                registry,
                peer_addr,
                correlation_id,
                crate::framing::AskNackReason::NoDispatcher,
            )
            .await;
        }
        MessageReadResult::DirectResponse {
            correlation_id,
            payload,
        } => {
            // Fast-path DirectResponse
            // The payload is the raw response data (no length prefix)
            // The payload contains only the direct frame body.
            // Deliver to correlation tracker - zero-copy using the payload directly
            handle_response_message(
                registry,
                peer_addr,
                correlation_id,
                payload,
                response_correlation,
            )
            .await;
        }
    }

    Ok(())
}

enum ResponseMode {
    InlineOnly,
    AutoStream,
}

fn should_stream_response(
    registry: &Arc<GossipRegistry>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    response_len: usize,
    response_mode: ResponseMode,
) -> bool {
    // Absolute correctness bound: the reader rejects frames larger than max_message_size.
    // `msg_len` on the wire is ASK_RESPONSE_HEADER_LEN + payload_len (length prefix excluded).
    let inline_payload_limit = registry
        .config
        .max_message_size
        .saturating_sub(crate::framing::ASK_RESPONSE_HEADER_LEN);
    if response_len > inline_payload_limit {
        return true;
    }

    match response_mode {
        ResponseMode::InlineOnly => false,
        ResponseMode::AutoStream => {
            // Prefer streaming for large-ish payloads to avoid huge inline frames.
            let _ = response_connection; // threshold is currently a global constant
            let threshold = crate::connection_pool::STREAMING_THRESHOLD;
            response_len > threshold
        }
    }
}

async fn handle_assembled_message(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    authenticated_peer_id: Option<&PeerId>,
    actor_id: u64,
    type_hash: u32,
    complete_data: crate::AlignedBytes,
    corr_id: u32,
    request_id: Option<u64>,
    _schema_hash: Option<u64>,
    response_connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
    response_mode: ResponseMode,
) {
    // Complete message assembled - route to actor
    // corr_id == 0 means tell (fire-and-forget), non-zero means ask (expects response)
    let correlation_opt = if corr_id == 0 { None } else { Some(corr_id) };
    let response = if corr_id == 0 {
        if let Some(cell) = registry.actor_tell_handler_sync_context.load_full() {
            cell.handle(
                actor_id,
                type_hash,
                complete_data,
                crate::TellContext::new(authenticated_peer_id),
            )
            .map(|_| None)
        } else if let Some(cell) = registry.actor_tell_handler_sync.load_full() {
            cell.handle(actor_id, type_hash, complete_data)
                .map(|_| None)
        } else {
            registry
                .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                .await
        }
    } else if let Some(cell) = registry.actor_ask_immediate_handler_sync.load_full() {
        if cell.can_handle(actor_id, type_hash) {
            cell.handle(actor_id, type_hash, complete_data).and_then(
                |disposition| match disposition {
                    crate::registry::AskDisposition::Immediate(response) => Ok(Some(response)),
                    crate::registry::AskDisposition::ImmediateBytes(response) => {
                        Ok(Some(ActorResponse::Bytes(response)))
                    }
                    crate::registry::AskDisposition::ImmediateAligned(response) => {
                        Ok(Some(ActorResponse::Aligned(response)))
                    }
                    crate::registry::AskDisposition::ImmediatePooled {
                        payload,
                        prefix,
                        payload_len,
                    } => Ok(Some(ActorResponse::Pooled {
                        payload,
                        prefix,
                        payload_len,
                    })),
                    crate::registry::AskDisposition::Deferred => Ok(None),
                    crate::registry::AskDisposition::Nack(reason) => {
                        Err(GossipError::AskNacked(reason))
                    }
                },
            )
        } else if let Some(cell) = registry.actor_ask_handler_sync.load_full() {
            if let Some(stream_handle) =
                response_connection.and_then(|conn| conn.stream_handle.as_ref().cloned())
            {
                let context = crate::AskContext::from_stream_handle_with_request_id(
                    corr_id,
                    &stream_handle,
                    authenticated_peer_id,
                    request_id,
                );
                cell.handle(actor_id, type_hash, complete_data, context)
                    .and_then(|disposition| match disposition {
                        crate::registry::AskDisposition::Immediate(response) => Ok(Some(response)),
                        crate::registry::AskDisposition::ImmediateBytes(response) => {
                            Ok(Some(ActorResponse::Bytes(response)))
                        }
                        crate::registry::AskDisposition::ImmediateAligned(response) => {
                            Ok(Some(ActorResponse::Aligned(response)))
                        }
                        crate::registry::AskDisposition::ImmediatePooled {
                            payload,
                            prefix,
                            payload_len,
                        } => Ok(Some(ActorResponse::Pooled {
                            payload,
                            prefix,
                            payload_len,
                        })),
                        crate::registry::AskDisposition::Deferred => Ok(None),
                        crate::registry::AskDisposition::Nack(reason) => {
                            Err(GossipError::AskNacked(reason))
                        }
                    })
            } else {
                registry
                    .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                    .await
            }
        } else {
            registry
                .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                .await
        }
    } else if let Some(cell) = registry.actor_ask_handler_sync.load_full() {
        if let Some(stream_handle) =
            response_connection.and_then(|conn| conn.stream_handle.as_ref().cloned())
        {
            let context = crate::AskContext::from_stream_handle_with_request_id(
                corr_id,
                &stream_handle,
                authenticated_peer_id,
                request_id,
            );
            cell.handle(actor_id, type_hash, complete_data, context)
                .and_then(|disposition| match disposition {
                    crate::registry::AskDisposition::Immediate(response) => Ok(Some(response)),
                    crate::registry::AskDisposition::ImmediateBytes(response) => {
                        Ok(Some(ActorResponse::Bytes(response)))
                    }
                    crate::registry::AskDisposition::ImmediateAligned(response) => {
                        Ok(Some(ActorResponse::Aligned(response)))
                    }
                    crate::registry::AskDisposition::ImmediatePooled {
                        payload,
                        prefix,
                        payload_len,
                    } => Ok(Some(ActorResponse::Pooled {
                        payload,
                        prefix,
                        payload_len,
                    })),
                    crate::registry::AskDisposition::Deferred => Ok(None),
                    crate::registry::AskDisposition::Nack(reason) => {
                        Err(GossipError::AskNacked(reason))
                    }
                })
        } else {
            registry
                .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
                .await
        }
    } else {
        registry
            .handle_actor_message(actor_id, type_hash, complete_data, correlation_opt)
            .await
    };

    match response {
        Ok(Some(response)) => {
            // Only send response for asks (non-zero correlation_id)
            if corr_id != 0 {
                match response {
                    ActorResponse::Bytes(response) => {
                        if should_stream_response(
                            registry,
                            response_connection,
                            response.len(),
                            response_mode,
                        ) {
                            send_streaming_response(registry, peer_addr, corr_id, response).await;
                        } else {
                            send_inline_response(registry, peer_addr, corr_id, response).await;
                        }
                    }
                    ActorResponse::Aligned(response) => {
                        if should_stream_response(
                            registry,
                            response_connection,
                            response.len(),
                            response_mode,
                        ) {
                            let bytes = response.into_bytes();
                            send_streaming_response(registry, peer_addr, corr_id, bytes).await;
                        } else {
                            send_inline_response_aligned(registry, peer_addr, corr_id, response)
                                .await;
                        }
                    }
                    ActorResponse::Pooled {
                        payload,
                        prefix,
                        payload_len,
                    } => {
                        if should_stream_response(
                            registry,
                            response_connection,
                            payload_len,
                            response_mode,
                        ) {
                            // Fallback to copying for oversize pooled responses so the caller doesn't
                            // time out on a valid response.
                            let mut buf = bytes::BytesMut::with_capacity(payload_len);
                            if let Some(p) = prefix {
                                buf.extend_from_slice(&p); // ALLOW_COPY
                            }
                            let mut payload = payload;
                            while payload.has_remaining() {
                                let chunk = payload.chunk();
                                if chunk.is_empty() {
                                    break;
                                }
                                buf.extend_from_slice(chunk); // ALLOW_COPY
                                let len = chunk.len();
                                payload.advance(len);
                            }
                            let bytes = buf.freeze();

                            send_streaming_response(registry, peer_addr, corr_id, bytes).await;
                        } else {
                            send_pooled_response(
                                registry,
                                peer_addr,
                                corr_id,
                                payload,
                                prefix,
                                payload_len,
                            )
                            .await;
                        }
                    }
                }
            }
        }
        Ok(None) => {}
        Err(e) => {
            // Only NACK asks (non-zero correlation_id); a tell has no waiter.
            if corr_id != 0 {
                crate::handle::send_ask_nack(registry, peer_addr, corr_id, e.ask_nack_reason())
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    /// Item 2: a DirectAsk has no registered application handler in any
    /// build mode. Before this fix, test/test-helpers/debug builds echoed
    /// the request bytes back as a fabricated DirectResponse, so a debug
    /// binary and a release binary answered the same request differently.
    /// Now every build mode NACKs identically -- no fabricated reply, ever.
    #[tokio::test]
    async fn direct_ask_never_echoes_and_always_nacks() {
        let addr: SocketAddr = "127.0.0.1:19996".parse().unwrap();
        let config = crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing(
                "direct-ask-never-echoes-self",
            )),
            ..Default::default()
        };
        let registry = crate::registry::GossipRegistry::<()>::new(addr, config);

        let (io, mut peer_io) = tokio::io::duplex(256);
        let (stream_handle, _writer_task, _reader_task) =
            crate::connection_pool::LockFreeStreamHandle::new(
                io,
                addr,
                crate::connection_pool::ChannelId::Global,
                crate::connection_pool::BufferConfig::default(),
                None,
                None,
            );
        let mut conn = crate::connection_pool::LockFreeConnection::new(
            addr,
            crate::connection_pool::ConnectionDirection::Inbound,
        );
        conn.stream_handle = Some(Arc::new(stream_handle));
        conn.set_state(crate::connection_pool::ConnectionState::Connected);
        let peer_id = crate::KeyPair::new_for_testing("direct-ask-never-echoes-peer").peer_id();
        registry
            .connection_pool
            .add_connection_by_peer_id(peer_id, addr, Arc::new(conn));

        let registry = Arc::new(registry);
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let request_payload = b"do-not-echo-me-back";
        let mut streaming_state = StreamingState::new();
        process_read_result(
            MessageReadResult::DirectAsk {
                correlation_id: 4242,
                request_id: 99,
                payload: crate::AlignedBytes::from_pooled_slice(request_payload, pool),
            },
            &mut streaming_state,
            &registry,
            addr,
            addr,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let mut frame = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            peer_io.read_exact(&mut frame),
        )
        .await
        .expect("a NACK must be sent immediately, not left to a timeout")
        .expect("peer must receive the NACK frame");

        let control = crate::framing::decode_control(frame[..4].try_into().unwrap())
            .expect("valid control word");
        // Never a DirectResponse (which would carry the echoed bytes as its
        // payload): always the NACK-flagged Response frame.
        assert_eq!(control.kind, crate::framing::WireKind::Response);
        assert_eq!(u32::from_be_bytes(frame[4..8].try_into().unwrap()), 4242);
        assert_eq!(
            crate::framing::ask_nack_reason(&frame[4..]),
            Some(crate::framing::AskNackReason::NoDispatcher)
        );
    }

    #[test]
    fn streaming_rejects_oversize() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 1,
            total_size: (crate::MAX_STREAM_SIZE as u64) + 1,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };

        assert!(
            state
                .start_stream_with_correlation(header, 1, pool, None)
                .is_err()
        );
    }

    #[test]
    fn streaming_enforces_per_connection_inflight_budget() {
        // A peer must not be able to force unbounded eager allocation by opening
        // many max-size streams: the SUM of declared in-flight sizes is capped.
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let per = crate::MAX_STREAM_SIZE as u64;
        let allowed = crate::MAX_INFLIGHT_STREAM_BYTES / crate::MAX_STREAM_SIZE;

        for i in 0..allowed as u64 {
            let header = crate::StreamHeader {
                stream_id: i,
                total_size: per,
                chunk_size: 0,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .start_stream_with_correlation(header, 1, pool.clone(), None)
                .expect("streams within the budget are accepted");
        }

        // One more max-size stream would push the aggregate over the budget.
        let header = crate::StreamHeader {
            stream_id: 9999,
            total_size: per,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        assert!(
            state
                .start_stream_with_correlation(header, 1, pool, None)
                .is_err(),
            "aggregate in-flight stream allocation must be bounded per connection"
        );
    }

    #[test]
    fn streaming_reassembles_into_final_buffer() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let start = crate::StreamHeader {
            stream_id: 42,
            total_size: 8,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 7,
        };
        state
            .start_stream_with_correlation(start, 9, pool.clone(), None)
            .unwrap();

        let chunk0 = crate::StreamHeader {
            stream_id: 42,
            total_size: 8,
            chunk_size: 4,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 7,
        };
        let chunk1 = crate::StreamHeader {
            stream_id: 42,
            total_size: 8,
            chunk_size: 4,
            chunk_index: 1,
            type_hash: 1,
            actor_id: 7,
        };

        assert!(
            state
                .add_chunk_with_correlation(chunk0, Bytes::from_static(b"abcd"), None)
                .unwrap()
                .is_none()
        );
        let assembled = state
            .add_chunk_with_correlation(chunk1, Bytes::from_static(b"efgh"), None)
            .unwrap()
            .expect("assembled");
        assert_eq!(assembled.0.as_ref(), b"abcdefgh");
        assert_eq!(assembled.1, 9);
        assert_eq!(assembled.2, None);
    }

    #[test]
    fn finalize_incomplete_stream_is_rejected() {
        // ACTOR_REM_2 R9: a StreamEnd arriving before all declared bytes are
        // received must NOT deliver the pre-allocated, zero-padded buffer as a
        // complete message.
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let start = crate::StreamHeader {
            stream_id: 7,
            total_size: 8,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        state
            .start_stream_with_correlation(start, 3, pool.clone(), None)
            .unwrap();

        // Only 4 of the declared 8 bytes are delivered.
        let chunk0 = crate::StreamHeader {
            stream_id: 7,
            total_size: 8,
            chunk_size: 4,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        assert!(
            state
                .add_chunk_with_correlation(chunk0, Bytes::from_static(b"abcd"), None)
                .unwrap()
                .is_none(),
            "a 4-of-8 stream is not complete"
        );

        // StreamEnd (finalize) arrives early: must error, not deliver padding.
        assert!(
            state.finalize_stream_with_correlation(7, None).is_err(),
            "R9: finalizing an incomplete stream must be rejected"
        );
    }

    #[test]
    fn finalize_empty_stream_is_allowed() {
        // A legitimately zero-length payload (total_size == 0) is complete on
        // finalize and must not be rejected by the R9 completeness check.
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let start = crate::StreamHeader {
            stream_id: 8,
            total_size: 0,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        state
            .start_stream_with_correlation(start, 5, pool.clone(), None)
            .unwrap();
        let out = state
            .finalize_stream_with_correlation(8, None)
            .expect("empty stream finalizes")
            .expect("empty stream yields a message");
        assert_eq!(out.0.as_ref(), b"");
        assert_eq!(out.1, 5);
    }

    #[test]
    fn finalize_empty_stream_survives_exhausted_pool() {
        let pool = Arc::new(crate::AlignedBytesPool::new(2));
        let _checked_out: Vec<_> =
            std::iter::from_fn(|| (pool.available_count() > 0).then(|| pool.get_buffer(64)))
                .collect();
        assert_eq!(pool.available_count(), 0, "pool must be fully checked out");

        let mut state = StreamingState::new();
        let start = crate::StreamHeader {
            stream_id: 99,
            total_size: 0,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 1,
            actor_id: 2,
        };
        state
            .start_stream_with_correlation(start, 11, pool, None)
            .expect("zero-length stream must start with an exhausted pool");
        let out = state
            .finalize_stream_with_correlation(99, None)
            .expect("zero-length stream must finalize")
            .expect("empty stream yields a message");
        assert_eq!(out.0.as_ref(), b"");
        assert_eq!(out.1, 11);
    }

    /// Helpers for the chunk-integrity tests below.
    mod chunk_integrity {
        use super::*;

        pub(super) const STRIDE: usize = 8;

        pub(super) fn start(state: &mut StreamingState, stream_id: u64, total_size: u64) {
            let pool = Arc::new(crate::AlignedBytesPool::default());
            let header = crate::StreamHeader {
                stream_id,
                total_size,
                chunk_size: 0,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .start_stream_with_correlation(header, 7, pool, None)
                .expect("stream start is accepted");
        }

        pub(super) fn chunk(
            state: &mut StreamingState,
            stream_id: u64,
            total_size: u64,
            chunk_index: u32,
            payload: &[u8],
        ) -> Result<Option<(Bytes, u32, Option<u64>)>> {
            let header = crate::StreamHeader {
                stream_id,
                total_size,
                chunk_size: payload.len() as u32,
                chunk_index,
                type_hash: 0,
                actor_id: 0,
            };
            state.add_chunk_with_correlation(header, Bytes::copy_from_slice(payload), None)
        }
    }

    /// A duplicated chunk must not be able to stand in for a chunk that never
    /// arrived. Counting only received *bytes* let `received_size` reach
    /// `total_size` while a hole in the pre-allocated (zero-filled) buffer was
    /// never written, so a corrupt, zero-padded payload was delivered to the
    /// actor as a complete message.
    #[test]
    fn duplicate_chunk_never_substitutes_for_a_missing_chunk() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 2) as u64;
        start(&mut state, 1, total);

        let first = [0xAAu8; STRIDE];
        assert!(
            chunk(&mut state, 1, total, 0, &first)
                .expect("first chunk is accepted")
                .is_none(),
            "a single chunk of a two-chunk stream must not complete it"
        );

        // Chunk 0 again -- a retransmit. Chunk 1 never arrives.
        let completed = chunk(&mut state, 1, total, 0, &first);

        match completed {
            Err(_) => {}
            Ok(None) => {}
            Ok(Some((data, _, _))) => panic!(
                "stream completed on a duplicated chunk while chunk 1 was never \
                 received; delivered {} bytes with tail {:?}",
                data.len(),
                &data[STRIDE..]
            ),
        }
    }

    /// A duplicate that arrives alongside the full set must not corrupt the
    /// payload or the accounting: the assembled message is still byte-exact.
    #[test]
    fn duplicate_chunk_does_not_corrupt_a_complete_stream() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 2) as u64;
        start(&mut state, 2, total);

        let first = [0xAAu8; STRIDE];
        let second = [0xBBu8; STRIDE];

        assert!(chunk(&mut state, 2, total, 0, &first).unwrap().is_none());
        // Retransmit of chunk 0 before chunk 1 lands.
        let _ = chunk(&mut state, 2, total, 0, &first);
        let completed = chunk(&mut state, 2, total, 1, &second)
            .expect("final chunk is accepted")
            .expect("stream completes once every chunk has arrived");

        let mut expected = Vec::new();
        expected.extend_from_slice(&first);
        expected.extend_from_slice(&second);
        assert_eq!(
            completed.0.as_ref(),
            expected.as_slice(),
            "assembled payload must be byte-exact despite the duplicate"
        );
    }

    /// Chunks may legitimately arrive out of order; completion must still be
    /// exact and the payload correctly placed.
    #[test]
    fn out_of_order_chunks_still_assemble_byte_exactly() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 3) as u64;
        start(&mut state, 3, total);

        let c0 = [0x11u8; STRIDE];
        let c1 = [0x22u8; STRIDE];
        let c2 = [0x33u8; STRIDE];

        assert!(chunk(&mut state, 3, total, 2, &c2).unwrap().is_none());
        assert!(chunk(&mut state, 3, total, 0, &c0).unwrap().is_none());
        let completed = chunk(&mut state, 3, total, 1, &c1)
            .expect("final chunk is accepted")
            .expect("stream completes once every chunk has arrived");

        let mut expected = Vec::new();
        expected.extend_from_slice(&c0);
        expected.extend_from_slice(&c1);
        expected.extend_from_slice(&c2);
        assert_eq!(completed.0.as_ref(), expected.as_slice());
    }

    /// A chunk index past the declared end of the stream must be rejected
    /// rather than silently ignored or written out of range.
    #[test]
    fn chunk_index_beyond_declared_length_is_rejected() {
        use chunk_integrity::{STRIDE, chunk, start};

        let mut state = StreamingState::new();
        let total = (STRIDE * 2) as u64;
        start(&mut state, 4, total);

        let payload = [0xCCu8; STRIDE];
        assert!(
            chunk(&mut state, 4, total, 7, &payload).is_err(),
            "chunk_index 7 of a two-chunk stream must be rejected"
        );
    }

    /// A stream that keeps receiving chunks must not be reaped just because it
    /// has been running a long time. Reaping keyed on age-since-start killed
    /// legitimately slow or large transfers mid-flight, failing the sender's
    /// ask with no server-side diagnostic.
    #[test]
    fn actively_progressing_stream_survives_past_the_idle_timeout() {
        use chunk_integrity::{STRIDE, chunk, start};
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(40);
        // Wide enough that this test's ~60ms run never crosses it: this test
        // is about the idle check alone, not the rate floor.
        let rate_window = Duration::from_secs(3600);

        let mut state = StreamingState::new();
        let total = (STRIDE * 4) as u64;
        start(&mut state, 10, total);

        // Trickle chunks, each comfortably inside the idle window, but let the
        // stream's total age pass the idle timeout several times over.
        for idx in 0..4u32 {
            std::thread::sleep(Duration::from_millis(15));
            let payload = [0x5Au8; STRIDE];
            let _ = chunk(&mut state, 10, total, idx, &payload);
            let _ = state.cleanup_stale_with(idle_timeout, rate_window, 1);
            if idx < 3 {
                assert_eq!(
                    state.active_stream_count(),
                    1,
                    "a stream that received a chunk {}ms ago was reaped while still progressing",
                    15
                );
            }
        }
    }

    /// A stream that sustains at least the minimum required rate survives
    /// across many tumbling rate windows, no matter how many reaper ticks
    /// land while it is transferring. This is the positive side of the
    /// minimum-sustained-rate check: a slow-but-adequate transfer is never
    /// mistaken for the drip attack `stream_below_the_rate_floor_is_reaped_despite_dodging_the_idle_check`
    /// guards against below.
    #[test]
    fn stream_meeting_the_rate_floor_survives_across_many_windows() {
        use chunk_integrity::{STRIDE, chunk, start};
        use std::time::Duration;

        let idle_timeout = Duration::from_secs(3600); // not under test here
        let rate_window = Duration::from_millis(30);
        let min_bytes_per_window = 1; // any chunk at all clears it

        let mut state = StreamingState::new();
        // Declares far more chunks than the loop sends, so the stream never
        // completes mid-test.
        let total = (STRIDE * 1000) as u64;
        start(&mut state, 14, total);

        for idx in 0..12u32 {
            std::thread::sleep(Duration::from_millis(10));
            let payload = [0x5Au8; STRIDE];
            let _ = chunk(&mut state, 14, total, idx, &payload);
            let _ = state.cleanup_stale_with(idle_timeout, rate_window, min_bytes_per_window);
            assert_eq!(
                state.active_stream_count(),
                1,
                "a stream clearing the minimum rate every window must not be reaped"
            );
        }
    }

    /// The bound that keeps "any progress resets the clock" from being
    /// exploitable: a stream that sends real, nonzero progress on every
    /// single reaper tick -- so the zero-progress idle check alone never
    /// fires -- must still be reaped once it fails to sustain
    /// `min_bytes_per_window` across a full rate window. Without this
    /// check, a peer trickling one chunk just under the idle timeout could
    /// hold its slot and inflight-byte reservation forever for the cost of
    /// a few bytes a tick.
    ///
    /// This test genuinely fails if the rate-floor check is removed, or
    /// inverted to require only nonzero (rather than sufficient) progress
    /// per window: with either of those, this stream would never be
    /// reaped and the loop would exhaust its iterations still active.
    #[test]
    fn stream_below_the_rate_floor_is_reaped_despite_dodging_the_idle_check() {
        use chunk_integrity::{STRIDE, chunk, start};
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(50); // never crossed: a chunk lands every tick
        let rate_window = Duration::from_millis(30);
        // One STRIDE-sized chunk per tick delivers STRIDE bytes per window at
        // best; demanding far more than that per window makes the floor
        // unmeetable by this drip, regardless of exact tick timing.
        let min_bytes_per_window = STRIDE * 100;

        let mut state = StreamingState::new();
        let total = (STRIDE * 1000) as u64;
        start(&mut state, 15, total);

        let mut reaped = false;
        for idx in 0..40u32 {
            std::thread::sleep(Duration::from_millis(5));
            let payload = [0x5Au8; STRIDE];
            // The drip keeps making real, nonzero progress every tick, which
            // is enough to defeat a zero-progress-only idle check.
            let _ = chunk(&mut state, 15, total, idx, &payload);
            let _ = state.cleanup_stale_with(idle_timeout, rate_window, min_bytes_per_window);
            if state.active_stream_count() == 0 {
                reaped = true;
                break;
            }
        }
        assert!(
            reaped,
            "a stream making only trickle progress every tick must eventually be reaped \
             for failing to sustain the minimum transfer rate, even though it never goes \
             idle long enough to trip the zero-progress check alone"
        );
    }

    /// The reaper must still collect a stream that genuinely stalls, and
    /// release both its slot and its reserved inflight-byte budget.
    #[test]
    fn idle_stream_is_reaped_and_releases_its_slot_and_reserved_bytes() {
        use std::time::Duration;

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let per = crate::MAX_STREAM_SIZE as u64;
        let allowed = crate::MAX_INFLIGHT_STREAM_BYTES / crate::MAX_STREAM_SIZE;

        // Fully commit the per-connection inflight budget across the max
        // number of max-size streams it allows, none of which ever receive
        // a chunk.
        for i in 0..allowed as u64 {
            let header = crate::StreamHeader {
                stream_id: i,
                total_size: per,
                chunk_size: 0,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .start_stream_with_correlation(header, 1, pool.clone(), None)
                .expect("streams within the budget are accepted");
        }
        assert_eq!(state.active_stream_count(), allowed);

        // The budget is fully committed: one more max-size stream must be
        // rejected while the others are still active.
        let blocked = crate::StreamHeader {
            stream_id: 999,
            total_size: per,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        assert!(
            state
                .start_stream_with_correlation(blocked, 1, pool.clone(), None)
                .is_err(),
            "inflight budget must be fully committed by the existing streams"
        );

        std::thread::sleep(Duration::from_millis(30));
        let _ = state.cleanup_stale_with(Duration::from_millis(10), Duration::from_secs(3600), 1);

        assert_eq!(
            state.active_stream_count(),
            0,
            "streams with no activity past the idle timeout must be reaped"
        );

        // Every slot and its reserved bytes must be released: the full set
        // of max-size streams can be admitted again from scratch.
        for i in 1000..(1000 + allowed as u64) {
            let header = crate::StreamHeader {
                stream_id: i,
                total_size: per,
                chunk_size: 0,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .start_stream_with_correlation(header, 1, pool.clone(), None)
                .expect("reaping stalled streams must release their reserved inflight bytes");
        }
        assert_eq!(state.active_stream_count(), allowed);
    }

    /// The existing zero-length stream contract must keep working.
    #[test]
    fn empty_stream_finalizes_without_chunks() {
        use chunk_integrity::start;

        let mut state = StreamingState::new();
        start(&mut state, 5, 0);

        let completed = state
            .finalize_stream_with_correlation(5, None)
            .expect("empty stream finalizes")
            .expect("empty stream yields a message");
        assert_eq!(completed.0.len(), 0);
    }

    #[test]
    fn v5_direct_read_commits_the_final_allocation_without_a_chunk_copy() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(1));
        let start = crate::StreamHeader {
            stream_id: 77,
            total_size: 5,
            chunk_size: 3,
            chunk_index: 0,
            type_hash: 9,
            actor_id: 11,
        };
        let first = state
            .begin_v5_stream(start, 13, pool, false, 3)
            .expect("reserve start chunk");
        let first_ptr = {
            let target = state.v5_chunk_target(first, 0).expect("first target");
            target.copy_from_slice(b"abc");
            target.as_ptr()
        };
        assert!(state.commit_v5_chunk(first).unwrap().is_none());

        let second = state.reserve_v5_chunk(77, 1, 2).expect("reserve tail");
        {
            let target = state.v5_chunk_target(second, 0).expect("tail target");
            target.copy_from_slice(b"de");
        }
        let complete = state
            .commit_v5_chunk(second)
            .unwrap()
            .expect("complete stream");
        assert_eq!(complete.payload.as_ref(), b"abcde");
        assert_eq!(complete.payload.as_ref().as_ptr(), first_ptr);
        assert_eq!(
            (complete.payload.as_ref().as_ptr() as usize) % crate::PAYLOAD_ALIGNMENT,
            0
        );
    }

    #[test]
    fn v5_abort_discards_partial_assembly_without_delivery() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(1));
        let reservation = state
            .begin_v5_stream(
                crate::StreamHeader {
                    stream_id: 88,
                    total_size: 4,
                    chunk_size: 2,
                    chunk_index: 0,
                    type_hash: 1,
                    actor_id: 2,
                },
                3,
                pool,
                false,
                2,
            )
            .unwrap();
        state
            .v5_chunk_target(reservation, 0)
            .unwrap()
            .copy_from_slice(b"ab");
        assert!(state.abort_stream(88));
        assert!(!state.abort_stream(88));
        assert!(state.commit_v5_chunk(reservation).is_err());
    }

    #[test]
    fn qa_r8_resource_rejection_tombstones_trailing_chunks() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(1));

        for stream_id in 1..=16 {
            let reservation = state
                .begin_v5_stream(
                    crate::StreamHeader {
                        stream_id,
                        total_size: 1,
                        chunk_size: 1,
                        chunk_index: 0,
                        type_hash: 0,
                        actor_id: 0,
                    },
                    0,
                    pool.clone(),
                    false,
                    1,
                )
                .expect("fill the active-stream budget");
            let target = state.v5_chunk_target(reservation, 0).unwrap();
            target[0] = 1;
        }

        let rejected = state
            .begin_v5_stream_or_discard(
                crate::StreamHeader {
                    stream_id: 17,
                    total_size: 2,
                    chunk_size: 1,
                    chunk_index: 0,
                    type_hash: 0,
                    actor_id: 0,
                },
                0,
                pool,
                false,
                1,
            )
            .expect("resource pressure is a stream-local rejection");
        assert!(rejected.is_none());
        assert!(
            state
                .reserve_v5_chunk_or_discard(17, 1, 1)
                .expect("trailing chunk is discarded")
                .is_none()
        );
        assert_eq!(state.active_stream_count(), 16);

        // Once pressure clears, a later StreamStart is a new generation on
        // the ordered transport and must not be silently discarded because
        // its old generation was tombstoned.
        assert!(state.abort_stream(1));
        assert!(
            state
                .begin_v5_stream_or_discard(
                    crate::StreamHeader {
                        stream_id: 17,
                        total_size: 1,
                        chunk_size: 1,
                        chunk_index: 0,
                        type_hash: 0,
                        actor_id: 0,
                    },
                    0,
                    Arc::new(crate::AlignedBytesPool::new(1)),
                    false,
                    1,
                )
                .expect("a retry start is valid after pressure clears")
                .is_some()
        );
    }

    /// The reap-tombstone table has no capacity bound: an incomplete
    /// tombstone is never evicted merely to make room for another. Reaping
    /// many more streams than the old fixed 32-entry cap allowed must not
    /// remove any earlier, still-incomplete tombstone -- each is cleared
    /// only by its own completion or a retry.
    ///
    /// Drives `tombstone_reaped_stream` directly (the exact function
    /// `cleanup_stale_with` calls for every id it reaps) rather than forcing
    /// real streams through real idle timeouts, so this is exercised
    /// deterministically.
    #[test]
    fn rejected_stream_table_never_evicts_incomplete_tombstones_no_matter_how_many_are_reaped() {
        let mut state = StreamingState::new();
        const REAPED_COUNT: u64 = 96; // several multiples of the old 32-entry cap

        for stream_id in 0..REAPED_COUNT {
            // total_size=16, stride=8: chunk 0 already committed, chunk 1
            // still outstanding -- genuinely incomplete, so it must survive.
            state.tombstone_reaped_stream(stream_id, 16, Some(8), Some(2), vec![0b1]);
        }

        assert_eq!(
            state.rejected_stream_count(),
            REAPED_COUNT as usize,
            "no incomplete tombstone may be evicted merely because more streams were reaped afterward"
        );

        // The *first* reaped id -- the one an eviction-by-age/order policy
        // would sacrifice first -- must still be tombstoned: its own
        // trailing chunk (index 1) must be a clean discard, not a fatal
        // unknown-stream error.
        assert!(
            state
                .reserve_v5_chunk_or_discard(0, 1, 8)
                .expect("the first reaped id's trailing chunk must still be discarded cleanly")
                .is_none(),
            "the first reaped id must not have been evicted by the reaps that followed it"
        );
        // Completing its declared size removes the tombstone.
        assert_eq!(state.rejected_stream_count(), REAPED_COUNT as usize - 1);
    }

    /// Review finding (`protocol.rs:963`): evicting the oldest tombstone at
    /// a fixed capacity is not a correct answer to "which tombstone can be
    /// sacrificed" -- streaming frames interleave, so nothing bounds how
    /// many *other* streams can be rejected while one specific rejected
    /// generation still has trailing chunks in flight. Reproduces exactly:
    /// 16 active streams, then a multi-frame rejection, then 33 further
    /// distinct multi-frame rejections with no reaps or retries in between
    /// (more than the old 32-entry cap) -- the first rejection's own
    /// trailing chunk must still be a clean discard, not the fatal
    /// "unknown stream_id" a since-evicted tombstone would produce.
    #[test]
    fn reject_stream_never_fails_and_never_evicts_an_earlier_incomplete_tombstone() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());

        // Fill max_concurrent_streams (16) with real, tiny active streams so
        // every further begin_v5_stream_or_discard call hits the
        // resource-pressure path deterministically.
        for stream_id in 0..16u64 {
            let header = crate::StreamHeader {
                stream_id,
                total_size: 8,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8)
                .expect("connection has room for the first 16 streams")
                .expect("must be admitted, not discarded");
        }

        // The 17th stream is a multi-frame rejection (total_size=16,
        // first_chunk_len=8 leaves one more chunk outstanding) -- the first
        // rejection, and the one an LRU-by-touch policy would evict first.
        let first_rejected = crate::StreamHeader {
            stream_id: 17,
            total_size: 16,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        assert!(
            state
                .begin_v5_stream_or_discard(first_rejected, 1, pool.clone(), false, 8)
                .expect("resource pressure is a stream-local rejection")
                .is_none()
        );

        // 33 further, distinct, multi-frame rejections follow -- more than
        // the old 32-entry cap -- with no reaps or retries in between.
        // Under the old LRU-at-capacity policy this is exactly what evicted
        // the first tombstone.
        for stream_id in 1000..1033u64 {
            let header = crate::StreamHeader {
                stream_id,
                total_size: 16,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            let result = state.begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8);
            assert!(
                result.is_ok(),
                "resource-pressure rejection #{stream_id} must be a clean discard, not a fatal error: {result:?}"
            );
            assert!(
                result.unwrap().is_none(),
                "a resource-pressure-rejected stream must not be admitted"
            );
        }

        // The first rejected stream's own trailing chunk (index 1) must
        // still be a clean discard -- its tombstone must not have been
        // evicted by the 33 unrelated rejections that followed it.
        let trailing = state.reserve_v5_chunk_or_discard(17, 1, 8);
        assert!(
            trailing.is_ok(),
            "the first rejected stream's trailing chunk must not be a fatal unknown-stream error: {trailing:?}"
        );
        assert!(
            trailing.unwrap().is_none(),
            "the first rejected stream's trailing chunk must be discarded, not accepted as fresh"
        );
    }

    /// Correction to the completion-tracking redesign: removing capacity
    /// *eviction* (the LRU bug) does not mean the table should have no
    /// budget at all. `REJECTED_STREAMS_BITMAP_WORD_BUDGET` bounds its
    /// aggregate size; exhausting it must be a hard, connection-fatal error
    /// for the specific rejection that crosses it -- never a silent
    /// eviction of a different, still-needed tombstone, and never silent
    /// admission of a stream this connection has no room to track.
    #[test]
    fn tombstone_budget_exhaustion_closes_the_connection_without_evicting_an_existing_tombstone() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());

        for stream_id in 0..16u64 {
            let header = crate::StreamHeader {
                stream_id,
                total_size: 8,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8)
                .expect("connection has room for the first 16 streams")
                .expect("must be admitted, not discarded");
        }

        // A modest, legitimate multi-frame rejection -- this tombstone must
        // survive everything that follows.
        let first_rejected = crate::StreamHeader {
            stream_id: 17,
            total_size: 16,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        assert!(
            state
                .begin_v5_stream_or_discard(first_rejected, 1, pool.clone(), false, 8)
                .expect("resource pressure is a stream-local rejection")
                .is_none()
        );

        // A single further rejection at MAX_STREAM_SIZE with an 8-byte
        // stride -- an ordinary, legitimately-sized large multi-frame
        // request, not a malformed declaration -- needs a completion
        // bitmap exactly as large as the entire aggregate budget on its
        // own (64 MiB / 8-byte stride / 64 chunks-per-word ==
        // REJECTED_STREAMS_BITMAP_WORD_BUDGET words), so it alone, on top
        // of the first rejection's 1-word tombstone, crosses it.
        let budget_buster = crate::StreamHeader {
            stream_id: 18,
            total_size: crate::MAX_STREAM_SIZE as u64,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let result = state.begin_v5_stream_or_discard(budget_buster, 1, pool, false, 8);
        assert!(
            result.is_err(),
            "exhausting the aggregate tombstone budget must be a hard, connection-fatal error, \
             not a silent discard with no tombstone to catch its trailing chunks: {result:?}"
        );

        // The *first* rejection's tombstone must still be intact -- budget
        // exhaustion must never silently evict an existing, unrelated
        // tombstone to make room for the one that was refused.
        let trailing = state.reserve_v5_chunk_or_discard(17, 1, 8);
        assert!(
            matches!(trailing, Ok(None)),
            "budget exhaustion must never evict an existing tombstone: {trailing:?}"
        );
    }

    /// Review finding: a legacy stream reaped before its stride was ever
    /// established -- a header-only `StreamStart` (`chunk_size == 0`, the
    /// dispatcher's `chunk_data.is_empty()` short-circuit) with no
    /// `StreamData` chunk following before the idle reaper catches it -- used
    /// to be tombstoned with an empty completion bitmap: zero bitmap words,
    /// entirely invisible to `REJECTED_STREAMS_BITMAP_WORD_BUDGET`. Worse,
    /// nothing removes it afterward: legacy chunk delivery gates on
    /// `active_streams` alone (`metadata_for`), never on `rejected_streams`,
    /// and legacy's own `StreamStart` entry point
    /// (`start_stream_with_correlation_and_kind`) never clears an existing
    /// tombstone the way `begin_v5_stream_or_discard` does for V5. A peer
    /// repeating this -- well past `max_concurrent_streams`, since each
    /// cycle reaps its batch before the next one starts -- used to grow
    /// `rejected_streams` without bound: the same shape as the zero-stride
    /// hole closed on the V5 path (`begin_v5_stream`'s `first_chunk_len ==
    /// 0` check), just reached through legacy's header-only short-circuit
    /// instead.
    ///
    /// Asserts the bound holds throughout every cycle, not merely that some
    /// later insertion eventually gets refused: the bug is that these
    /// insertions were never refused, or budgeted, at all.
    #[test]
    fn header_only_legacy_streams_never_accumulate_tombstones() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut next_stream_id = 1u64;

        for cycle in 0..300 {
            for _ in 0..16 {
                let header = crate::StreamHeader {
                    stream_id: next_stream_id,
                    total_size: 64,
                    chunk_size: 0,
                    chunk_index: 0,
                    type_hash: 0,
                    actor_id: 0,
                };
                next_stream_id += 1;
                state
                    .start_stream_with_correlation_and_kind(header, 1, pool.clone(), None, false)
                    .expect("header-only legacy StreamStart is accepted while capacity allows");
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
            let _ = state.cleanup_stale_with(
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(3600),
                1,
            );
            assert_eq!(
                state.rejected_stream_count(),
                0,
                "cycle {cycle}: a stride-less legacy stream must never be tombstoned -- it can \
                 never be removed (legacy chunk delivery never consults rejected_streams, and \
                 legacy's StreamStart never clears it either), so tombstoning it can only \
                 accumulate"
            );
        }

        assert_eq!(
            state.active_stream_count(),
            0,
            "every header-only stream must have been reaped by the end"
        );
    }

    /// Review finding: a byte-word budget alone cannot bound an entry that
    /// costs zero (or, as here, very few) words -- `REJECTED_STREAMS_ENTRY_BUDGET`
    /// exists as an independent, orthogonal ceiling on the table's entry
    /// count. Uses genuine, cheap (1-word) multi-frame V5 rejections --
    /// `REJECTED_STREAMS_ENTRY_BUDGET + 1` of them sums to a fraction of
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET`, so only the entry-count check
    /// can be the one that eventually refuses.
    #[test]
    fn entry_budget_bounds_the_table_independent_of_bitmap_words() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());

        // Fill max_concurrent_streams (16) so every further start hits the
        // resource-pressure path.
        for stream_id in 0..16u64 {
            let header = crate::StreamHeader {
                stream_id,
                total_size: 8,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8)
                .expect("connection has room for the first 16 streams")
                .expect("must be admitted, not discarded");
        }

        let mut refused_at = None;
        for i in 0..=REJECTED_STREAMS_ENTRY_BUDGET {
            let header = crate::StreamHeader {
                stream_id: 1000 + i as u64,
                total_size: 128,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            // total_size=128, first_chunk_len=8 -> 16 expected chunks -> a
            // single (1-word) bitmap: deliberately far under the byte
            // budget even summed across every iteration.
            let result = state.begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8);
            if result.is_err() {
                refused_at = Some(i);
                break;
            }
        }

        assert_eq!(
            refused_at,
            Some(REJECTED_STREAMS_ENTRY_BUDGET),
            "the entry-count budget must refuse the (REJECTED_STREAMS_ENTRY_BUDGET + 1)-th \
             cheap tombstone even though its aggregate bitmap-word cost \
             ({}) is nowhere near REJECTED_STREAMS_BITMAP_WORD_BUDGET ({})",
            REJECTED_STREAMS_ENTRY_BUDGET,
            REJECTED_STREAMS_BITMAP_WORD_BUDGET
        );
    }

    /// Review finding: `reject_stream` used to build the whole
    /// `RejectedStreamTombstone` -- allocating its completion bitmap --
    /// before `try_insert_tombstone` ever checked
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET`. A valid `MAX_STREAM_SIZE`
    /// declaration paired with a one-byte first chunk needs
    /// `MAX_STREAM_SIZE / 64` == 1,048,576 words (8 MiB) -- eight times the
    /// entire 131,072-word (1 MiB) budget -- so the old code paid for that
    /// allocation on *every* such rejection, budget or no budget. A peer
    /// repeating this against many connections spikes memory well before
    /// the budget check does anything.
    ///
    /// Asserts the allocation itself never happens, not merely that the
    /// tombstone was not inserted afterward -- see
    /// `LAST_ESTABLISHED_BITMAP_WORDS`. A fresh `StreamingState` is enough:
    /// this single request already exceeds the *entire* budget on its own,
    /// with nothing else competing for it.
    #[test]
    fn reject_stream_checks_the_bitmap_budget_before_allocating_it() {
        LAST_ESTABLISHED_BITMAP_WORDS.with(|cell| cell.set(None));

        let mut state = StreamingState::new();
        // Not admitted via begin_v5_stream_or_discard (which would need a
        // full active-stream setup to reach ResourceBusy first): calling
        // the private rejection path directly isolates exactly the
        // allocate-before-budget-check ordering this finding is about.
        let admitted = state.reject_stream(1, crate::MAX_STREAM_SIZE as u64, 1);

        assert!(
            !admitted,
            "a bitmap this large (1,048,576 words) must exceed the 131,072-word budget"
        );
        assert_eq!(
            LAST_ESTABLISHED_BITMAP_WORDS.with(|cell| cell.get()),
            None,
            "the budget check must run and refuse this rejection *before* \
             establish_stride ever allocates the ~8 MiB bitmap it would need -- \
             not merely discard the tombstone after paying for the allocation"
        );
        assert_eq!(
            state.rejected_stream_count(),
            0,
            "a budget-refused rejection must not be inserted"
        );
    }

    /// Review finding: `begin_v5_stream_or_discard` reclassified
    /// `start_stream_with_correlation_and_kind`'s `ResourceBusy` into a
    /// clean, stream-local discard -- and built a `RejectedStreamTombstone`
    /// from the request's own declared `total_size`/`first_chunk_len` --
    /// *before* that function had validated `total_size` at all, since the
    /// capacity check ran first. A peer could pair a tiny first chunk with a
    /// declared size many times over `MAX_STREAM_SIZE` and have the
    /// resulting tombstone's completion bitmap sized directly off that
    /// unvalidated value. Uses a size well past `MAX_STREAM_SIZE` (8x) --
    /// large enough to prove the bug (the old code actually performs a
    /// multi-ten-megabyte allocation for it) without needing to attempt an
    /// astronomical allocation in the test process itself to make the point.
    #[test]
    fn oversized_declared_size_is_fatal_even_when_capacity_pressure_would_otherwise_discard_it() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());

        // Fill max_concurrent_streams (16) so the next start hits the
        // resource-pressure path.
        for stream_id in 0..16u64 {
            let header = crate::StreamHeader {
                stream_id,
                total_size: 8,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8)
                .expect("connection has room for the first 16 streams")
                .expect("must be admitted, not discarded");
        }

        // Capacity is now full, so this would ordinarily hit the
        // resource-pressure discard path -- except its declared size is 8x
        // MAX_STREAM_SIZE with a one-byte first chunk, which is malformed
        // regardless of capacity and must never reach tombstone
        // construction.
        const OVERSIZED: u64 = crate::MAX_STREAM_SIZE as u64 * 8;
        let malicious = crate::StreamHeader {
            stream_id: 999,
            total_size: OVERSIZED,
            chunk_size: 1,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let result = state.begin_v5_stream_or_discard(malicious, 1, pool, false, 1);
        assert!(
            result.is_err(),
            "a declared size far past MAX_STREAM_SIZE must be a fatal protocol error, not a \
             resource-pressure discard that builds a tombstone sized off the unvalidated \
             value: {result:?}"
        );
    }

    /// Review finding: `begin_v5_stream`'s own validation only checked
    /// `first_chunk_len > total_size`, never `first_chunk_len == 0` -- and
    /// `reserve_v5_chunk`'s existing "V5 stream chunk is empty" rejection
    /// for that shape is never reached in the resource-pressure path
    /// (capacity fails first). `RejectedStreamTombstone::establish_stride`
    /// treats a zero length as "stride unknown" and leaves `received_chunks`
    /// empty, so that tombstone charges *zero* words against
    /// `REJECTED_STREAMS_BITMAP_WORD_BUDGET`. A peer at capacity could
    /// therefore send unlimited distinct header-only `StreamStart`s and
    /// grow `rejected_streams` without bound, straight past the budget
    /// meant to prevent exactly that -- the exact hole the budget exists to
    /// close, just via a different field than `total_size`.
    #[test]
    fn header_only_stream_start_is_fatal_even_when_capacity_pressure_would_otherwise_discard_it() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());

        for stream_id in 0..16u64 {
            let header = crate::StreamHeader {
                stream_id,
                total_size: 8,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8)
                .expect("connection has room for the first 16 streams")
                .expect("must be admitted, not discarded");
        }

        // Capacity is now full, so this would ordinarily hit the
        // resource-pressure discard path -- except its first chunk is
        // empty, which is malformed regardless of capacity and must never
        // reach tombstone construction.
        let header_only = crate::StreamHeader {
            stream_id: 999,
            total_size: 64,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let result = state.begin_v5_stream_or_discard(header_only, 1, pool, false, 0);
        assert!(
            result.is_err(),
            "a header-only StreamStart (an empty first chunk) must be a fatal protocol error, \
             not a resource-pressure discard that builds a zero-budget tombstone: {result:?}"
        );
        assert_eq!(
            state.rejected_stream_count(),
            0,
            "a rejected header-only StreamStart must not leave any tombstone behind -- a \
             zero-budget entry defeats the aggregate cap regardless of how small it looks"
        );
    }

    /// Review finding (P2): when the `StreamStart`'s own inline first chunk
    /// already covers the entire declared size, `RejectedStreamTombstone::rejected`
    /// produces an already-complete tombstone -- but `reject_stream` used to
    /// insert it anyway. Nothing will ever arrive to remove it (completion
    /// is already true, so it can never re-trigger; the only other removal
    /// path is a retry of this exact `stream_id`, which a peer that never
    /// reuses ids will never do), so every ordinary one-frame rejection
    /// leaked one map entry forever.
    #[test]
    fn single_frame_rejection_does_not_leak_an_already_complete_tombstone() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());

        for stream_id in 0..16u64 {
            let header = crate::StreamHeader {
                stream_id,
                total_size: 8,
                chunk_size: 8,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            };
            state
                .begin_v5_stream_or_discard(header, 1, pool.clone(), false, 8)
                .expect("connection has room for the first 16 streams")
                .expect("must be admitted, not discarded");
        }

        // total_size == first_chunk_len: the entire declared stream fits in
        // the StreamStart's own inline payload -- an ordinary single-frame
        // rejection, not malformed at all.
        let single_frame = crate::StreamHeader {
            stream_id: 999,
            total_size: 8,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let result = state.begin_v5_stream_or_discard(single_frame, 1, pool, false, 8);
        assert!(
            result
                .expect("a single-frame rejection is a clean resource-pressure discard, not fatal")
                .is_none(),
            "a single-frame rejection must not be admitted"
        );

        assert_eq!(
            state.rejected_stream_count(),
            0,
            "a tombstone that is already complete at construction must not be inserted at all \
             -- it can never be removed by completion (already true) and may never be retried, \
             leaking a map entry forever otherwise"
        );
    }

    /// A single large chunk that trickles in slowly must not be reaped
    /// mid-read while the reader holds a live reservation. Progress must be
    /// recorded explicitly via `record_v5_chunk_progress` -- mirroring what
    /// the IO reader does after each successful partial read -- rather than
    /// as a side effect of merely asking for the next write target, since a
    /// chunk larger than one idle window (e.g. a 1 MiB chunk at <17 KB/s) is
    /// polled for its target many times before any of those polls actually
    /// transfers a byte.
    #[test]
    fn partial_v5_chunk_progress_keeps_stream_alive_past_idle_timeout() {
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(40);

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 200,
            total_size: 64,
            chunk_size: 64,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let reservation = state
            .begin_v5_stream(header, 1, pool, false, 64)
            .expect("reserve the single large chunk");

        // Trickle the one large chunk in small pieces, each comfortably
        // inside the idle window, but let total elapsed time cross the idle
        // window several times over before the chunk ever fully commits.
        let mut read = 0usize;
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(15));
            let target = state
                .v5_chunk_target(reservation, read)
                .expect("reservation is still live");
            target[..8].copy_from_slice(&[0xAB; 8]);
            read += 8;
            state.record_v5_chunk_progress(reservation, 8);
            let _ = state.cleanup_stale_with(idle_timeout, Duration::from_secs(3600), 1);
            assert_eq!(
                state.active_stream_count(),
                1,
                "a stream receiving partial-chunk byte progress must not be reaped mid-read"
            );
        }
    }

    /// The minimum-sustained-rate window must be credited by bytes actually
    /// read, not just bytes committed. A single large chunk that never
    /// fully commits during this test still trickles in real, steady
    /// partial progress every tick; the rate window must see that progress
    /// and let the stream survive across several windows, exactly like
    /// `partial_v5_chunk_progress_keeps_stream_alive_past_idle_timeout`
    /// proves for the idle check.
    ///
    /// This fails against a `cleanup_stale_with` that credits the rate
    /// window off `received_size` alone: `received_size` stays 0 for the
    /// whole test (the chunk never commits), so the very first rate window
    /// to elapse would see zero bytes and reap the stream mid-transfer --
    /// reintroducing, through the rate bound, the mid-chunk reap this whole
    /// change exists to prevent.
    #[test]
    fn steady_partial_progress_through_one_large_chunk_survives_several_rate_windows() {
        use std::time::Duration;

        let idle_timeout = Duration::from_secs(3600); // not under test here
        let rate_window = Duration::from_millis(15);
        let min_bytes_per_window = 4;

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 220,
            // Large enough that 8 ticks of 8 bytes each (64 bytes total)
            // never completes it, so `received_size` never advances at all
            // during this test -- any survival here is entirely down to
            // crediting the in-flight partial read.
            total_size: 128,
            chunk_size: 128,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let reservation = state
            .begin_v5_stream(header, 1, pool, false, 128)
            .expect("reserve the single large chunk");

        let mut read = 0usize;
        for tick in 0..8 {
            std::thread::sleep(Duration::from_millis(5));
            let target = state
                .v5_chunk_target(reservation, read)
                .expect("reservation is still live");
            target[..8].copy_from_slice(&[0xCDu8; 8]);
            read += 8;
            state.record_v5_chunk_progress(reservation, 8);
            let _ = state.cleanup_stale_with(idle_timeout, rate_window, min_bytes_per_window);
            assert_eq!(
                state.active_stream_count(),
                1,
                "tick {tick}: steady partial progress through an uncommitted chunk must survive \
                 the rate-window check"
            );
        }
    }

    /// Merely asking for the write target (polling, with no bytes actually
    /// transferred) must not count as progress -- otherwise a peer that
    /// opens a stream's reservation and then never sends another byte would
    /// still get its idle timer refreshed on every poll of the IO reader,
    /// pinning the slot and its reservation forever.
    #[test]
    fn polling_the_chunk_target_without_reading_bytes_does_not_refresh_activity() {
        use std::time::Duration;

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 210,
            total_size: 64,
            chunk_size: 64,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let reservation = state
            .begin_v5_stream(header, 1, pool, false, 64)
            .expect("reserve the chunk");

        std::thread::sleep(Duration::from_millis(15));
        // The reader is polled repeatedly (e.g. the socket is not yet
        // readable) but no byte is ever transferred, so progress is never
        // recorded.
        for _ in 0..5 {
            let _ = state
                .v5_chunk_target(reservation, 0)
                .expect("reservation is still live");
        }

        let _ = state.cleanup_stale_with(Duration::from_millis(10), Duration::from_secs(3600), 1);
        assert_eq!(
            state.active_stream_count(),
            0,
            "polling for a write target without transferring bytes must not block idle reaping"
        );
    }

    /// Once the reaper removes a stream (idle timeout, no absolute cap), the
    /// sender's next in-flight chunk for that id must be a clean per-stream
    /// rejection, not a fatal "unknown stream_id" that tears down the whole
    /// connection. This requires the reaped id to be tombstoned into
    /// `rejected_streams`, mirroring the existing resource-pressure
    /// rejection path.
    #[test]
    fn reaped_stream_tombstones_so_late_chunk_is_a_clean_rejection_not_a_fatal_teardown() {
        use std::time::Duration;

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 201,
            total_size: 16,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let _ = state
            .begin_v5_stream(header, 1, pool, false, 8)
            .expect("start stream and reserve its first chunk");

        // Idle out with nothing further arriving for this stream, then reap.
        std::thread::sleep(Duration::from_millis(20));
        let _ = state.cleanup_stale_with(Duration::from_millis(5), Duration::from_secs(3600), 1);
        assert_eq!(
            state.active_stream_count(),
            0,
            "the stream must have been reaped for the test to be meaningful"
        );

        // The sender, unaware the stream was reaped, sends its next chunk.
        let result = state.reserve_v5_chunk_or_discard(201, 1, 8);
        assert!(
            result.is_ok(),
            "a late chunk for a reaped stream must not be a fatal protocol error: {result:?}"
        );
        assert!(
            result.unwrap().is_none(),
            "a late chunk for a reaped stream must be discarded, not accepted as a fresh reservation"
        );
    }

    /// A tombstone must never be pruned by wall-clock age alone: it is
    /// removed only once proven complete or superseded by a retry, never by
    /// a TTL swept by `cleanup_stale_with` (see `RejectedStreamTombstone`'s
    /// invariant, and `rejected_stream_table_never_evicts_incomplete_tombstones_no_matter_how_many_are_reaped`
    /// for the companion "no capacity eviction either" property). A sender
    /// that keeps trickling chunks into a reaped id must keep getting them
    /// silently discarded no matter how many cleanup sweeps run in between
    /// or how long the gaps between hits are -- there is no window in which
    /// the tombstone can have "aged out" on its own.
    #[test]
    fn tombstone_survives_repeated_cleanup_sweeps_regardless_of_elapsed_time() {
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(30);

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 301,
            total_size: 8,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let _ = state
            .begin_v5_stream(header, 1, pool, false, 8)
            .expect("start stream and reserve its first chunk");

        // Wants elapsed >= idle_timeout to force the reap: `sleep` only
        // guarantees a lower bound, so any overshoot only makes the reap
        // more certain.
        std::thread::sleep(idle_timeout + Duration::from_millis(10));
        let _ = state.cleanup_stale_with(idle_timeout, Duration::from_secs(3600), 1);
        assert_eq!(
            state.active_stream_count(),
            0,
            "stream must be reaped first"
        );
        assert_eq!(state.rejected_stream_count(), 1);

        // Repeated cleanup sweeps, each well past the old TTL this table
        // used to be pruned by, must never remove the tombstone on their
        // own -- only its own completion or a retry can.
        for _ in 0..4 {
            std::thread::sleep(idle_timeout * 3);
            let _ = state.cleanup_stale_with(idle_timeout, Duration::from_secs(3600), 1);
            assert_eq!(
                state.rejected_stream_count(),
                1,
                "a cleanup sweep must never prune a tombstone by age alone"
            );
        }

        // The tombstone is still live after all of that: a late chunk is
        // still a clean discard, not a fatal "unknown stream_id".
        let result = state
            .reserve_v5_chunk_or_discard(301, 1, 8)
            .expect("a late chunk for a still-tombstoned id must never be a fatal protocol error");
        assert!(
            result.is_none(),
            "a late chunk for a reaped stream must be discarded, not accepted as fresh"
        );
    }

    /// The production `io_task` loop calls `cleanup_stale` once per turn
    /// *before* draining any new frames (see `cleanup_stale_with`'s doc
    /// comment) -- so an arbitrary number of cleanup sweeps can run between
    /// a stream being reaped and its sender's next chunk being read, with no
    /// refreshing hit in between. A version of this test that hit the
    /// tombstone (refreshing it) before each cleanup sweep -- the reverse of
    /// production order -- could never observe a prune-before-classify race
    /// and would pass whether or not `rejected_streams` was ever pruned by
    /// age at all. This one drives cleanup, cleanup, cleanup, *then* the
    /// late chunk, matching production exactly.
    #[test]
    fn late_chunk_is_a_clean_discard_after_cleanup_sweeps_ahead_of_it_with_no_refresh() {
        use std::time::Duration;

        let idle_timeout = Duration::from_millis(30);

        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let header = crate::StreamHeader {
            stream_id: 401,
            total_size: 8,
            chunk_size: 8,
            chunk_index: 0,
            type_hash: 0,
            actor_id: 0,
        };
        let _ = state
            .begin_v5_stream(header, 1, pool, false, 8)
            .expect("start stream and reserve its first chunk");

        std::thread::sleep(idle_timeout + Duration::from_millis(10));
        let _ = state.cleanup_stale_with(idle_timeout, Duration::from_secs(3600), 1);
        assert_eq!(
            state.active_stream_count(),
            0,
            "stream must be reaped first"
        );

        // Several more cleanup sweeps run, each well past what the
        // tombstone's TTL used to be, with no chunk arriving -- and so no
        // refresh -- in between. This is the production ordering: `io_task`
        // calls `cleanup_stale` on every turn regardless of whether a frame
        // is waiting to be read.
        for _ in 0..3 {
            std::thread::sleep(idle_timeout * 3);
            let _ = state.cleanup_stale_with(idle_timeout, Duration::from_secs(3600), 1);
        }

        // *Now* the late chunk arrives -- after cleanup has already swept
        // ahead of it, never before it. Under wall-clock-TTL pruning this
        // tombstone would already be gone by this point, and this call
        // would return the fatal "unknown stream_id" network error instead
        // of a clean discard.
        let result = state.reserve_v5_chunk_or_discard(401, 1, 8);
        assert!(
            result.is_ok(),
            "a late chunk must not become a fatal protocol error just because cleanup swept \
             ahead of it with no intervening refresh: {result:?}"
        );
        assert!(
            result.unwrap().is_none(),
            "a late chunk for a reaped stream must be discarded, not accepted as a fresh reservation"
        );
    }

    #[test]
    fn v5_direct_read_keeps_alternating_streams_byte_exact() {
        let mut state = StreamingState::new();
        let pool = Arc::new(crate::AlignedBytesPool::new(2));
        let reserve_start = |state: &mut StreamingState, id, pool: Arc<crate::AlignedBytesPool>| {
            state.begin_v5_stream(
                crate::StreamHeader {
                    stream_id: id,
                    total_size: 4,
                    chunk_size: 2,
                    chunk_index: 0,
                    type_hash: id as u32,
                    actor_id: id,
                },
                id as u32,
                pool,
                false,
                2,
            )
        };
        let a0 = reserve_start(&mut state, 100, pool.clone()).unwrap();
        state.v5_chunk_target(a0, 0).unwrap().copy_from_slice(b"ab");
        let b0 = reserve_start(&mut state, 200, pool).unwrap();
        state.v5_chunk_target(b0, 0).unwrap().copy_from_slice(b"12");
        assert!(state.commit_v5_chunk(b0).unwrap().is_none());
        assert!(state.commit_v5_chunk(a0).unwrap().is_none());

        let a1 = state.reserve_v5_chunk(100, 1, 2).unwrap();
        state.v5_chunk_target(a1, 0).unwrap().copy_from_slice(b"cd");
        let b1 = state.reserve_v5_chunk(200, 1, 2).unwrap();
        state.v5_chunk_target(b1, 0).unwrap().copy_from_slice(b"34");
        let b = state.commit_v5_chunk(b1).unwrap().unwrap();
        let a = state.commit_v5_chunk(a1).unwrap().unwrap();
        assert_eq!(a.payload.as_ref(), b"abcd");
        assert_eq!(b.payload.as_ref(), b"1234");
    }
}
