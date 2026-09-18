//! Connection-local lease qualification counters.
//!
//! This module is compiled only for tests and the opt-in test-helper feature.
//! Counters are owned by one `ReplySlots` instance; there is no production
//! global state, lock, or allocator hook on the lease hot path.

#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub(crate) struct LeaseStats {
    pub(crate) live_slots: AtomicUsize,
    pub(crate) reserved_jobs: AtomicUsize,
    pub(crate) reserved_bytes: AtomicUsize,
    pub(crate) retained_payload_owners: AtomicUsize,
    pub(crate) cancellation_publications: AtomicUsize,
    pub(crate) discarded_reservations: AtomicUsize,
    pub(crate) actual_stream_aborts: AtomicUsize,
    pub(crate) normal_completions: AtomicUsize,
    pub(crate) terminal_completions: AtomicUsize,
    pub(crate) too_late_cancellations: AtomicUsize,
    pub(crate) frame_progress: AtomicUsize,
    pub(crate) flush_progress: AtomicUsize,
    #[cfg(test)]
    pub(crate) permits_released: AtomicBool,
    #[cfg(test)]
    pub(crate) accounting_before_permits: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaseStatsSnapshot {
    pub live_slots: usize,
    pub free_slots: usize,
    pub ready_depth: usize,
    pub reserved_jobs: usize,
    pub reserved_bytes: usize,
    pub retained_payload_owners: usize,
    pub cancellation_publications: usize,
    pub discarded_reservations: usize,
    pub actual_stream_aborts: usize,
    pub normal_completions: usize,
    pub terminal_completions: usize,
    pub too_late_cancellations: usize,
    pub frame_progress: usize,
    pub flush_progress: usize,
}

impl LeaseStats {
    pub(crate) fn snapshot(&self, free_slots: usize, ready_depth: usize) -> LeaseStatsSnapshot {
        LeaseStatsSnapshot {
            live_slots: self.live_slots.load(Ordering::Acquire),
            free_slots,
            ready_depth,
            reserved_jobs: self.reserved_jobs.load(Ordering::Acquire),
            reserved_bytes: self.reserved_bytes.load(Ordering::Acquire),
            retained_payload_owners: self.retained_payload_owners.load(Ordering::Acquire),
            cancellation_publications: self.cancellation_publications.load(Ordering::Acquire),
            discarded_reservations: self.discarded_reservations.load(Ordering::Acquire),
            actual_stream_aborts: self.actual_stream_aborts.load(Ordering::Acquire),
            normal_completions: self.normal_completions.load(Ordering::Acquire),
            terminal_completions: self.terminal_completions.load(Ordering::Acquire),
            too_late_cancellations: self.too_late_cancellations.load(Ordering::Acquire),
            frame_progress: self.frame_progress.load(Ordering::Acquire),
            flush_progress: self.flush_progress.load(Ordering::Acquire),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connection_pool::{BufferConfig, ChannelId, LockFreeStreamHandle};
    use crate::{AskResponder, ReplyDeliveryBudget, ReplyPayload};
    use bytes::Bytes;
    use std::sync::{Arc, atomic::AtomicBool};
    use tokio::io::AsyncReadExt;

    #[test]
    fn stats_classify_committed_wire_outcome() {
        let slots = crate::connection_pool::reply_slots::ReplySlots::new(
            1,
            "127.0.0.1:40572".parse().expect("test address"),
            Arc::new(tokio::sync::Notify::new()),
        );
        let jobs = Arc::new(tokio::sync::Semaphore::new(1));
        let bytes = Arc::new(tokio::sync::Semaphore::new(16));
        let record = slots
            .try_reserve(
                1,
                16,
                Arc::new(ReplyPayload::from_static(b"cancelled")),
                jobs.try_acquire_owned().expect("job permit"),
                bytes.try_acquire_many_owned(16).expect("byte permit"),
            )
            .expect("lease reservation");
        record
            .publish(ReplyPayload::from_static(b"normal-payload"))
            .expect("normal publication");
        // The payload is normal, but the writer selected a terminal outcome.
        // Stats must report the committed writer outcome rather than infer it
        // from publication/cancellation state.
        record.note_terminal_outcome();
        record.complete();
        let stats = slots.stats_snapshot();
        assert_eq!(stats.normal_completions, 0);
        assert_eq!(stats.terminal_completions, 1);
        record.finish();
    }

    #[test]
    fn stats_distinguish_discard_from_committed_and_too_late_cancel() {
        let slots = crate::connection_pool::reply_slots::ReplySlots::new(
            1,
            "127.0.0.1:40573".parse().expect("test address"),
            Arc::new(tokio::sync::Notify::new()),
        );
        let jobs = Arc::new(tokio::sync::Semaphore::new(2));
        let bytes = Arc::new(tokio::sync::Semaphore::new(32));
        let discarded = slots
            .try_reserve(
                1,
                16,
                Arc::new(ReplyPayload::from_static(b"cancelled")),
                jobs.clone().try_acquire_owned().expect("job permit"),
                bytes
                    .clone()
                    .try_acquire_many_owned(16)
                    .expect("byte permit"),
            )
            .expect("discarded reservation");
        discarded.discard();
        let after_discard = slots.stats_snapshot();
        assert_eq!(after_discard.discarded_reservations, 1);
        assert_eq!(after_discard.normal_completions, 0);
        assert_eq!(
            after_discard.terminal_completions, 0,
            "discarded reservations did not deliver a terminal frame"
        );

        let committed = slots
            .try_reserve(
                2,
                16,
                Arc::new(ReplyPayload::from_static(b"cancelled")),
                jobs.try_acquire_owned().expect("job permit"),
                bytes.try_acquire_many_owned(16).expect("byte permit"),
            )
            .expect("committed reservation");
        committed
            .publish(ReplyPayload::from_static(b"reply"))
            .expect("normal publication");
        committed.note_normal_outcome();
        committed.cancel();
        let after_late_cancel = slots.stats_snapshot();
        assert_eq!(
            after_late_cancel.too_late_cancellations, 1,
            "cancellation after writer outcome commit must be classified as too late"
        );
    }

    #[tokio::test]
    async fn stats_track_reservation_publication_and_abort() {
        let budget =
            ReplyDeliveryBudget::new(2, 128, ReplyPayload::from_static(b"duplicate-suppressed"))
                .expect("valid budget");
        let (io, mut peer) = tokio::io::duplex(4096);
        let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
            io,
            "127.0.0.1:40570".parse().expect("test address"),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let handle = Arc::new(handle);
        let responder = AskResponder::from_stream_handle(
            100,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        );
        let lease = responder
            .try_reply_lease(&budget, 32)
            .expect("lease admission");
        let reserved = handle.reply_slots().stats_snapshot();
        assert_eq!(reserved.live_slots, 1);
        assert_eq!(reserved.reserved_jobs, 1);
        assert_eq!(reserved.reserved_bytes, 32);

        lease
            .try_reply_bytes(ReplyPayload::copy_from_slice(b"reply"))
            .expect("publish");
        let published = handle.reply_slots().stats_snapshot();
        assert_eq!(published.retained_payload_owners, 1);
        assert_eq!(published.cancellation_publications, 0);

        let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
        peer.read_exact(&mut header).await.expect("response header");
        let payload_len = crate::framing::decode_control(header[..4].try_into().expect("control"))
            .expect("response control")
            .body_len
            .saturating_sub(crate::framing::ASK_RESPONSE_HEADER_LEN);
        let mut payload = vec![0u8; payload_len];
        peer.read_exact(&mut payload)
            .await
            .expect("response payload");
        assert_eq!(Bytes::from(payload), Bytes::from_static(b"reply"));

        let canceled = AskResponder::from_stream_handle(
            101,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        )
        .try_reply_lease(&budget, 32)
        .expect("second lease admission");
        drop(canceled);
        let before_shutdown = handle.reply_slots().stats_snapshot();
        assert_eq!(before_shutdown.cancellation_publications, 1);

        handle.shutdown();
        let _ = writer_task.await;
        let final_stats = handle.reply_slots().stats_snapshot();
        assert!(final_stats.normal_completions >= 1);
        assert_eq!(final_stats.reserved_jobs, 0);
        assert_eq!(final_stats.reserved_bytes, 0);
    }
}
