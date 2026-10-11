//! Connection-scoped ActorAsk route slots.
//!
//! A table belongs to exactly one transport instance. Dropping that instance
//! drops all bindings, so a slot from a previous connection can never become
//! valid after reconnect.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

/// A peer cannot grow route state beyond this per-connection limit.
pub(crate) const MAX_ROUTES_PER_CONNECTION: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RouteKey {
    pub(crate) actor_id: u64,
    pub(crate) type_hash: u32,
}

#[derive(Debug)]
pub(crate) struct RouteTable {
    next_slot: AtomicU32,
    max_routes: usize,
    maps: RwLock<RouteMaps>,
    published: OnceLock<Box<[PublishedRoute]>>,
    inbound: OnceLock<Box<[PublishedRoute]>>,
}

// Fixed, immutable publication cells. Each successful bind consumes one cell
// for the connection lifetime; saturation falls back to the uncompact frame.
// Warm lookup performs at most eight acquire loads, with no locks or allocation.
const ROUTE_PROBES: usize = 8;

#[derive(Debug, Default)]
struct PublishedRoute {
    slot: AtomicU32,
    actor_id: AtomicU64,
    type_hash: AtomicU32,
}

impl PublishedRoute {
    fn key(&self) -> RouteKey {
        RouteKey {
            actor_id: self.actor_id.load(Ordering::Relaxed),
            type_hash: self.type_hash.load(Ordering::Relaxed),
        }
    }

    // All writers are serialized by the cold map lock. Published cells are
    // never modified, including by cancellation and rollback.
    fn publish(&self, slot: u32, key: RouteKey) {
        self.actor_id.store(key.actor_id, Ordering::Relaxed);
        self.type_hash.store(key.type_hash, Ordering::Relaxed);
        self.slot.store(slot, Ordering::Release);
    }
}

#[derive(Debug, Default)]
struct RouteMaps {
    by_key: HashMap<RouteKey, u32>,
    by_slot: HashMap<u32, RouteKey>,
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        Self::with_limit(MAX_ROUTES_PER_CONNECTION)
    }

    fn with_limit(max_routes: usize) -> Self {
        Self {
            next_slot: AtomicU32::new(1),
            max_routes,
            maps: RwLock::new(RouteMaps::default()),
            published: OnceLock::new(),
            inbound: OnceLock::new(),
        }
    }

    fn initialize_cells<'a>(
        &self,
        cells: &'a OnceLock<Box<[PublishedRoute]>>,
    ) -> &'a [PublishedRoute] {
        cells.get_or_init(|| {
            (0..self.max_routes.saturating_mul(2).max(1))
                .map(|_| PublishedRoute::default())
                .collect()
        })
    }

    /// Hands out the next slot id, or `None` once the id space is exhausted.
    ///
    /// A CAS loop (not `fetch_add`) so exhaustion at `u32::MAX` is terminal:
    /// `fetch_add` wraps silently on overflow, which would eventually hand
    /// out slot `0`, then `1`, `2`, ... again — colliding with whatever
    /// (actor_id, type_hash) is still bound at those low ids and letting the
    /// peer misroute a compact frame to the wrong actor. Once exhausted this
    /// always returns `None`; callers fall back to the uncompact ask path.
    fn allocate_slot(&self) -> Option<u32> {
        loop {
            let current = self.next_slot.load(Ordering::Acquire);
            if current == 0 || current == u32::MAX {
                return None;
            }
            if self
                .next_slot
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(current);
            }
        }
    }

    /// Allocates once per route for this connection. Zero is never a route.
    pub(crate) fn slot_for(&self, key: RouteKey) -> Option<(u32, bool)> {
        let cells = self.initialize_cells(&self.published);
        if let Some(slot) = self.maps.read().ok()?.by_key.get(&key).copied() {
            return Some((slot, false));
        }
        let mut maps = self.maps.write().ok()?;
        if let Some(slot) = maps.by_key.get(&key).copied() {
            return Some((slot, false));
        }
        if maps.by_key.len() >= self.max_routes {
            return None;
        }
        let start = Self::route_hash(key) % cells.len();
        if !(0..ROUTE_PROBES).any(|probe| {
            cells[(start + probe) % cells.len()]
                .slot
                .load(Ordering::Acquire)
                == 0
        }) {
            return None;
        }
        let slot = self.allocate_slot()?;
        maps.by_slot.insert(slot, key);
        maps.by_key.insert(key, slot);
        Some((slot, true))
    }

    /// Receiver-side bind. Conflicting rebinding is rejected fail-closed.
    pub(crate) fn bind(&self, slot: u32, key: RouteKey) -> bool {
        if slot == 0 {
            return false;
        }
        let cells = self.initialize_cells(&self.inbound);
        let mut maps = match self.maps.write() {
            Ok(value) => value,
            Err(_) => return false,
        };
        match maps.by_slot.get(&slot) {
            Some(existing) if *existing != key => false,
            Some(_) => {
                let start = slot as usize % cells.len();
                for probe in 0..ROUTE_PROBES {
                    let cell = &cells[(start + probe) % cells.len()];
                    match cell.slot.load(Ordering::Acquire) {
                        existing if existing == slot => return true,
                        0 => {
                            cell.publish(slot, key);
                            return true;
                        }
                        _ => {}
                    }
                }
                false
            }
            None => {
                if maps.by_key.len() >= self.max_routes {
                    return false;
                }
                if maps
                    .by_key
                    .get(&key)
                    .is_some_and(|existing| *existing != slot)
                {
                    return false;
                }
                let start = slot as usize % cells.len();
                let Some(cell) = (0..ROUTE_PROBES)
                    .map(|probe| &cells[(start + probe) % cells.len()])
                    .find(|cell| cell.slot.load(Ordering::Acquire) == 0)
                else {
                    return false;
                };
                cell.publish(slot, key);
                maps.by_slot.insert(slot, key);
                maps.by_key.insert(key, slot);
                true
            }
        }
    }

    pub(crate) fn resolve(&self, slot: u32) -> Option<RouteKey> {
        let cells = self.inbound.get()?;
        let start = slot as usize % cells.len();
        for probe in 0..ROUTE_PROBES {
            let cell = &cells[(start + probe) % cells.len()];
            if cell.slot.load(Ordering::Acquire) == slot && slot != 0 {
                return Some(cell.key());
            }
        }
        None
    }

    fn route_hash(key: RouteKey) -> usize {
        (key.actor_id ^ (u64::from(key.type_hash) << 32)).wrapping_mul(0x9e37_79b9_7f4a_7c15)
            as usize
    }

    pub(crate) fn published_slot_for(&self, key: RouteKey) -> Option<u32> {
        let cells = self.published.get()?;
        let start = Self::route_hash(key) % cells.len();
        for probe in 0..ROUTE_PROBES {
            let cell = &cells[(start + probe) % cells.len()];
            let slot = cell.slot.load(Ordering::Acquire);
            if slot != 0 && cell.key() == key {
                return Some(slot);
            }
        }
        None
    }

    fn publish_outbound(&self, slot: u32, key: RouteKey) {
        let cells = self.initialize_cells(&self.published);
        let Ok(_maps) = self.maps.write() else {
            return;
        };
        let start = Self::route_hash(key) % cells.len();
        for probe in 0..ROUTE_PROBES {
            let cell = &cells[(start + probe) % cells.len()];
            let existing = cell.slot.load(Ordering::Acquire);
            if existing == slot {
                return;
            }
            if existing == 0 {
                cell.publish(slot, key);
                return;
            }
        }
    }

    /// Undo an outbound allocation whose bind frame was not enqueued. The
    /// caller serializes first-use attempts, so removing only this exact pair
    /// cannot discard a route that has since become usable on the wire.
    pub(crate) fn remove_unbound(&self, slot: u32, key: RouteKey) {
        let Ok(mut maps) = self.maps.write() else {
            return;
        };
        if maps.by_slot.get(&slot) == Some(&key) && maps.by_key.get(&key) == Some(&slot) {
            maps.by_slot.remove(&slot);
            maps.by_key.remove(&key);
        }
    }
}

/// RAII rollback for a freshly-allocated outbound route whose `RouteBind`
/// frame has not yet been enqueued. `Drop` calls [`RouteTable::remove_unbound`]
/// unless the caller [`disarm`](Self::disarm)s the guard after the bind frame is
/// successfully queued — making the allocation cancel-safe at every await
/// point (R-3).
///
/// Without this, dropping the `write_routed_actor_ask` future while its bind
/// enqueue is parked on a full write queue leaves the route marked bound, so
/// the next ask emits a `RoutedActorAsk` for a slot the peer never learned
/// (`unknown route slot` -> connection teardown).
pub(crate) struct UnboundRouteGuard<'a> {
    table: &'a RouteTable,
    slot: u32,
    key: RouteKey,
    armed: bool,
}

impl<'a> UnboundRouteGuard<'a> {
    /// Arm a rollback for `key` freshly allocated at `slot`.
    pub(crate) fn new(table: &'a RouteTable, slot: u32, key: RouteKey) -> Self {
        Self {
            table,
            slot,
            key,
            armed: true,
        }
    }

    /// Cancel the rollback. Call only once the `RouteBind` frame is
    /// successfully enqueued — the writer task owns retry/teardown from there,
    /// so the route must stay bound.
    pub(crate) fn disarm(&mut self) {
        self.table.publish_outbound(self.slot, self.key);
        self.armed = false;
    }
}

impl Drop for UnboundRouteGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.table.remove_unbound(self.slot, self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warm_routes_bypass_cold_bind_publication() {
        let table = RouteTable::new();
        let warm = RouteKey {
            actor_id: 17,
            type_hash: 19,
        };
        let cold = RouteKey {
            actor_id: 23,
            type_hash: 29,
        };
        let (warm_slot, _) = table.slot_for(warm).unwrap();
        let mut committed = UnboundRouteGuard::new(&table, warm_slot, warm);
        committed.disarm();
        assert!(table.bind(warm_slot, warm));
        let (cold_slot, _) = table.slot_for(cold).unwrap();
        let pending = UnboundRouteGuard::new(&table, cold_slot, cold);
        {
            let _cold_writer = table.maps.write().unwrap();
            assert_eq!(table.published_slot_for(warm), Some(warm_slot));
            assert_eq!(table.resolve(warm_slot), Some(warm));
            assert_eq!(table.published_slot_for(cold), None);
            assert_eq!(table.resolve(cold_slot), None);
        }
        drop(pending);
        assert_eq!(table.published_slot_for(cold), None);
        assert_eq!(table.published_slot_for(warm), Some(warm_slot));
    }

    #[test]
    fn warm_route_lookup_has_scoped_allocation_and_lock_controls() {
        use crate::ask_forwarder::allocation_evidence;

        let table = RouteTable::new();
        let key = RouteKey {
            actor_id: 17,
            type_hash: 19,
        };
        let (slot, _) = table.slot_for(key).unwrap();
        let mut publication = UnboundRouteGuard::new(&table, slot, key);
        publication.disarm();
        assert!(table.bind(slot, key));
        let cold_lock = table.maps.write().unwrap();
        let (_, counts) = allocation_evidence::measure(|| {
            for _ in 0..32 {
                assert_eq!(table.published_slot_for(key), Some(slot));
                assert_eq!(table.resolve(slot), Some(key));
                assert_eq!(table.resolve(0), None);
            }
        });
        drop(cold_lock);
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        let (positive, counts) =
            allocation_evidence::measure(|| std::hint::black_box(vec![0u8; 17]));
        assert!(counts.allocations > 0);
        assert!(counts.requested_bytes >= positive.len());
    }

    #[test]
    fn route_slots_bind_once_and_reject_conflicts() {
        let table = RouteTable::new();
        let first = RouteKey {
            actor_id: 7,
            type_hash: 9,
        };
        let second = RouteKey {
            actor_id: 8,
            type_hash: 9,
        };
        let (slot, fresh) = table.slot_for(first).unwrap();
        assert!(fresh);
        assert_eq!(table.slot_for(first), Some((slot, false)));
        assert!(table.bind(slot, first));
        assert!(!table.bind(slot, second));
        assert!(!table.bind(slot + 1, first));
        assert_eq!(table.resolve(slot), Some(first));
    }

    #[test]
    fn reconnect_starts_with_no_stale_slots() {
        let old = RouteTable::new();
        let (slot, _) = old
            .slot_for(RouteKey {
                actor_id: 7,
                type_hash: 9,
            })
            .unwrap();
        let new_connection = RouteTable::new();
        assert_eq!(new_connection.resolve(slot), None);
    }

    #[test]
    fn failed_first_bind_can_be_rolled_back_for_a_safe_retry() {
        let table = RouteTable::new();
        let key = RouteKey {
            actor_id: 7,
            type_hash: 9,
        };
        let (slot, fresh) = table.slot_for(key).unwrap();
        assert!(fresh);
        table.remove_unbound(slot, key);
        assert_eq!(table.resolve(slot), None);
        let (retry_slot, retry_is_fresh) = table.slot_for(key).unwrap();
        assert!(retry_is_fresh);
        assert_ne!(retry_slot, slot);
    }

    #[test]
    fn route_table_has_a_hard_per_connection_limit() {
        let table = RouteTable::with_limit(1);
        let first = RouteKey {
            actor_id: 7,
            type_hash: 9,
        };
        let second = RouteKey {
            actor_id: 8,
            type_hash: 9,
        };
        assert!(table.slot_for(first).is_some());
        assert_eq!(table.slot_for(second), None);
        assert!(!table.bind(2, second));
        assert!(table.bind(1, first), "existing binding remains idempotent");
    }

    /// Slot-id exhaustion must be a terminal, safe dead end: once the
    /// allocator hits `u32::MAX` it must return `None` forever, never wrap
    /// back around to a low id that a still-live route already owns. A
    /// `fetch_add`-based allocator wraps silently past `u32::MAX`, which
    /// would eventually reissue slot `1` (still bound to `early` here) to a
    /// brand-new, unrelated key — letting the peer misroute a compact frame.
    #[test]
    fn next_slot_exhaustion_is_permanent_and_never_wraps_to_a_reusable_slot() {
        let table = RouteTable::new();
        let early = RouteKey {
            actor_id: 1,
            type_hash: 1,
        };
        let (early_slot, _) = table.slot_for(early).unwrap();
        assert_eq!(early_slot, 1);

        // Simulate years of cancel/rollback churn driving the allocator to
        // the brink of exhaustion without needing 4B real routes.
        table.next_slot.store(u32::MAX - 1, Ordering::Relaxed);

        let last_valid = RouteKey {
            actor_id: 2,
            type_hash: 2,
        };
        let (slot, fresh) = table.slot_for(last_valid).unwrap();
        assert!(fresh);
        assert_eq!(slot, u32::MAX - 1);

        let probe = RouteKey {
            actor_id: 99,
            type_hash: 99,
        };
        for _ in 0..8 {
            assert_eq!(
                table.slot_for(probe),
                None,
                "exhausted allocator must not wrap back to a low, still-live slot"
            );
        }
    }

    /// R-3: a dropped-while-armed `UnboundRouteGuard` rolls back the fresh
    /// allocation; a disarmed guard leaves the binding intact. This is the
    /// cancel-safety primitive for `write_routed_actor_ask`'s bind enqueue.
    #[test]
    fn qa_r3_unbound_route_guard_rolls_back_only_while_armed() {
        let table = RouteTable::new();
        let key = RouteKey {
            actor_id: 7,
            type_hash: 9,
        };

        // Armed drop -> rolled back.
        let (slot, fresh) = table.slot_for(key).unwrap();
        assert!(fresh);
        {
            let guard = UnboundRouteGuard::new(&table, slot, key);
            assert_eq!(
                table.maps.read().unwrap().by_slot.get(&slot).copied(),
                Some(key),
                "guard construction must not remove the binding"
            );
            drop(guard); // armed
        }
        assert_eq!(
            table.resolve(slot),
            None,
            "armed guard drop must roll back the fresh allocation"
        );
        let (retry_slot, retry_fresh) = table.slot_for(key).unwrap();
        assert!(retry_fresh, "rolled-back route re-binds fresh");
        assert_ne!(retry_slot, slot);

        // Disarmed drop -> binding stays.
        let (slot2, _) = table.slot_for(key).unwrap();
        assert_eq!(slot2, retry_slot);
        {
            let mut guard = UnboundRouteGuard::new(&table, slot2, key);
            guard.disarm();
            drop(guard); // disarmed
        }
        assert_eq!(
            table.published_slot_for(key),
            Some(slot2),
            "disarmed guard must leave the binding intact"
        );
    }
}
