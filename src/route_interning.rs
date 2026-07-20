//! Connection-scoped ActorAsk route slots.
//!
//! A table belongs to exactly one transport instance. Dropping that instance
//! drops all bindings, so a slot from a previous connection can never become
//! valid after reconnect.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;

/// A peer cannot grow route state beyond this per-connection limit.
pub(crate) const MAX_ROUTES_PER_CONNECTION: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RouteKey {
    pub(crate) actor_id: u64,
    pub(crate) type_hash: u32,
}

#[derive(Debug, Default)]
pub(crate) struct RouteTable {
    next_slot: AtomicU32,
    max_routes: usize,
    maps: RwLock<RouteMaps>,
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
            ..Self::default()
        }
    }

    /// Allocates once per route for this connection. Zero is never a route.
    pub(crate) fn slot_for(&self, key: RouteKey) -> Option<(u32, bool)> {
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
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        if slot == 0 || slot == u32::MAX { return None; }
        maps.by_slot.insert(slot, key);
        maps.by_key.insert(key, slot);
        Some((slot, true))
    }

    /// Receiver-side bind. Conflicting rebinding is rejected fail-closed.
    pub(crate) fn bind(&self, slot: u32, key: RouteKey) -> bool {
        if slot == 0 { return false; }
        let mut maps = match self.maps.write() { Ok(value) => value, Err(_) => return false };
        match maps.by_slot.get(&slot) {
            Some(existing) if *existing != key => false,
            Some(_) => true,
            None => {
                if maps.by_key.len() >= self.max_routes {
                    return false;
                }
                if maps.by_key.get(&key).is_some_and(|existing| *existing != slot) {
                    return false;
                }
                maps.by_slot.insert(slot, key);
                maps.by_key.insert(key, slot);
                true
            }
        }
    }

    pub(crate) fn resolve(&self, slot: u32) -> Option<RouteKey> {
        self.maps.read().ok()?.by_slot.get(&slot).copied()
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
    fn route_slots_bind_once_and_reject_conflicts() {
        let table = RouteTable::new();
        let first = RouteKey { actor_id: 7, type_hash: 9 };
        let second = RouteKey { actor_id: 8, type_hash: 9 };
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
        let (slot, _) = old.slot_for(RouteKey { actor_id: 7, type_hash: 9 }).unwrap();
        let new_connection = RouteTable::new();
        assert_eq!(new_connection.resolve(slot), None);
    }

    #[test]
    fn failed_first_bind_can_be_rolled_back_for_a_safe_retry() {
        let table = RouteTable::new();
        let key = RouteKey { actor_id: 7, type_hash: 9 };
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
        let first = RouteKey { actor_id: 7, type_hash: 9 };
        let second = RouteKey { actor_id: 8, type_hash: 9 };
        assert!(table.slot_for(first).is_some());
        assert_eq!(table.slot_for(second), None);
        assert!(!table.bind(2, second));
        assert!(table.bind(1, first), "existing binding remains idempotent");
    }

    /// R-3: a dropped-while-armed `UnboundRouteGuard` rolls back the fresh
    /// allocation; a disarmed guard leaves the binding intact. This is the
    /// cancel-safety primitive for `write_routed_actor_ask`'s bind enqueue.
    #[test]
    fn qa_r3_unbound_route_guard_rolls_back_only_while_armed() {
        let table = RouteTable::new();
        let key = RouteKey { actor_id: 7, type_hash: 9 };

        // Armed drop -> rolled back.
        let (slot, fresh) = table.slot_for(key).unwrap();
        assert!(fresh);
        {
            let guard = UnboundRouteGuard::new(&table, slot, key);
            assert_eq!(
                table.resolve(slot),
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
            table.resolve(slot2),
            Some(key),
            "disarmed guard must leave the binding intact"
        );
    }
}
