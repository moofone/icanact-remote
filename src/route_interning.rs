//! Connection-scoped ActorAsk route slots.
//!
//! A table belongs to exactly one transport instance. Dropping that instance
//! drops all bindings, so a slot from a previous connection can never become
//! valid after reconnect.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RouteKey {
    pub(crate) actor_id: u64,
    pub(crate) type_hash: u32,
}

#[derive(Debug, Default)]
pub(crate) struct RouteTable {
    next_slot: AtomicU32,
    by_key: RwLock<HashMap<RouteKey, u32>>,
    by_slot: RwLock<HashMap<u32, RouteKey>>,
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        Self { next_slot: AtomicU32::new(1), ..Self::default() }
    }

    /// Allocates once per route for this connection. Zero is never a route.
    pub(crate) fn slot_for(&self, key: RouteKey) -> Option<(u32, bool)> {
        if let Some(slot) = self.by_key.read().ok()?.get(&key).copied() {
            return Some((slot, false));
        }
        let mut by_key = self.by_key.write().ok()?;
        if let Some(slot) = by_key.get(&key).copied() {
            return Some((slot, false));
        }
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        if slot == 0 || slot == u32::MAX { return None; }
        self.by_slot.write().ok()?.insert(slot, key);
        by_key.insert(key, slot);
        Some((slot, true))
    }

    /// Receiver-side bind. Conflicting rebinding is rejected fail-closed.
    pub(crate) fn bind(&self, slot: u32, key: RouteKey) -> bool {
        if slot == 0 { return false; }
        let mut by_slot = match self.by_slot.write() { Ok(value) => value, Err(_) => return false };
        match by_slot.get(&slot) {
            Some(existing) if *existing != key => false,
            Some(_) => true,
            None => {
                by_slot.insert(slot, key);
                self.by_key.write().map(|mut map| { map.insert(key, slot); }).is_ok()
            }
        }
    }

    pub(crate) fn resolve(&self, slot: u32) -> Option<RouteKey> {
        self.by_slot.read().ok()?.get(&slot).copied()
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
        assert_eq!(table.resolve(slot), Some(first));
    }

    #[test]
    fn reconnect_starts_with_no_stale_slots() {
        let old = RouteTable::new();
        let (slot, _) = old.slot_for(RouteKey { actor_id: 7, type_hash: 9 }).unwrap();
        let new_connection = RouteTable::new();
        assert_eq!(new_connection.resolve(slot), None);
    }
}
