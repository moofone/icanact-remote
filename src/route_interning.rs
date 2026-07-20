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
    maps: RwLock<RouteMaps>,
}

#[derive(Debug, Default)]
struct RouteMaps {
    by_key: HashMap<RouteKey, u32>,
    by_slot: HashMap<u32, RouteKey>,
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        Self { next_slot: AtomicU32::new(1), ..Self::default() }
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
}
