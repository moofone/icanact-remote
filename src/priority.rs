use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// Priority levels for actor registrations
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Default,
)]
pub enum RegistrationPriority {
    /// Normal priority - uses standard gossip intervals
    #[default]
    Normal = 0,
    /// Immediate priority - triggers immediate propagation
    Immediate = 1,
}

impl RegistrationPriority {
    /// Returns true if this priority should trigger immediate gossip
    pub fn should_trigger_immediate_gossip(&self) -> bool {
        matches!(self, RegistrationPriority::Immediate)
    }

    /// Returns true if this priority is critical and needs maximum propagation
    pub fn is_critical(&self) -> bool {
        matches!(self, RegistrationPriority::Immediate)
    }

    /// Returns the gossip fanout multiplier for this priority
    pub fn fanout_multiplier(&self) -> f64 {
        match self {
            RegistrationPriority::Normal => 1.0,
            RegistrationPriority::Immediate => 2.0,
        }
    }

    /// Returns the retry count for immediate propagation
    pub fn immediate_retry_count(&self) -> usize {
        match self {
            RegistrationPriority::Normal => 0,
            RegistrationPriority::Immediate => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registration_priority_default() {
        assert_eq!(
            RegistrationPriority::default(),
            RegistrationPriority::Normal
        );
    }

    #[test]
    fn test_registration_priority_ordering() {
        assert!(RegistrationPriority::Normal < RegistrationPriority::Immediate);
        assert!(RegistrationPriority::Immediate > RegistrationPriority::Normal);
    }

    #[test]
    fn test_registration_priority_should_trigger_immediate_gossip() {
        assert!(!RegistrationPriority::Normal.should_trigger_immediate_gossip());
        assert!(RegistrationPriority::Immediate.should_trigger_immediate_gossip());
    }

    #[test]
    fn test_registration_priority_is_critical() {
        assert!(!RegistrationPriority::Normal.is_critical());
        assert!(RegistrationPriority::Immediate.is_critical());
    }

    #[test]
    fn test_registration_priority_fanout_multiplier() {
        assert_eq!(RegistrationPriority::Normal.fanout_multiplier(), 1.0);
        assert_eq!(RegistrationPriority::Immediate.fanout_multiplier(), 2.0);
    }

    #[test]
    fn test_registration_priority_immediate_retry_count() {
        assert_eq!(RegistrationPriority::Normal.immediate_retry_count(), 0);
        assert_eq!(RegistrationPriority::Immediate.immediate_retry_count(), 3);
    }

    #[test]
    fn test_registration_priority_serialization() {
        let priority = RegistrationPriority::Immediate;
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&priority).unwrap();
        let deserialized: RegistrationPriority =
            rkyv::from_bytes::<RegistrationPriority, rkyv::rancor::Error>(&serialized).unwrap(); // ALLOW_RKYV_FROM_BYTES
        assert_eq!(priority, deserialized);
    }

    #[test]
    fn test_registration_priority_debug() {
        let priority = RegistrationPriority::Immediate;
        let debug_str = format!("{:?}", priority);
        assert_eq!(debug_str, "Immediate");
    }

    #[test]
    fn test_registration_priority_clone() {
        let priority = RegistrationPriority::Immediate;
        let cloned = priority;
        assert_eq!(priority, cloned);
    }
}
