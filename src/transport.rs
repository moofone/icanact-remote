use crate::{
    GossipConfig, Result, SecretKey, config::ConnectionRecoveryPolicy, registry::GossipRegistry,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportWireKind {
    TcpStream,
}

/// Core bootstrap contract used by `GossipRegistryHandle::new_with_transport_stack`.
pub trait RegistryTransportBootstrap {
    fn stack_name(&self) -> &'static str;
    fn wire_kind(&self) -> TransportWireKind {
        TransportWireKind::TcpStream
    }
    fn connection_recovery_policy(&self) -> Option<ConnectionRecoveryPolicy> {
        None
    }
    fn prepare_config(&self, secret_key: &SecretKey, config: &mut GossipConfig) -> Result<()>;
    fn configure_registry(
        &self,
        registry: &mut GossipRegistry,
        secret_key: SecretKey,
    ) -> Result<()>;
}
