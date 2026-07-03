use std::{io, net::SocketAddr, sync::Arc};

use futures::future::BoxFuture;

use crate::{
    GossipConfig, NodeId, PeerId, Result, SecretKey, config::ConnectionRecoveryPolicy,
    handshake::PeerCapabilities, registry::GossipRegistry,
};

#[derive(Debug, Clone, Copy)]
pub struct RemoteAddrMeta {
    pub peer_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy)]
pub struct TargetAddr {
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Default)]
pub struct AuthContext {
    pub peer_id: Option<PeerId>,
    pub node_id: Option<NodeId>,
    pub session_binding: Option<[u8; 32]>,
    pub capabilities: Option<PeerCapabilities>,
}

pub trait TransportListener {
    type Conn: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static;

    fn accept(&self) -> BoxFuture<'_, io::Result<(Self::Conn, RemoteAddrMeta)>>;
}

pub trait TransportConnector {
    type Conn: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static;

    fn connect(&self, target: TargetAddr) -> BoxFuture<'_, io::Result<Self::Conn>>;
}

pub trait PeerAuthenticator {
    type Conn: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static;
    type AuthContext;

    fn authenticate_inbound(
        &self,
        conn: Self::Conn,
    ) -> BoxFuture<'_, io::Result<(Self::Conn, Self::AuthContext)>>;

    fn authenticate_outbound(
        &self,
        conn: Self::Conn,
        _expected_peer: PeerId,
    ) -> BoxFuture<'_, io::Result<(Self::Conn, Self::AuthContext)>>;
}

pub trait TransportStack {
    type Listener: TransportListener<Conn = Self::Conn>;
    type Connector: TransportConnector<Conn = Self::Conn>;
    type Conn: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static;
    type Auth: PeerAuthenticator<Conn = Self::Conn, AuthContext = AuthContext>;

    fn listener(&self, bind_addr: SocketAddr) -> io::Result<Self::Listener>;
    fn connector(&self) -> Self::Connector;
    fn authenticator(&self) -> &Self::Auth;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportWireKind {
    TcpStream,
}

#[derive(Debug)]
pub struct TransportBootstrapArtifacts {
    pub wire_kind: TransportWireKind,
    pub bind_addr: SocketAddr,
    pub tcp_listener: Option<tokio::net::TcpListener>,
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

/// Transport runtime lifecycle hooks kept out of hot paths.
pub trait TransportRuntime {
    fn maybe_tick_housekeeping(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Transport server launcher contract.
pub trait TransportServer {
    fn spawn_server(
        &self,
        registry: Arc<GossipRegistry>,
        artifacts: TransportBootstrapArtifacts,
    ) -> Result<tokio::task::JoinHandle<()>>;
}
