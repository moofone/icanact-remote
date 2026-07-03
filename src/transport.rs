use std::{fmt::Debug, io, net::SocketAddr, sync::Arc, sync::OnceLock};

use futures::future::BoxFuture;
use tokio::net as tokio_net;

use crate::{
    GossipConfig, GossipNodeId, PeerId, Result, SecretKey, config::ConnectionRecoveryPolicy,
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
    pub node_id: Option<GossipNodeId>,
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
    UdpDatagram,
}

#[derive(Debug)]
pub struct TransportBootstrapArtifacts {
    pub wire_kind: TransportWireKind,
    pub bind_addr: SocketAddr,
    pub tcp_listener: Option<tokio::net::TcpListener>,
    pub udp_socket: Option<Arc<tokio_net::UdpSocket>>,
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

/// Writer contract for native datagram send paths.
pub trait TransportDatagramWriter {
    fn send_bytes(&self, datagram: bytes::Bytes) -> BoxFuture<'_, Result<()>>;

    fn send_header_and_payload16(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> BoxFuture<'_, Result<()>>;

    fn send_header_and_payload32(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> BoxFuture<'_, Result<()>>;

    fn try_send_header_and_payload16(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()>;

    fn try_send_header_and_payload32(&self, header: [u8; 32], payload: bytes::Bytes) -> Result<()>;

    fn send_header_prefix_pooled(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        payload: crate::typed::PooledPayload,
    ) -> BoxFuture<'_, Result<()>>;

    fn try_send_header_prefix_pooled(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()>;

    fn try_send_pooled_datagram(&self, datagram: crate::typed::PooledPayload) -> Result<()>;

    fn send_bytes_vectored(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> BoxFuture<'_, Result<()>>;

    fn try_send_chunks(&self, chunks: &[bytes::Bytes]) -> Result<()>;
}

pub trait TransportDatagramWriterDyn: TransportDatagramWriter + Debug + Send + Sync {}

impl<T> TransportDatagramWriterDyn for T where T: TransportDatagramWriter + Debug + Send + Sync {}

/// Runtime contract for direct datagram send paths.
pub trait TransportDatagramRuntime {
    type Writer: TransportDatagramWriter + Clone + Debug + Send + Sync + 'static;

    fn make_writer(
        socket: Arc<tokio_net::UdpSocket>,
        peer_addr: SocketAddr,
        queue_capacity: usize,
    ) -> Self::Writer;

    fn try_send_bytes_to_addr(
        socket: &tokio_net::UdpSocket,
        addr: SocketAddr,
        data: bytes::Bytes,
    ) -> Result<()>;

    fn try_send_parts_to_addr(
        socket: &tokio_net::UdpSocket,
        addr: SocketAddr,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()>;
}

type MakeWriterFn =
    fn(Arc<tokio_net::UdpSocket>, SocketAddr, usize) -> Arc<dyn TransportDatagramWriterDyn>;
type TrySendBytesToAddrFn = fn(&tokio_net::UdpSocket, SocketAddr, bytes::Bytes) -> Result<()>;
type TrySendPartsToAddrFn =
    fn(&tokio_net::UdpSocket, SocketAddr, bytes::Bytes, bytes::Bytes) -> Result<()>;

#[derive(Clone, Copy)]
struct DatagramRuntimeHooks {
    make_writer: MakeWriterFn,
    try_send_bytes_to_addr: TrySendBytesToAddrFn,
    try_send_parts_to_addr: TrySendPartsToAddrFn,
}

static DATAGRAM_RUNTIME_HOOKS: OnceLock<DatagramRuntimeHooks> = OnceLock::new();

fn datagram_runtime_not_installed_error() -> crate::GossipError {
    crate::GossipError::InvalidConfig(
        "UDP/datagram runtime hooks are not installed for this process".to_string(),
    )
}

#[derive(Debug, Clone, Copy, Default)]
struct UnconfiguredDatagramWriter;

impl TransportDatagramWriter for UnconfiguredDatagramWriter {
    fn send_bytes(&self, _datagram: bytes::Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(datagram_runtime_not_installed_error()) })
    }

    fn send_header_and_payload16(
        &self,
        _header: [u8; 16],
        _header_len: u8,
        _payload: bytes::Bytes,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(datagram_runtime_not_installed_error()) })
    }

    fn send_header_and_payload32(
        &self,
        _header: [u8; 32],
        _payload: bytes::Bytes,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(datagram_runtime_not_installed_error()) })
    }

    fn try_send_header_and_payload16(
        &self,
        _header: [u8; 16],
        _header_len: u8,
        _payload: bytes::Bytes,
    ) -> Result<()> {
        Err(datagram_runtime_not_installed_error())
    }

    fn try_send_header_and_payload32(
        &self,
        _header: [u8; 32],
        _payload: bytes::Bytes,
    ) -> Result<()> {
        Err(datagram_runtime_not_installed_error())
    }

    fn send_header_prefix_pooled(
        &self,
        _header: [u8; 16],
        _header_len: u8,
        _prefix: Option<[u8; 16]>,
        _payload: crate::typed::PooledPayload,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(datagram_runtime_not_installed_error()) })
    }

    fn try_send_header_prefix_pooled(
        &self,
        _header: [u8; 16],
        _header_len: u8,
        _prefix: Option<[u8; 16]>,
        _payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        Err(datagram_runtime_not_installed_error())
    }

    fn try_send_pooled_datagram(&self, _datagram: crate::typed::PooledPayload) -> Result<()> {
        Err(datagram_runtime_not_installed_error())
    }

    fn send_bytes_vectored(
        &self,
        _header: bytes::Bytes,
        _payload: bytes::Bytes,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(datagram_runtime_not_installed_error()) })
    }

    fn try_send_chunks(&self, _chunks: &[bytes::Bytes]) -> Result<()> {
        Err(datagram_runtime_not_installed_error())
    }
}

fn make_writer_for_runtime<R: TransportDatagramRuntime>(
    socket: Arc<tokio_net::UdpSocket>,
    peer_addr: SocketAddr,
    queue_capacity: usize,
) -> Arc<dyn TransportDatagramWriterDyn> {
    Arc::new(R::make_writer(socket, peer_addr, queue_capacity))
}

fn try_send_bytes_to_addr_for_runtime<R: TransportDatagramRuntime>(
    socket: &tokio_net::UdpSocket,
    addr: SocketAddr,
    data: bytes::Bytes,
) -> Result<()> {
    R::try_send_bytes_to_addr(socket, addr, data)
}

fn try_send_parts_to_addr_for_runtime<R: TransportDatagramRuntime>(
    socket: &tokio_net::UdpSocket,
    addr: SocketAddr,
    header: bytes::Bytes,
    payload: bytes::Bytes,
) -> Result<()> {
    R::try_send_parts_to_addr(socket, addr, header, payload)
}

pub fn install_datagram_runtime<R: TransportDatagramRuntime>() {
    let _ = DATAGRAM_RUNTIME_HOOKS.set(DatagramRuntimeHooks {
        make_writer: make_writer_for_runtime::<R>,
        try_send_bytes_to_addr: try_send_bytes_to_addr_for_runtime::<R>,
        try_send_parts_to_addr: try_send_parts_to_addr_for_runtime::<R>,
    });
}

pub(crate) fn make_datagram_writer(
    socket: Arc<tokio_net::UdpSocket>,
    peer_addr: SocketAddr,
    queue_capacity: usize,
) -> Arc<dyn TransportDatagramWriterDyn> {
    if let Some(hooks) = DATAGRAM_RUNTIME_HOOKS.get() {
        (hooks.make_writer)(socket, peer_addr, queue_capacity)
    } else {
        Arc::new(UnconfiguredDatagramWriter)
    }
}

pub(crate) fn try_send_bytes_to_addr(
    socket: &tokio_net::UdpSocket,
    addr: SocketAddr,
    data: bytes::Bytes,
) -> Result<()> {
    if let Some(hooks) = DATAGRAM_RUNTIME_HOOKS.get() {
        (hooks.try_send_bytes_to_addr)(socket, addr, data)
    } else {
        Err(datagram_runtime_not_installed_error())
    }
}

pub(crate) fn try_send_parts_to_addr(
    socket: &tokio_net::UdpSocket,
    addr: SocketAddr,
    header: bytes::Bytes,
    payload: bytes::Bytes,
) -> Result<()> {
    if let Some(hooks) = DATAGRAM_RUNTIME_HOOKS.get() {
        (hooks.try_send_parts_to_addr)(socket, addr, header, payload)
    } else {
        Err(datagram_runtime_not_installed_error())
    }
}
