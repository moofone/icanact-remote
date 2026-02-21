use std::{io, net::SocketAddr};

use futures::future::BoxFuture;

/// Injectable DNS resolver for deterministic tests.
///
/// This is only used on control-plane paths (reconnect / dial failure handling),
/// never on the per-message send hot path.
pub trait DnsResolver: Send + Sync + 'static {
    fn lookup<'a>(&'a self, dns: &'a str) -> BoxFuture<'a, io::Result<Vec<SocketAddr>>>;
}

#[derive(Debug, Default)]
pub struct TokioDnsResolver;

impl DnsResolver for TokioDnsResolver {
    fn lookup<'a>(&'a self, dns: &'a str) -> BoxFuture<'a, io::Result<Vec<SocketAddr>>> {
        Box::pin(async move { Ok(tokio::net::lookup_host(dns).await?.collect()) })
    }
}
