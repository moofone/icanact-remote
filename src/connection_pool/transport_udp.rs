use super::*;

impl<T> ConnectionPool<T> {
    pub(super) async fn connect_via_udp(&self, addr: SocketAddr) -> Result<ConnectionHandle<T>> {
        self.ensure_udp_connection(addr).await?;
        if let Some(handle) = self.get_existing_connection(addr) {
            return Ok(handle);
        }
        Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            format!("udp connection to {addr} was not established"),
        )))
    }
}
