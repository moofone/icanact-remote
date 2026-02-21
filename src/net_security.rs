use std::net::{IpAddr, Ipv6Addr, SocketAddr};

/// Address-level SSRF/bogon filtering used by peer discovery and gossip ingestion.
///
/// This is intentionally allocation-free and cheap (pure IP checks).
#[inline]
pub(crate) fn is_safe_to_dial(
    addr: &SocketAddr,
    allow_private: bool,
    allow_loopback: bool,
    allow_link_local: bool,
) -> bool {
    match addr.ip() {
        IpAddr::V4(ipv4) => {
            // Loopback (127.0.0.0/8)
            if ipv4.is_loopback() && !allow_loopback {
                return false;
            }

            // Link-local (169.254.0.0/16)
            if ipv4.is_link_local() && !allow_link_local {
                return false;
            }

            // Private (RFC1918)
            if ipv4.is_private() && !allow_private {
                return false;
            }

            // Unspecified (0.0.0.0)
            if ipv4.is_unspecified() {
                return false;
            }

            // Broadcast (255.255.255.255)
            if ipv4.is_broadcast() {
                return false;
            }

            // Documentation ranges (RFC5737)
            if ipv4.is_documentation() {
                return false;
            }

            true
        }
        IpAddr::V6(ipv6) => is_safe_ipv6(&ipv6, allow_private, allow_loopback, allow_link_local),
    }
}

#[inline]
fn is_safe_ipv6(
    ipv6: &Ipv6Addr,
    allow_private: bool,
    allow_loopback: bool,
    allow_link_local: bool,
) -> bool {
    if ipv6.is_loopback() && !allow_loopback {
        return false;
    }

    if ipv6.is_unspecified() {
        return false;
    }

    if ipv6.is_unicast_link_local() && !allow_link_local {
        return false;
    }

    // Unique local addresses (fc00::/7)
    if ipv6.is_unique_local() && !allow_private {
        return false;
    }

    true
}
