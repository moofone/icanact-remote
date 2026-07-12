use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

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
        IpAddr::V4(ipv4) => is_safe_ipv4(&ipv4, allow_private, allow_loopback, allow_link_local),
        IpAddr::V6(ipv6) => is_safe_ipv6(&ipv6, allow_private, allow_loopback, allow_link_local),
    }
}

#[inline]
fn is_safe_ipv4(
    ipv4: &Ipv4Addr,
    allow_private: bool,
    allow_loopback: bool,
    allow_link_local: bool,
) -> bool {
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

#[inline]
fn is_safe_ipv6(
    ipv6: &Ipv6Addr,
    allow_private: bool,
    allow_loopback: bool,
    allow_link_local: bool,
) -> bool {
    // ACTOR_REM_2 R3: canonicalize IPv4-mapped IPv6 (::ffff:a.b.c.d) so a
    // v4 loopback/link-local/private address smuggled inside an IPv6 literal
    // cannot bypass the v4 bogon gates.
    if let Some(mapped) = ipv6.to_ipv4_mapped() {
        return is_safe_ipv4(&mapped, allow_private, allow_loopback, allow_link_local);
    }

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

    // Documentation prefix (2001:db8::/32, RFC3849) — mirror the v4 gate.
    if (ipv6.segments()[0] == 0x2001) && (ipv6.segments()[1] == 0x0db8) {
        return false;
    }

    true
}
