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

    // Multicast (224.0.0.0/4) is never a valid unicast peer target.
    if ipv4.is_multicast() {
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

    // Multicast (ff00::/8) is never a valid unicast peer target.
    if ipv6.is_multicast() {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(ip: IpAddr) -> SocketAddr {
        SocketAddr::new(ip, 443)
    }

    #[test]
    fn multicast_is_never_safe_to_dial() {
        let multicast = [
            addr(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1))),
            addr(IpAddr::V6("ff02::1".parse().expect("IPv6 multicast"))),
        ];

        for candidate in multicast {
            for allow_private in [false, true] {
                for allow_loopback in [false, true] {
                    for allow_link_local in [false, true] {
                        assert!(
                            !is_safe_to_dial(
                                &candidate,
                                allow_private,
                                allow_loopback,
                                allow_link_local,
                            ),
                            "multicast address {candidate} must remain non-dialable"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn conditional_ranges_follow_only_their_matching_flag() {
        #[derive(Clone, Copy)]
        enum Gate {
            Private,
            Loopback,
            LinkLocal,
        }

        let cases = [
            (addr(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))), Gate::Private),
            (addr(IpAddr::V4(Ipv4Addr::LOCALHOST)), Gate::Loopback),
            (
                addr(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1))),
                Gate::LinkLocal,
            ),
            (addr(IpAddr::V6(Ipv6Addr::LOCALHOST)), Gate::Loopback),
            (
                addr(IpAddr::V6("fc00::1".parse().expect("IPv6 ULA"))),
                Gate::Private,
            ),
            (
                addr(IpAddr::V6("fe80::1".parse().expect("IPv6 link-local"))),
                Gate::LinkLocal,
            ),
            (
                addr(IpAddr::V6(
                    "::ffff:10.0.0.1".parse().expect("mapped private IPv4"),
                )),
                Gate::Private,
            ),
        ];

        for (candidate, gate) in cases {
            for allow_private in [false, true] {
                for allow_loopback in [false, true] {
                    for allow_link_local in [false, true] {
                        let expected = match gate {
                            Gate::Private => allow_private,
                            Gate::Loopback => allow_loopback,
                            Gate::LinkLocal => allow_link_local,
                        };
                        assert_eq!(
                            is_safe_to_dial(
                                &candidate,
                                allow_private,
                                allow_loopback,
                                allow_link_local,
                            ),
                            expected,
                            "unexpected policy for {candidate} with private={allow_private}, loopback={allow_loopback}, link_local={allow_link_local}",
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn unconditional_bogons_remain_blocked_when_all_flags_are_enabled() {
        let bogons = [
            addr(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            addr(IpAddr::V4(Ipv4Addr::BROADCAST)),
            addr(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))),
            addr(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
            addr(IpAddr::V6(
                "2001:db8::1".parse().expect("IPv6 documentation address"),
            )),
        ];

        for candidate in bogons {
            assert!(
                !is_safe_to_dial(&candidate, true, true, true),
                "unconditional bogon {candidate} must remain non-dialable"
            );
        }
    }

    #[test]
    fn public_unicast_is_safe_with_restrictive_flags() {
        let public = [
            addr(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))),
            addr(IpAddr::V6(
                "2606:4700:4700::1111".parse().expect("public IPv6 address"),
            )),
        ];

        for candidate in public {
            assert!(is_safe_to_dial(&candidate, false, false, false));
        }
    }
}
