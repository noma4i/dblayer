use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub(super) fn allowed(origin: &str) -> bool {
    let (secure, authority) = match (origin.strip_prefix("https://"), origin.strip_prefix("http://")) {
        (Some(rest), _) => (true, rest),
        (_, Some(rest)) => (false, rest),
        _ => return false,
    };
    if authority.contains(|c: char| matches!(c, '/' | '?' | '#' | '@' | '\\') || c.is_whitespace()) {
        return false;
    }
    let Some((host, port)) = split_port(authority) else { return false };
    let port_ok = port.is_none_or(|port| !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) && port.parse::<u16>().is_ok());
    port_ok && !host.is_empty() && (secure || development_host(host))
}

fn split_port(authority: &str) -> Option<(&str, Option<&str>)> {
    match authority.strip_prefix('[') {
        Some(rest) => {
            let (host, tail) = rest.split_once(']')?;
            match tail {
                "" => Some((host, None)),
                _ => Some((host, Some(tail.strip_prefix(':')?))),
            }
        }
        None => Some(authority.split_once(':').map_or((authority, None), |(host, port)| (host, Some(port)))),
    }
}

fn development_host(host: &str) -> bool {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => development_v4(address),
        Ok(IpAddr::V6(address)) => development_v6(address),
        Err(_) => development_name(host),
    }
}

fn development_v4(address: Ipv4Addr) -> bool {
    address.is_loopback() || address.is_private() || address.is_link_local()
}

fn development_v6(address: Ipv6Addr) -> bool {
    match address.to_ipv4_mapped() {
        Some(mapped) => development_v4(mapped),
        None => address.is_loopback() || (address.segments()[0] & 0xfe00) == 0xfc00 || (address.segments()[0] & 0xffc0) == 0xfe80,
    }
}

fn development_name(host: &str) -> bool {
    let labels_ok = host.split('.').all(|label| !label.is_empty() && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'));
    labels_ok && (host.eq_ignore_ascii_case("localhost") || host.len() > ".local".len() && host[host.len() - ".local".len()..].eq_ignore_ascii_case(".local"))
}
