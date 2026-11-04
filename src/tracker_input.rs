//! Port of Tracker/tracker_input.cpp + tracker_input.h.

use crate::bt_misc::to_int;

pub const E_NONE: i64 = 0;
pub const E_COMPLETED: i64 = 1;
pub const E_STARTED: i64 = 2;
pub const E_STOPPED: i64 = 3;

#[derive(Clone)]
pub struct TrackerInput {
    pub event: i64,
    /// raw 20 bytes; empty when absent/invalid (std::string info_hash_)
    pub info_hash: Vec<u8>,
    /// every valid 20-byte info_hash parameter, in order (scrape)
    pub info_hashes: Vec<Vec<u8>>,
    /// always exactly 20 bytes (std::array<char, 20> peer_id_)
    pub peer_id: [u8; 20],
    pub downloaded: i64,
    pub left: i64,
    pub uploaded: i64,
    /// sockaddr_in6 address bytes of the client (::ffff:a.b.c.d for IPv4)
    pub ipv6: [u8; 16],
    /// host-order port value (the original stores htons(to_int(port)) and
    /// ntohs()es it back everywhere; same observable behavior)
    pub port: u16,
}

impl Default for TrackerInput {
    fn default() -> Self {
        TrackerInput {
            event: E_NONE,
            info_hash: Vec::new(),
            info_hashes: Vec::new(),
            peer_id: [0; 20],
            downloaded: 0,
            left: 0,
            uploaded: 0,
            ipv6: [0; 16],
            port: 0,
        }
    }
}

impl TrackerInput {
    /// Port of `tracker_input_t::set(std::string_view, std::string_view)`.
    pub fn set(&mut self, name: &[u8], value: &[u8]) {
        if name.is_empty() {
            return;
        }
        match name[0] {
            b'd' => {
                if name == b"downloaded" {
                    self.downloaded = to_int(value);
                }
            }
            b'e' => {
                if name == b"event" {
                    self.event = if value == b"completed" {
                        E_COMPLETED
                    } else if value == b"started" {
                        E_STARTED
                    } else if value == b"stopped" {
                        E_STOPPED
                    } else {
                        E_NONE
                    };
                }
            }
            b'i' => {
                if name == b"info_hash" && value.len() == 20 {
                    self.info_hash = value.to_vec();
                    self.info_hashes.push(value.to_vec());
                } else if name == b"ip" {
                    // inet_pton(AF_INET, ...) into bytes 12..16, else
                    // inet_pton(AF_INET6, ...); failures go to syslog.
                    let s = String::from_utf8_lossy(value);
                    if let Ok(v4) = s.parse::<std::net::Ipv4Addr>() {
                        // inet_pton(AF_INET, ...) succeeded: mapped into 12..16.
                        // (The original overwrites ti.ipv6_ with the socket
                        // address right after parsing, so ip= is effectively
                        // dead for HTTP; kept for parity.)
                        self.ipv6 = [0; 16];
                        self.ipv6[10] = 0xff;
                        self.ipv6[11] = 0xff;
                        self.ipv6[12..16].copy_from_slice(&v4.octets());
                    } else if let Ok(v6) = s.parse::<std::net::Ipv6Addr>() {
                        self.ipv6 = v6.octets();
                    } else {
                        crate::bt_misc::xbt_syslog(&format!("inet_pton failed: {}", s));
                    }
                }
            }
            b'l' => {
                if name == b"left" {
                    self.left = to_int(value);
                }
            }
            b'p' => {
                if name == b"peer_id" && value.len() == 20 {
                    self.peer_id.copy_from_slice(value);
                } else if name == b"port" {
                    // htons(to_int(value)): truncated to 16 bits
                    self.port = to_int(value) as u16;
                }
            }
            b'u' => {
                if name == b"uploaded" {
                    self.uploaded = to_int(value);
                }
            }
            _ => {}
        }
    }

    /// Port of `tracker_input_t::valid()`. Note: peer_id_ and port_ always
    /// satisfy their checks in the original (fixed-size array; non-negative
    /// int), so they are not re-checked here either.
    pub fn valid(&self) -> bool {
        self.downloaded >= 0
            && (self.event != E_COMPLETED || self.left == 0)
            && self.info_hash.len() == 20
            && self.left >= -1
            && self.uploaded >= 0
    }

    pub fn is_seeder(&self) -> bool {
        self.left == 0
    }

    #[allow(dead_code)]
    pub fn is_leecher(&self) -> bool {
        self.left != 0
    }
}

/// Port of `is_ipv4(std::array<unsigned char, 16>)`.
pub fn is_ipv4(v: &[u8; 16]) -> bool {
    v[0] == 0
        && v[1] == 0
        && v[2] == 0
        && v[3] == 0
        && v[4] == 0
        && v[5] == 0
        && v[6] == 0
        && v[7] == 0
        && v[8] == 0
        && v[9] == 0
        && v[10] == 0xff
        && v[11] == 0xff
}

/// Port of `is_zero(std::array<unsigned char, N>)`.
pub fn is_zero(v: &[u8]) -> bool {
    v.iter().all(|&c| c == 0)
}

/// Port of `to_sql(std::array<unsigned char, 16>)`: 4 bytes for IPv4-mapped
/// addresses, the full 16 bytes otherwise.
pub fn to_sql(v: &[u8; 16]) -> &[u8] {
    if is_ipv4(v) {
        &v[12..]
    } else {
        &v[..]
    }
}

/// Build the 16-byte sockaddr_in6-style address representation the original
/// gets from its dual-stack sockets.
pub fn addr_to_ipv6_bytes(addr: &std::net::SocketAddr) -> [u8; 16] {
    match addr {
        std::net::SocketAddr::V4(a) => {
            let mut v = [0u8; 16];
            v[10] = 0xff;
            v[11] = 0xff;
            v[12..16].copy_from_slice(&a.ip().octets());
            v
        }
        std::net::SocketAddr::V6(a) => a.ip().octets(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_valid() {
        let mut ti = TrackerInput::default();
        assert!(!ti.valid()); // no info_hash
        ti.set(b"info_hash", &[0xab; 20]);
        ti.set(b"peer_id", b"-UT355W-0123456789ab");
        ti.set(b"port", b"6881");
        ti.set(b"uploaded", b"10");
        ti.set(b"downloaded", b"20");
        ti.set(b"left", b"30");
        ti.set(b"event", b"started");
        assert!(ti.valid());
        assert_eq!(ti.port, 6881);
        assert_eq!(ti.event, E_STARTED);
        assert_eq!(ti.info_hashes.len(), 1);
        // completed with left != 0 -> invalid
        ti.set(b"event", b"completed");
        assert!(!ti.valid());
        ti.set(b"left", b"0");
        assert!(ti.valid());
        // unknown event -> none
        ti.set(b"event", b"bogus");
        assert_eq!(ti.event, E_NONE);
        // left = -1 valid, -2 invalid
        ti.set(b"left", b"-1");
        assert!(ti.valid());
        ti.set(b"left", b"-2");
        assert!(!ti.valid());
        // negative downloaded invalid
        ti.set(b"left", b"0");
        ti.set(b"downloaded", b"-5");
        assert!(!ti.valid());
        // bad info_hash size ignored
        let mut t2 = TrackerInput::default();
        t2.set(b"info_hash", b"short");
        assert!(t2.info_hash.is_empty());
        assert!(t2.info_hashes.is_empty());
    }

    #[test]
    fn test_port_truncation() {
        let mut ti = TrackerInput::default();
        ti.set(b"port", b"99999"); // htons(to_int): 99999 & 0xffff
        assert_eq!(ti.port, (99999u32 & 0xffff) as u16);
        ti.set(b"port", b"-1");
        assert_eq!(ti.port, 65535);
        ti.set(b"port", b"bogus");
        assert_eq!(ti.port, 0);
    }

    #[test]
    fn test_ipv4_helpers() {
        let v4 = addr_to_ipv6_bytes(&"127.0.0.1:1".parse().unwrap());
        assert!(is_ipv4(&v4));
        assert_eq!(to_sql(&v4), &[127, 0, 0, 1]);
        let v6 = addr_to_ipv6_bytes(&"[2001:db8::1]:1".parse().unwrap());
        assert!(!is_ipv4(&v6));
        assert_eq!(to_sql(&v6).len(), 16);
        assert!(is_zero(&[0u8; 4]));
        assert!(!is_zero(&[0u8, 1]));
    }
}
