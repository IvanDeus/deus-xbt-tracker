//! Port of Tracker/transaction.cpp: the UDP tracker protocol (BEP 15).
//!
//! NOTE (intentional deviation): the original passes `sizeof(sockaddr_in)`
//! as the address length to recvfrom/sendto on an AF_INET6 socket. On
//! modern Linux kernels this makes every UDP *reply* fail with EINVAL
//! (verified against the reference build), so UDP is effectively dead
//! there. This port uses the full source address, keeping every other
//! detail identical; connection_id uses the intended
//! sha1(secret_be8 || addr16)[0..8] (upstream hashes uninitialized stack
//! there — see `connection_id` docs and CHANGELOG).

use sha1::{Digest, Sha1};

use crate::stream_int::{read_int, read_int_at, write_int};
use crate::tracker::{select_peers, srv_insert_peer, Shared};
use crate::tracker_input::{addr_to_ipv6_bytes, TrackerInput};

// bt_misc.h enums
pub const UTA_CONNECT: u64 = 0;
pub const UTA_ANNOUNCE: u64 = 1;
pub const UTA_SCRAPE: u64 = 2;
pub const UTA_ERROR: u64 = 3;

pub const UTI_CONNECTION_ID: usize = 0;
pub const UTI_ACTION: usize = 8;
pub const UTI_TRANSACTION_ID: usize = 12;
pub const UTI_SIZE: usize = 16;

pub const UTIC_SIZE: usize = 16;

pub const UTIA_INFO_HASH: usize = 16;
pub const UTIA_PEER_ID: usize = 36;
pub const UTIA_DOWNLOADED: usize = 56;
pub const UTIA_LEFT: usize = 64;
pub const UTIA_UPLOADED: usize = 72;
pub const UTIA_EVENT: usize = 80;
pub const UTIA_PORT: usize = 96;
pub const UTIA_SIZE: usize = 98;

pub const UTIS_SIZE: usize = 16;

pub const UTO_ACTION: usize = 0;
pub const UTO_TRANSACTION_ID: usize = 4;

pub const UTOC_CONNECTION_ID: usize = 8;
pub const UTOC_SIZE: usize = 16;

pub const UTOA_INTERVAL: usize = 8;
pub const UTOA_LEECHERS: usize = 12;
pub const UTOA_SEEDERS: usize = 16;
pub const UTOA_SIZE: usize = 20;

pub const UTOS_SIZE: usize = 8;

pub const UTOE_SIZE: usize = 8;

pub const PROTOCOL_ID: u64 = 0x41727101980;

pub struct Ctransaction<'a> {
    pub sh: &'a Shared,
    /// m_a: source address as the original's sockaddr_in6
    pub addr16: [u8; 16],
}

impl<'a> Ctransaction<'a> {
    /// Port of `Ctransaction::connection_id()`.
    ///
    /// NOTE (intentional deviation / bug fix): the upstream IPv6 commit left
    /// `write_int(8, s, secret)` at offset 0 and then `memcpy(s, addr16, 16)`
    /// over it, hashing 8 bytes of *uninitialized stack* (UB; the pre-IPv6
    /// code hashed secret(8) || addr(4)). This port uses the clearly
    /// intended sha1(secret_be(8) || addr16)[0..8], deterministic across
    /// processes and restarts.
    pub fn connection_id(&self) -> u64 {
        let secret = self.sh.lock().secret;
        let mut s = [0u8; 24];
        write_int(&mut s, 0, 8, secret);
        s[8..].copy_from_slice(&self.addr16);
        let d = Sha1::digest(s);
        read_int(8, &d)
    }

    /// Port of `Ctransaction::recv()` (single-datagram granularity; the
    /// surrounding loop lives in the server task). Returns the response
    /// bytes to send back, if any.
    pub fn recv(&self, b: &[u8]) -> Option<Vec<u8>> {
        {
            let mut st = self.sh.lock();
            st.stats.received_udp += 1;
        }
        if b.len() < UTI_SIZE {
            return None;
        }
        match read_int_at(4, b, UTI_ACTION) {
            UTA_CONNECT => {
                if b.len() >= UTIC_SIZE {
                    self.send_connect(b)
                } else {
                    None
                }
            }
            UTA_ANNOUNCE => {
                if b.len() >= UTIA_SIZE {
                    self.send_announce(b)
                } else {
                    None
                }
            }
            UTA_SCRAPE => {
                if b.len() >= UTIS_SIZE {
                    self.send_scrape(b)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn send_connect(&self, r: &[u8]) -> Option<Vec<u8>> {
        if read_int_at(8, r, UTI_CONNECTION_ID) != PROTOCOL_ID {
            return None;
        }
        let mut d = [0u8; UTOC_SIZE];
        write_int(&mut d, UTO_ACTION, 4, UTA_CONNECT);
        write_int(&mut d, UTO_TRANSACTION_ID, 4, read_int_at(4, r, UTI_TRANSACTION_ID));
        write_int(&mut d, UTOC_CONNECTION_ID, 8, self.connection_id());
        Some(d.to_vec())
    }

    pub fn send_announce(&self, r: &[u8]) -> Option<Vec<u8>> {
        if read_int_at(8, r, UTI_CONNECTION_ID) != self.connection_id() {
            return None;
        }
        let (anonymous_announce, announce_interval) = {
            let st = self.sh.lock();
            (st.config.anonymous_announce, st.config.announce_interval)
        };
        if !anonymous_announce {
            return self.send_error(r, b"access denied");
        }
        let mut ti = TrackerInput::default();
        ti.downloaded = read_int_at(8, r, UTIA_DOWNLOADED) as i64;
        ti.event = read_int_at(4, r, UTIA_EVENT) as i64;
        ti.info_hash = r[UTIA_INFO_HASH..UTIA_INFO_HASH + 20].to_vec();
        ti.ipv6 = self.addr16;
        ti.left = read_int_at(8, r, UTIA_LEFT) as i64;
        ti.peer_id.copy_from_slice(&r[UTIA_PEER_ID..UTIA_PEER_ID + 20]);
        ti.port = read_int_at(2, r, UTIA_PORT) as u16;
        ti.uploaded = read_int_at(8, r, UTIA_UPLOADED) as i64;
        let (error, resp, execs) = {
            let mut st = self.sh.lock();
            let mut execs = Vec::new();
            let error = srv_insert_peer(&mut st, &ti, true, None, &mut execs);
            let resp = if error.is_empty() {
                match st.find_torrent(&ti.info_hash) {
                    None => None,
                    Some(torrent) => {
                        let mut d = vec![0u8; UTOA_SIZE];
                        write_int(&mut d, UTO_ACTION, 4, UTA_ANNOUNCE);
                        write_int(
                            &mut d,
                            UTO_TRANSACTION_ID,
                            4,
                            read_int_at(4, r, UTI_TRANSACTION_ID),
                        );
                        write_int(&mut d, UTOA_INTERVAL, 4, announce_interval as u64);
                        write_int(&mut d, UTOA_LEECHERS, 4, torrent.leechers as u64);
                        write_int(&mut d, UTOA_SEEDERS, 4, torrent.seeders as u64);
                        // the original uses select_peers (IPv4 records) for UDP
                        // announces regardless of the announcer's family
                        let peers = select_peers(torrent, &ti);
                        let mut out = d;
                        out.extend_from_slice(&peers);
                        Some(out)
                    }
                }
            } else {
                None
            };
            (error, resp, execs)
        };
        // fire inline DB flushes produced by srv_insert_peer
        for sql in execs {
            self.sh.db.exec_fire(sql);
        }
        if !error.is_empty() {
            return self.send_error(r, error.as_bytes());
        }
        resp
    }

    pub fn send_scrape(&self, r: &[u8]) -> Option<Vec<u8>> {
        if read_int_at(8, r, UTI_CONNECTION_ID) != self.connection_id() {
            return None;
        }
        let anonymous_scrape = self.sh.lock().config.anonymous_scrape;
        if !anonymous_scrape {
            return self.send_error(r, b"access denied");
        }
        const CB_D: usize = 2 << 10;
        let mut d = vec![0u8; CB_D];
        write_int(&mut d, UTO_ACTION, 4, UTA_SCRAPE);
        write_int(
            &mut d,
            UTO_TRANSACTION_ID,
            4,
            read_int_at(4, r, UTI_TRANSACTION_ID),
        );
        let mut w = UTOS_SIZE;
        let mut off = UTIS_SIZE;
        {
            let st = self.sh.lock();
            while r.len() - off >= 20 && w + 12 <= CB_D {
                let (seeders, completed, leechers) = match st.find_torrent(&r[off..off + 20]) {
                    Some(t) => (t.seeders, t.completed, t.leechers),
                    None => (0, 0, 0),
                };
                write_int(&mut d, w, 4, seeders as u64);
                write_int(&mut d, w + 4, 4, completed as u64);
                write_int(&mut d, w + 8, 4, leechers as u64);
                w += 12;
                off += 20;
            }
        }
        self.sh.lock().stats.scraped_udp += 1;
        d.truncate(w);
        Some(d)
    }

    pub fn send_error(&self, r: &[u8], msg: &[u8]) -> Option<Vec<u8>> {
        let mut d = vec![0u8; UTOE_SIZE + msg.len()];
        write_int(&mut d, UTO_ACTION, 4, UTA_ERROR);
        write_int(
            &mut d,
            UTO_TRANSACTION_ID,
            4,
            read_int_at(4, r, UTI_TRANSACTION_ID),
        );
        d[UTOE_SIZE..].copy_from_slice(msg);
        Some(d)
    }
}

/// Helpers used by the UDP server task.
pub fn udp_addr16(addr: &std::net::SocketAddr) -> [u8; 16] {
    addr_to_ipv6_bytes(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Db;
    use crate::tracker::State;
    use std::sync::Mutex;

    fn shared(secret: u64) -> Shared {
        let mut st = State::new("test.conf".into(), "xbt_".into(), secret);
        st.config.anonymous_announce = true;
        st.config.anonymous_scrape = true;
        st.config.auto_register = true;
        Shared {
            state: Mutex::new(st),
            db: Db::dummy(),
            access_log: Mutex::new(None),
        }
    }

    fn tx<'a>(sh: &'a Shared, addr: &str) -> Ctransaction<'a> {
        Ctransaction {
            sh,
            addr16: addr_to_ipv6_bytes(&addr.parse().unwrap()),
        }
    }

    #[test]
    fn test_connection_id_derivation() {
        let sh = shared(0x0102030405060708);
        let t = tx(&sh, "10.0.0.1:5000");
        // sha1(secret_be8 || addr16)[0..8] (intended upstream semantics)
        let mut s = [0u8; 24];
        write_int(&mut s, 0, 8, 0x0102030405060708);
        s[8..].copy_from_slice(&t.addr16);
        let want = read_int(8, &Sha1::digest(s));
        assert_eq!(t.connection_id(), want);
        // different addr -> different id
        let t2 = tx(&sh, "10.0.0.2:5000");
        assert_ne!(t.connection_id(), t2.connection_id());
    }

    #[test]
    fn test_connect_flow() {
        let sh = shared(42);
        let t = tx(&sh, "10.0.0.1:5000");
        // note: action lives at offset 8 (uti_action), tid at 12
        let mut req = vec![0u8; 16];
        write_int(&mut req, UTI_CONNECTION_ID, 8, PROTOCOL_ID);
        write_int(&mut req, UTI_ACTION, 4, UTA_CONNECT);
        write_int(&mut req, UTI_TRANSACTION_ID, 4, 0xdeadbeef);
        let resp = t.recv(&req).expect("connect response");
        assert_eq!(resp.len(), 16);
        assert_eq!(read_int(4, &resp[UTO_ACTION..]), UTA_CONNECT);
        assert_eq!(read_int(4, &resp[UTO_TRANSACTION_ID..]), 0xdeadbeef);
        assert_eq!(read_int(8, &resp[UTOC_CONNECTION_ID..]), t.connection_id());
        // wrong protocol id -> no response
        let mut bad = req.clone();
        write_int(&mut bad, UTI_CONNECTION_ID, 8, 123);
        assert!(t.recv(&bad).is_none());
    }

    #[test]
    fn test_announce_and_scrape_flow() {
        let sh = shared(7);
        let t = tx(&sh, "10.0.0.9:6000");
        let cid = t.connection_id();
        let ih = [0x5au8; 20];
        let pid = *b"-UT355W-udpudpudp012";
        let mut req = vec![0u8; UTIA_SIZE];
        write_int(&mut req, UTI_CONNECTION_ID, 8, cid);
        write_int(&mut req, UTI_ACTION, 4, UTA_ANNOUNCE);
        write_int(&mut req, UTI_TRANSACTION_ID, 4, 999);
        req[UTIA_INFO_HASH..UTIA_INFO_HASH + 20].copy_from_slice(&ih);
        req[UTIA_PEER_ID..UTIA_PEER_ID + 20].copy_from_slice(&pid);
        write_int(&mut req, UTIA_DOWNLOADED, 8, 0);
        write_int(&mut req, UTIA_LEFT, 8, 1234);
        write_int(&mut req, UTIA_UPLOADED, 8, 0);
        write_int(&mut req, UTIA_EVENT, 4, 2); // started
        write_int(&mut req, UTIA_PORT, 2, 4321);
        let resp = t.recv(&req).expect("announce response");
        assert_eq!(read_int(4, &resp[UTO_ACTION..]), UTA_ANNOUNCE);
        assert_eq!(read_int(4, &resp[UTO_TRANSACTION_ID..]), 999);
        assert_eq!(read_int(4, &resp[UTOA_INTERVAL..]), 1800);
        assert_eq!(read_int(4, &resp[UTOA_LEECHERS..]), 1);
        assert_eq!(read_int(4, &resp[UTOA_SEEDERS..]), 0);
        assert_eq!(resp.len(), UTOA_SIZE + 6);
        assert_eq!(&resp[UTOA_SIZE..UTOA_SIZE + 4], &[10, 0, 0, 9]);
        assert_eq!(&resp[UTOA_SIZE + 4..UTOA_SIZE + 6], &(4321u16).to_be_bytes());
        // peer registered in memory
        {
            let st = sh.lock();
            let tor = st.find_torrent(&ih).unwrap();
            assert_eq!(tor.leechers, 1);
            let p = tor.peers.get(&pid).unwrap();
            assert_eq!(p.ipv4, [10, 0, 0, 9]);
            assert_eq!(p.port, 4321);
        }
        // scrape for the announced hash + an unknown one
        let mut sreq = vec![0u8; UTIS_SIZE + 40];
        write_int(&mut sreq, UTI_CONNECTION_ID, 8, cid);
        write_int(&mut sreq, UTI_ACTION, 4, UTA_SCRAPE);
        write_int(&mut sreq, UTI_TRANSACTION_ID, 4, 1000);
        sreq[UTIS_SIZE..UTIS_SIZE + 20].copy_from_slice(&ih);
        sreq[UTIS_SIZE + 20..].copy_from_slice(&[0xff; 20]);
        let resp = t.recv(&sreq).expect("scrape response");
        assert_eq!(read_int(4, &resp[UTO_ACTION..]), UTA_SCRAPE);
        assert_eq!(resp.len(), UTOS_SIZE + 24);
        assert_eq!(read_int(4, &resp[UTOS_SIZE..]), 0); // seeders
        assert_eq!(read_int(4, &resp[UTOS_SIZE + 4..]), 0); // completed
        assert_eq!(read_int(4, &resp[UTOS_SIZE + 8..]), 1); // leechers
        assert_eq!(&resp[UTOS_SIZE + 12..], &[0; 12]); // unknown hash
        assert_eq!(sh.lock().stats.scraped_udp, 1);
        assert_eq!(sh.lock().stats.announced_udp, 1);
    }

    #[test]
    fn test_access_denied() {
        let sh = shared(7);
        sh.lock().config.anonymous_announce = false;
        sh.lock().config.anonymous_scrape = false;
        let t = tx(&sh, "10.0.0.9:6000");
        let cid = t.connection_id();
        let mut req = vec![0u8; UTIA_SIZE];
        write_int(&mut req, UTI_CONNECTION_ID, 8, cid);
        write_int(&mut req, UTI_ACTION, 4, UTA_ANNOUNCE);
        write_int(&mut req, UTI_TRANSACTION_ID, 4, 5);
        let resp = t.recv(&req).unwrap();
        assert_eq!(read_int(4, &resp[UTO_ACTION..]), UTA_ERROR);
        assert_eq!(&resp[UTOE_SIZE..], b"access denied");
    }

    #[test]
    fn test_bad_connection_id() {
        let sh = shared(7);
        let t = tx(&sh, "10.0.0.9:6000");
        let mut req = vec![0u8; UTIS_SIZE];
        write_int(&mut req, UTI_CONNECTION_ID, 8, 1); // wrong
        write_int(&mut req, UTI_ACTION, 4, UTA_SCRAPE);
        assert!(t.recv(&req).is_none());
    }
}
