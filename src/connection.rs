//! Port of Tracker/connection.cpp: the raw HTTP request-line state machine,
//! routing (announce / debug / scrape / statistics / metrics), torrent-pass
//! extraction from the URL path, response building (incl. gzip + redirect)
//! and the 4 KiB read buffer semantics of the original.

use std::io::Write;

use crate::bt_misc::{inet_ntoa16, uri_decode, uri_encode};
use crate::tracker::{srv_debug, srv_insert_peer, srv_metrics, srv_scrape, srv_select_peers, srv_statistics, Shared};
use crate::tracker_input::{addr_to_ipv6_bytes, TrackerInput};

pub const READ_BUF_SIZE: usize = 4 << 10; // std::array<char, 4 << 10> read_b_

pub struct Connection {
    /// read_b_
    pub buf: Vec<u8>,
    /// how much of buf holds received data
    pub filled: usize,
    /// scan position within the received data (the original's `a` pointer)
    pub scan: usize,
    /// state_
    pub state: u8,
    /// generated response (header + body), None until produced
    pub response: Option<Vec<u8>>,
    /// bytes of response already handed to the socket
    pub response_sent: usize,
    /// addr_ (as the original sees it: sockaddr_in6 address bytes + port)
    pub addr16: [u8; 16],
    pub addr_port: u16,
    /// ctime_
    pub ctime: i64,
    /// end of stream observed (recv() == 0 or read buffer exhausted)
    pub eof_seen: bool,
}

impl Connection {
    pub fn new(addr: &std::net::SocketAddr, now: i64) -> Connection {
        Connection {
            buf: vec![0; READ_BUF_SIZE],
            filled: 0,
            scan: 0,
            state: 0,
            response: None,
            response_sent: 0,
            addr16: addr_to_ipv6_bytes(addr),
            addr_port: addr.port(),
            ctime: now,
            eof_seen: false,
        }
    }

    /// The original closes a connection when `state_ == 5 && write_b_.empty()`
    /// (headers/EOF reached and the response fully sent).
    pub fn flushed_and_finished(&self) -> bool {
        self.state == 5
            && self.response_sent >= self.response.as_ref().map(|r| r.len()).unwrap_or(0)
    }

    /// Mark EOF observed (recv() returned 0): state_ = 5.
    pub fn mark_eof(&mut self) {
        self.eof_seen = true;
        self.state = 5;
    }

    /// Feed newly received bytes (the original's recv() scan loop).
    /// Returns the response slice when one was produced by this chunk.
    pub fn feed<'a>(&'a mut self, data: &[u8], sh: &Shared, execs: &mut Vec<String>) -> Option<&'a [u8]> {
        let space = READ_BUF_SIZE - self.filled;
        if space == 0 {
            // recv() into an empty buffer returns 0 -> treated as EOF
            self.mark_eof();
            return None;
        }
        let before = self.filled;
        let n = data.len().min(space);
        self.buf[self.filled..self.filled + n].copy_from_slice(&data[..n]);
        self.filled += n;

        let produced_before = self.response.is_some();
        // scan loop, faithful to connection_t::recv()
        let mut a = before;
        loop {
            let state = self.state;
            while a < self.filled && self.buf[a] != b'\n' && self.buf[a] != b'\r' {
                a += 1;
                if self.state != 0 {
                    self.state = 1;
                }
            }
            if a < self.filled {
                let c = self.buf[a];
                match self.state {
                    0 => {
                        let line = self.buf[0..a].to_vec();
                        let resp = self.read(line, sh, execs);
                        self.state = 1;
                        // [[fallthrough]] into case 1/3
                        self.state += if c == b'\n' { 2 } else { 1 };
                        self.response = resp;
                    }
                    1 | 3 => self.state += if c == b'\n' { 2 } else { 1 },
                    2 | 4 => self.state += 1,
                    _ => {}
                }
                a += 1;
            }
            if state == self.state {
                break;
            }
        }
        self.scan = a;

        if n < data.len() && self.filled == READ_BUF_SIZE {
            // the remaining bytes would be read into a full buffer -> EOF
            self.mark_eof();
        }
        if !produced_before && self.response.is_some() {
            Some(self.response.as_ref().unwrap())
        } else {
            None
        }
    }

    /// Port of `connection_t::read(std::string_view v)`: parses the request
    /// line, dispatches and builds the full response (header + body).
    /// Returns None for the original's silent `return` paths (no response).
    fn read(&mut self, v: Vec<u8>, sh: &Shared, execs: &mut Vec<String>) -> Option<Vec<u8>> {
        if sh.lock().config.log_access {
            let mut f = sh.access_log.lock().unwrap();
            if f.is_none() {
                *f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("xbt_tracker_raw.log")
                    .ok();
            }
            if let Some(f) = f.as_mut() {
                let head = format!(
                    "{}\t{}\t{}\t",
                    crate::tracker::srv_time(),
                    inet_ntoa16(self.addr16),
                    self.addr_port
                );
                let _ = f.write_all(head.as_bytes());
                let _ = f.write_all(&v);
                let _ = f.write_all(b"\n");
                let _ = f.flush();
            }
        }

        let mut ti = TrackerInput::default();
        let e = match v.iter().position(|&c| c == b'?') {
            None => v.len(),
            Some(e) => {
                let start = e + 1;
                match v[start..].iter().position(|&c| c == b' ') {
                    None => return None, // `if (b == npos) return;` — no response at all
                    Some(rel) => {
                        let b = start + rel;
                        let mut a = start;
                        while a < b {
                            // `size_t c = v.find('=', a); if (c++ == npos) break;`
                            let c = match v[a..].iter().position(|&c| c == b'=') {
                                None => break,
                                Some(rel) => a + rel + 1,
                            };
                            // `size_t d = v.find_first_of(" &", c); if (d == npos) break;`
                            let d = match v[c..].iter().position(|&x| x == b' ' || x == b'&') {
                                None => break,
                                Some(rel) => c + rel,
                            };
                            let name = &v[a..c - 1];
                            let value = uri_decode(&v[c..d]);
                            ti.set(name, &value);
                            a = d + 1;
                        }
                    }
                }
                e
            }
        };
        // the original unconditionally overwrites ti.ipv6_ with the socket address
        ti.ipv6 = self.addr16;

        let mut torrent_pass: &[u8] = &[];
        let mut a = 4usize;
        if a < e && a < v.len() && v[a] == b'/' {
            a += 1;
            if a + 32 < e && a + 32 < v.len() && v[a + 32] == b'/' {
                torrent_pass = &v[a..a + 32];
                a += 33;
            }
        }

        let mut h = String::from("HTTP/1.0 200 OK\r\n");
        let mut s: Vec<u8> = Vec::new();
        let mut gzip = true;

        let (route, debug_cfg, full_scrape_cfg, gzip_scrape_cfg) = {
            let st = sh.lock();
            (
                if a < v.len() { v[a] } else { 0 },
                st.config.debug,
                st.config.full_scrape,
                st.config.gzip_scrape,
            )
        };

        match route {
            b'a' => {
                if ti.valid() {
                    gzip = false;
                    let mut st = sh.lock();
                    let user = st.find_user_by_torrent_pass(torrent_pass, &ti.info_hash);
                    let error = srv_insert_peer(&mut st, &ti, false, user, execs);
                    s = if error.is_empty() {
                        srv_select_peers(&st, &ti)
                    } else {
                        let mut b = Vec::new();
                        b.extend_from_slice(b"d14:failure reason");
                        b.extend_from_slice(error.len().to_string().as_bytes());
                        b.push(b':');
                        b.extend_from_slice(error.as_bytes());
                        b.push(b'e');
                        b
                    };
                }
            }
            b'd' => {
                if debug_cfg {
                    h.push_str("Content-Type: text/html; charset=us-ascii\r\n");
                    let st = sh.lock();
                    s = srv_debug(&st, &ti).into_bytes();
                }
            }
            b's' => {
                if v.len() >= 7 && v[6] == b't' {
                    h.push_str("Content-Type: text/html; charset=us-ascii\r\n");
                    let st = sh.lock();
                    s = srv_statistics(&st).into_bytes();
                } else if full_scrape_cfg || !ti.info_hash.is_empty() {
                    gzip = gzip_scrape_cfg && ti.info_hash.is_empty();
                    let mut st = sh.lock();
                    let user = st.find_user_by_torrent_pass(torrent_pass, &ti.info_hash);
                    s = srv_scrape(&mut st, &ti, user);
                }
            }
            b'm' => {
                gzip = false;
                h.push_str("Content-Type: text/plain; charset=us-ascii\r\n");
                let st = sh.lock();
                s = srv_metrics(&st).into_bytes();
            }
            _ => {}
        }

        let redirect_url = sh.lock().config.redirect_url.clone();
        if s.is_empty() {
            if !ti.info_hash.is_empty() || redirect_url.is_empty() {
                h = String::from("HTTP/1.0 404 Not Found\r\n");
            } else {
                h = format!(
                    "HTTP/1.0 302 Found\r\nLocation: {}{}\r\n",
                    redirect_url,
                    if ti.info_hash.is_empty() {
                        String::new()
                    } else {
                        format!("?info_hash={}", uri_encode(&ti.info_hash))
                    }
                );
            }
        } else if gzip {
            let s2 = crate::gzip::gzip(&s);
            if s2.len() + 24 < s.len() {
                h.push_str("Content-Encoding: gzip\r\n");
                s = s2;
            }
        }
        h.push_str("\r\n");
        let mut out = h.into_bytes();
        out.extend_from_slice(&s);
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Connection::read needs a Shared; these tests exercise the pure parts
    // (state machine + query parsing) with a dummy DB. Full request/response
    // behavior is covered by the integration + differential tests.
    fn shared() -> Shared {
        use crate::database::Db;
        use crate::tracker::State;
        use std::sync::Mutex;
        let mut st = State::new("test.conf".into(), "xbt_".into(), 1);
        st.config.anonymous_announce = true;
        st.config.anonymous_scrape = true;
        st.config.auto_register = true;
        st.config.full_scrape = true;
        st.config.debug = true;
        Shared {
            state: Mutex::new(st),
            db: Db::dummy(),
            access_log: Mutex::new(None),
        }
    }

    #[test]
    fn test_state_machine_crlf() {
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        c.feed(b"GET /scrape HTTP/1.1\r\nHost: x\r\n\r\n", &sh, &mut execs);
        assert_eq!(c.state, 5);
        assert!(c.response.is_some());
        let r = c.response.unwrap();
        assert!(r.starts_with(b"HTTP/1.0 200 OK\r\n"));
        assert!(r.ends_with(b"d5:filesdee"));
    }

    #[test]
    fn test_state_machine_split_delivery() {
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        assert!(c.feed(b"GET /met", &sh, &mut execs).is_none());
        assert!(c.response.is_none());
        c.feed(b"rics HTTP/1.1\r\n", &sh, &mut execs);
        assert!(c.response.is_some()); // responds at end of request line
        c.feed(b"Host: x\r\n\r\n", &sh, &mut execs);
        assert_eq!(c.state, 5);
        let r = c.response.unwrap();
        assert!(r.starts_with(b"HTTP/1.0 200 OK\r\nContent-Type: text/plain; charset=us-ascii\r\n\r\nxbt_uptime "));
    }

    #[test]
    fn test_bare_lf_lines() {
        // Faithful quirk: with bare-LF line ends the do-while scan loop
        // exits early (net state change zero) and state stalls at 3 —
        // exactly like connection_t::recv(); the response is still produced.
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        c.feed(b"GET /scrape HTTP/1.1\nHost: x\n\n", &sh, &mut execs);
        assert_eq!(c.state, 3);
        assert!(c.response.is_some());
    }

    #[test]
    fn test_no_space_after_query_no_response() {
        // request line with '?' but no following ' ' -> read() returns without
        // responding (original quirk); connection hangs until timeout
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        c.feed(b"GET /announce?info_hash=xx\r\n\r\n", &sh, &mut execs);
        assert!(c.response.is_none());
    }

    #[test]
    fn test_404_root() {
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        c.feed(b"GET / HTTP/1.1\r\n\r\n", &sh, &mut execs);
        assert_eq!(c.response.unwrap(), b"HTTP/1.0 404 Not Found\r\n\r\n");
    }

    #[test]
    fn test_statistics_routing_quirk() {
        // "/st..." routes to statistics via the absolute v[6]=='t' check
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        c.feed(b"GET /stats HTTP/1.1\r\n\r\n", &sh, &mut execs);
        let r = c.response.unwrap();
        assert!(r.starts_with(b"HTTP/1.0 200 OK\r\nContent-Type: text/html; charset=us-ascii\r\n"));
        // the body is gzipped when that pays off (s2 + 24 < s), like the original
        if r.windows(22).any(|w| w == b"Content-Encoding: gzip") {
            let pos = r.windows(4).position(|w| w == b"\r\n\r\n").expect("header end") + 4;
            let mut gz = flate2::read::GzDecoder::new(&r[pos..]);
            let mut html = Vec::new();
            std::io::Read::read_to_end(&mut gz, &mut html).unwrap();
            assert!(html.windows(7).any(|w| w == b"<table>"));
        } else {
            assert!(r.windows(7).any(|w| w == b"<table>"));
        }
    }

    #[test]
    fn test_query_parse_quirks() {
        // '&' inside key when '=' only appears later (faithful find-from-a)
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        c.feed(b"GET /scrape?info_hash=a&info_hash HTTP/1.1\r\n\r\n", &sh, &mut execs);
        // no crash, some response produced
        assert!(c.response.is_some());
    }

    #[test]
    fn test_buffer_full_eof() {
        let sh = shared();
        let mut c = Connection::new(&"127.0.0.1:1234".parse().unwrap(), 0);
        let mut execs = vec![];
        let junk = vec![b'x'; READ_BUF_SIZE + 100];
        c.feed(&junk, &sh, &mut execs);
        assert_eq!(c.filled, READ_BUF_SIZE);
        assert_eq!(c.state, 5);
        assert!(c.response.is_none());
        assert!(c.flushed_and_finished());
    }
}
