//! Port of Tracker/tracker.cpp (+ tracker.h): in-memory state, announce /
//! scrape / debug / statistics / metrics logic and the periodic DB sync
//! functions (read_config, read_db_torrents, read_db_users,
//! write_db_torrents, write_db_users, clean_up, test_sql).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use sha1::{Digest, Sha1};

use crate::bt_misc::{duration2a, generate_random_string, hex_encode, inet_ntoa16, inet_ntoa4, n2a, uri_encode};
use crate::config::Config;
use crate::database::Db;
use crate::rng::rand;
use crate::sql::{make_query, row_bytes, row_int, Arg};
use crate::stream_int::read_int;
use crate::tracker_input::{is_ipv4, is_zero, to_sql, E_COMPLETED, E_STOPPED, TrackerInput};

// bts_* constants (misc/bt_strings.h)
pub const BTS_CAN_NOT_LEECH: &str = "access denied, leeching forbidden, you are only allowed to seed";
pub const BTS_PEERS_LIMIT_REACHED: &str = "access denied, peers limit reached";
pub const BTS_UNREGISTERED_TORRENT: &str = "unregistered torrent";
pub const BTS_UNREGISTERED_TORRENT_PASS: &str = "unregistered torrent pass";
pub const BTS_WAIT_TIME: &str = "access denied, wait time in effect";

// SQL statement templates (byte-identical to the original, including spacing)
const SQL_SELECT_CONFIG: &str = "select name, value from @config where value is not null";
const SQL_INSERT_CONFIG_KEY: &str = "insert into @config (name, value) values ('torrent_pass_private_key', ?)";
const SQL_UPDATE_ZERO_COUNTS: &str = "update @torrents set @leechers = 0, @seeders = 0";
const SQL_SELECT_UNREGISTERED: &str = "select info_hash, @tid from @torrents where flags != 0 and flags & 1";
const SQL_DELETE_TORRENT: &str = "delete from @torrents where @tid = ?";
const SQL_SELECT_TORRENTS: &str = "select info_hash, @completed, @tid, ctime from @torrents where @tid >= ?";
const SQL_INSERT_TORRENT: &str =
    "insert into @torrents (info_hash, mtime, ctime) values (?, unix_timestamp(), unix_timestamp())";
const SQL_UPSERT_TORRENTS: &str = concat!(
    "insert ignore into @torrents (@leechers, @seeders, @completed, @tid) values ?",
    " on duplicate key update",
    "  @leechers = values(@leechers),",
    "  @seeders = values(@seeders),",
    "  @completed = values(@completed),",
    "  mtime = unix_timestamp()"
);
const SQL_INSERT_ANNOUNCE_LOG: &str = "insert delayed into @announce_log (ipv6, port, event, info_hash, peer_id, downloaded, left0, uploaded, uid, mtime) values ?";
const SQL_INSERT_SCRAPE_LOG: &str = "insert delayed into @scrape_log (ipv6, uid, mtime) values ?";
const SQL_UPSERT_PEERS: &str = concat!(
    "insert ignore into @torrents_users (active, completed, downloaded, `left`, uploaded, mtime, tid, uid) values ?",
    " on duplicate key update",
    "  active = values(active),",
    "  completed = completed + values(completed),",
    "  downloaded = downloaded + values(downloaded),",
    "  `left` = values(`left`),",
    "  uploaded = uploaded + values(uploaded),",
    "  mtime = values(mtime)"
);
const SQL_DEACTIVATE_PEERS: &str = "update @torrents_users set active = 0 where active = 1 and mtime < unix_timestamp() - 60 * 60";
const SQL_UPSERT_USERS: &str = concat!(
    "insert ignore into @users (downloaded, uploaded, @uid) values ?",
    " on duplicate key update",
    "  downloaded = downloaded + values(downloaded),",
    "  uploaded = uploaded + values(uploaded)"
);

#[derive(Clone, Default)]
pub struct Peer {
    pub downloaded: i64,
    pub uploaded: i64,
    pub mtime: i64,
    pub uid: i32,
    pub port: u16, // host order; written big-endian into compact peer lists
    pub left: bool,
    pub ipv4: [u8; 4],
    pub ipv6: [u8; 16],
}

pub struct Torrent {
    pub peers: HashMap<[u8; 20], Peer>,
    pub ctime: i64,
    pub completed: i32,
    pub tid: i32,
    pub leechers: i32,
    pub seeders: i32,
    pub dirty: bool,
}

impl Default for Torrent {
    fn default() -> Self {
        // matches value-initialization of torrent_t in the original
        Torrent {
            peers: HashMap::new(),
            ctime: 0,
            completed: 0,
            tid: 0,
            leechers: 0,
            seeders: 0,
            dirty: true,
        }
    }
}

#[derive(Clone)]
pub struct User {
    pub uid: i32,
    pub peers_limit: i32,
    pub torrent_pass_version: i32,
    pub wait_time: i32,
    pub can_leech: bool,
    pub marked: bool,
}

impl Default for User {
    fn default() -> Self {
        User {
            uid: 0,
            peers_limit: 0,
            torrent_pass_version: 0,
            wait_time: 0,
            can_leech: true,
            marked: false,
        }
    }
}

#[derive(Default)]
pub struct Stats {
    pub accept_errors: i64,
    pub accepted_tcp: i64,
    pub announced_http: i64,
    pub announced_udp: i64,
    pub received_udp: i64,
    pub scraped_full: i64,
    pub scraped_http: i64,
    pub scraped_multi: i64,
    pub scraped_udp: i64,
    pub sent_udp: i64,
    pub slow_tcp: i64,
    pub start_time: i64,
}

impl Stats {
    pub fn announced(&self) -> i64 {
        self.announced_http + self.announced_udp
    }
    pub fn scraped(&self) -> i64 {
        self.scraped_http + self.scraped_udp
    }
}

pub struct State {
    pub torrents: HashMap<[u8; 20], Torrent>,
    pub users: HashMap<i32, User>,
    /// torrent_pass (32 chars) -> uid (the original maps to user_t*)
    pub users_torrent_passes: HashMap<[u8; 32], i32>,
    pub config: Config,
    /// g_database name map (@name -> table/column)
    pub db_names: HashMap<String, String>,
    pub stats: Stats,
    pub secret: u64,
    pub tid_end: i32,
    pub announce_log_buffer: String,
    pub scrape_log_buffer: String,
    pub torrents_users_updates_buffer: String,
    pub users_updates_buffer: String,
    pub read_users_can_leech: bool,
    pub read_users_peers_limit: bool,
    pub read_users_torrent_pass: bool,
    pub read_users_torrent_pass_version: bool,
    pub read_users_wait_time: bool,
    pub read_config_time: i64,
    pub clean_up_time: i64,
    pub read_db_torrents_time: i64,
    pub read_db_users_time: i64,
    pub write_db_torrents_time: i64,
    pub write_db_users_time: i64,
    pub conf_file: String,
    pub table_prefix: String,
}

pub struct Shared {
    pub state: Mutex<State>,
    pub db: Db,
    /// lazily opened "xbt_tracker_raw.log" (config log_access)
    pub access_log: Mutex<Option<std::fs::File>>,
}

impl Shared {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub fn srv_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Port of the `to_array<char, 20>(v)` idiom: copies when the size matches,
/// zero-fills otherwise (release-build behavior of the original).
pub fn to_array20(v: &[u8]) -> [u8; 20] {
    let mut d = [0u8; 20];
    if v.len() == 20 {
        d.copy_from_slice(v);
    }
    d
}

/// Port of the start-up secret: `for (i < 8) g_secret = g_secret << 8 ^ rand()`
pub fn generate_secret() -> u64 {
    let mut secret: u64 = 0;
    for _ in 0..8 {
        secret = (secret << 8) ^ rand();
    }
    secret
}

impl State {
    pub fn new(conf_file: String, table_prefix: String, secret: u64) -> State {
        let mut st = State {
            torrents: HashMap::new(),
            users: HashMap::new(),
            users_torrent_passes: HashMap::new(),
            config: Config::default(),
            db_names: HashMap::new(),
            stats: Stats { start_time: srv_time(), ..Default::default() },
            secret,
            tid_end: 0,
            announce_log_buffer: String::new(),
            scrape_log_buffer: String::new(),
            torrents_users_updates_buffer: String::new(),
            users_updates_buffer: String::new(),
            read_users_can_leech: false,
            read_users_peers_limit: false,
            read_users_torrent_pass: false,
            read_users_torrent_pass_version: false,
            read_users_wait_time: false,
            read_config_time: 0,
            clean_up_time: 0,
            read_db_torrents_time: 0,
            read_db_users_time: 0,
            write_db_torrents_time: 0,
            write_db_users_time: 0,
            conf_file,
            table_prefix,
        };
        // g_database.set_name("config", g_table_prefix + "config") from srv_run
        st.db_names
            .insert("config".into(), format!("{}config", st.table_prefix));
        st
    }

    pub fn find_torrent(&self, info_hash: &[u8]) -> Option<&Torrent> {
        self.torrents.get(&to_array20(info_hash))
    }

    pub fn find_user_by_uid(&self, v: i32) -> Option<&User> {
        self.users.get(&v)
    }

    /// Port of `find_user_by_torrent_pass`.
    pub fn find_user_by_torrent_pass(&self, v: &[u8], info_hash: &[u8]) -> Option<i32> {
        if v.len() != 32 {
            return None;
        }
        let uid = read_int(4, &crate::bt_misc::hex_decode(&v[0..8])) as i32;
        if let Some(user) = self.find_user_by_uid(uid) {
            // sha1("{key} {version} {uid} {info_hash}") first 12 bytes must match
            let computed = compute_torrent_pass(
                &self.config.torrent_pass_private_key,
                user.torrent_pass_version,
                user.uid,
                info_hash,
            );
            if computed.as_bytes() == v {
                return Some(user.uid);
            }
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(v);
        self.users_torrent_passes.get(&key).copied()
    }
}

/// Port of `srv_insert_peer`. Returns the error string ("" = success).
/// `execs` receives SQL statements that must be fired because of the
/// >255 KiB inline buffer flush (write_db_users call inside the original).
pub fn srv_insert_peer(
    st: &mut State,
    ti: &TrackerInput,
    udp: bool,
    user_uid: Option<i32>,
    execs: &mut Vec<String>,
) -> String {
    let now = srv_time();
    let mut ih20 = [0u8; 20];
    if ti.info_hash.len() == 20 {
        ih20.copy_from_slice(&ti.info_hash);
    }

    if st.config.log_announce {
        let sql = make_query(
            &st.db_names,
            "(?,?,?,?,?,?,?,?,?,?),",
            &[
                Arg::Hex(to_sql(&ti.ipv6)),
                Arg::Int(ti.port as i64),
                Arg::Int(ti.event),
                Arg::Hex(&ti.info_hash),
                Arg::Hex(&ti.peer_id),
                Arg::Int(ti.downloaded),
                Arg::Int(ti.left),
                Arg::Int(ti.uploaded),
                Arg::Int(user_uid.unwrap_or(0) as i64),
                Arg::Int(now),
            ],
        );
        st.announce_log_buffer.push_str(&sql);
    }
    if !st.config.offline_message.is_empty() {
        return st.config.offline_message.clone();
    }
    // `if (0) return bts_banned_client;` in the original — dead code
    let user = user_uid.and_then(|uid| st.users.get(&uid).cloned());
    if !st.config.anonymous_announce && user.is_none() {
        return BTS_UNREGISTERED_TORRENT_PASS.into();
    }
    if !st.config.auto_register && !st.torrents.contains_key(&ih20) {
        return BTS_UNREGISTERED_TORRENT.into();
    }
    if ti.left != 0 {
        if let Some(u) = &user {
            if !u.can_leech {
                return BTS_CAN_NOT_LEECH.into();
            }
        }
    }
    let t = st.torrents.entry(ih20).or_default();
    if t.ctime == 0 {
        t.ctime = now;
    }
    if ti.left != 0 {
        if let Some(u) = &user {
            if u.wait_time != 0 && t.ctime + u.wait_time as i64 > now {
                return BTS_WAIT_TIME.into();
            }
        }
    }
    let p_info = t.peers.get(&ti.peer_id).map(|p| (p.left, p.uid, p.downloaded, p.uploaded));
    if let Some((left, _, _, _)) = p_info {
        if left {
            t.leechers -= 1;
        } else {
            t.seeders -= 1;
        }
    } else if ti.left != 0 {
        if let Some(u) = &user {
            if u.peers_limit != 0 {
                let c = t
                    .peers
                    .values()
                    .filter(|j| j.left && j.uid == u.uid)
                    .count() as i32;
                if c >= u.peers_limit {
                    return BTS_PEERS_LIMIT_REACHED.into();
                }
            }
        }
    }
    if let Some(u) = &user {
        if t.tid != 0 {
            let mut downloaded = 0i64;
            let mut uploaded = 0i64;
            if let Some((_, puid, pdl, pul)) = p_info {
                if puid == u.uid && ti.downloaded >= pdl && ti.uploaded >= pul {
                    downloaded = ti.downloaded - pdl;
                    uploaded = ti.uploaded - pul;
                }
            }
            let sql = make_query(
                &st.db_names,
                "(?,?,?,?,?,?,?,?),",
                &[
                    Arg::Int((ti.event != E_STOPPED) as i64),
                    Arg::Int((ti.event == E_COMPLETED) as i64),
                    Arg::Int(downloaded),
                    Arg::Int(ti.left),
                    Arg::Int(uploaded),
                    Arg::Int(now),
                    Arg::Int(t.tid as i64),
                    Arg::Int(u.uid as i64),
                ],
            );
            st.torrents_users_updates_buffer.push_str(&sql);
            if downloaded != 0 || uploaded != 0 {
                let sql = make_query(
                    &st.db_names,
                    "(?,?,?),",
                    &[Arg::Int(downloaded), Arg::Int(uploaded), Arg::Int(u.uid as i64)],
                );
                st.users_updates_buffer.push_str(&sql);
            }
            if st.torrents_users_updates_buffer.len() > 255 << 10 {
                flush_db_users(
                    &st.db_names,
                    &mut st.torrents_users_updates_buffer,
                    &mut st.users_updates_buffer,
                    &mut st.write_db_users_time,
                    execs,
                );
            }
        }
    }
    if ti.event == E_STOPPED {
        t.peers.remove(&ti.peer_id);
    } else {
        let peer = t.peers.entry(ti.peer_id).or_default();
        peer.downloaded = ti.downloaded;
        peer.left = ti.left != 0;
        peer.port = ti.port;
        peer.uid = user_uid.unwrap_or(0);
        peer.uploaded = ti.uploaded;
        if peer.left {
            t.leechers += 1;
        } else {
            t.seeders += 1;
        }
        peer.mtime = now;
        if is_ipv4(&ti.ipv6) {
            peer.ipv4.copy_from_slice(&ti.ipv6[12..16]);
        } else {
            peer.ipv6 = ti.ipv6;
        }
    }
    if ti.event == E_COMPLETED {
        t.completed += 1;
    }
    if udp {
        st.stats.announced_udp += 1;
    } else {
        st.stats.announced_http += 1;
    }
    t.dirty = true;
    String::new()
}

/// Port of `torrent_t::select_peers` — returns the compact IPv4 peer list
/// (6-byte records, capped by the original's 300-byte buffer).
pub fn select_peers(t: &Torrent, ti: &TrackerInput) -> Vec<u8> {
    if ti.event == E_STOPPED {
        return Vec::new();
    }
    let mut candidates: Vec<[u8; 6]> = Vec::with_capacity(t.peers.len());
    for p in t.peers.values() {
        if ti.is_seeder() && !p.left {
            continue;
        }
        if is_zero(&p.ipv4) {
            continue;
        }
        let mut v = [0u8; 6];
        v[..4].copy_from_slice(&p.ipv4);
        v[4..].copy_from_slice(&p.port.to_be_bytes());
        candidates.push(v);
    }
    let mut d: Vec<u8> = Vec::new();
    let cap = 300 / 6; // original: array<char, 300>, c = d.size() / 6
    if candidates.len() <= cap {
        for v in &candidates {
            d.extend_from_slice(v);
        }
        return d;
    }
    let mut c = cap;
    while c > 0 {
        c -= 1;
        let i = (rand() as usize) % candidates.len();
        d.extend_from_slice(&candidates[i]);
        candidates.swap_remove(i);
    }
    d
}

/// Port of `torrent_t::select_peers6` (18-byte records, 900-byte buffer).
pub fn select_peers6(t: &Torrent, ti: &TrackerInput) -> Vec<u8> {
    if ti.event == E_STOPPED {
        return Vec::new();
    }
    let mut candidates: Vec<[u8; 18]> = Vec::with_capacity(t.peers.len());
    for p in t.peers.values() {
        if ti.is_seeder() && !p.left {
            continue;
        }
        if is_zero(&p.ipv6) {
            continue;
        }
        let mut v = [0u8; 18];
        v[..16].copy_from_slice(&p.ipv6);
        v[16..].copy_from_slice(&p.port.to_be_bytes());
        candidates.push(v);
    }
    let mut d: Vec<u8> = Vec::new();
    let cap = 900 / 18;
    if candidates.len() <= cap {
        for v in &candidates {
            d.extend_from_slice(v);
        }
        return d;
    }
    let mut c = cap;
    while c > 0 {
        c -= 1;
        let i = (rand() as usize) % candidates.len();
        d.extend_from_slice(&candidates[i]);
        candidates.swap_remove(i);
    }
    d
}

/// Port of `srv_select_peers`.
pub fn srv_select_peers(st: &State, ti: &TrackerInput) -> Vec<u8> {
    let t = match st.find_torrent(&ti.info_hash) {
        Some(t) => t,
        None => return Vec::new(),
    };
    if !is_ipv4(&ti.ipv6) {
        return srv_select_peers6(st, ti);
    }
    let peers = select_peers(t, ti);
    let mut s = Vec::with_capacity(1 << 10);
    s.extend_from_slice(b"d8:completei");
    s.extend_from_slice(t.seeders.to_string().as_bytes());
    s.extend_from_slice(b"e10:incompletei");
    s.extend_from_slice(t.leechers.to_string().as_bytes());
    s.extend_from_slice(b"e8:intervali");
    s.extend_from_slice(st.config.announce_interval.to_string().as_bytes());
    s.extend_from_slice(b"e12:min intervali");
    s.extend_from_slice(st.config.announce_interval.to_string().as_bytes());
    s.extend_from_slice(b"e5:peers");
    s.extend_from_slice(peers.len().to_string().as_bytes());
    s.push(b':');
    s.extend_from_slice(&peers);
    s.push(b'e');
    s
}

/// Port of `srv_select_peers6`.
pub fn srv_select_peers6(st: &State, ti: &TrackerInput) -> Vec<u8> {
    let t = match st.find_torrent(&ti.info_hash) {
        Some(t) => t,
        None => return Vec::new(),
    };
    let peers = select_peers6(t, ti);
    let mut s = Vec::with_capacity(2 << 10);
    s.extend_from_slice(b"d8:completei");
    s.extend_from_slice(t.seeders.to_string().as_bytes());
    s.extend_from_slice(b"e10:incompletei");
    s.extend_from_slice(t.leechers.to_string().as_bytes());
    s.extend_from_slice(b"e8:intervali");
    s.extend_from_slice(st.config.announce_interval.to_string().as_bytes());
    s.extend_from_slice(b"e12:min intervali");
    s.extend_from_slice(st.config.announce_interval.to_string().as_bytes());
    s.extend_from_slice(b"e6:peers6");
    s.extend_from_slice(peers.len().to_string().as_bytes());
    s.push(b':');
    s.extend_from_slice(&peers);
    s.push(b'e');
    s
}

/// Port of `srv_scrape`.
pub fn srv_scrape(st: &mut State, ti: &TrackerInput, user: Option<i32>) -> Vec<u8> {
    if st.config.log_scrape {
        let sql = make_query(
            &st.db_names,
            "(?,?,?),",
            &[
                Arg::Hex(to_sql(&ti.ipv6)),
                Arg::Int(user.unwrap_or(0) as i64),
                Arg::Int(srv_time()),
            ],
        );
        st.scrape_log_buffer.push_str(&sql);
    }
    if !st.config.anonymous_scrape && user.is_none() {
        return b"d14:failure reason25:unregistered torrent passe".to_vec();
    }
    let mut d: Vec<u8> = Vec::new();
    d.extend_from_slice(b"d5:filesd");
    let append_file = |d: &mut Vec<u8>, hash: &[u8], seeders: i32, completed: i32, leechers: i32| {
        d.extend_from_slice(b"20:");
        d.extend_from_slice(hash);
        d.extend_from_slice(b"d8:completei");
        d.extend_from_slice(seeders.to_string().as_bytes());
        d.extend_from_slice(b"e10:downloadedi");
        d.extend_from_slice(completed.to_string().as_bytes());
        d.extend_from_slice(b"e10:incompletei");
        d.extend_from_slice(leechers.to_string().as_bytes());
        d.extend_from_slice(b"ee");
    };
    if ti.info_hashes.is_empty() {
        st.stats.scraped_full += 1;
        d.reserve(90 * st.torrents.len());
        for (hash, t) in st.torrents.iter() {
            if t.leechers != 0 || t.seeders != 0 {
                append_file(&mut d, hash, t.seeders, t.completed, t.leechers);
            }
        }
    } else {
        st.stats.scraped_http += 1;
        if ti.info_hashes.len() > 1 {
            st.stats.scraped_multi += 1;
        }
        for i in &ti.info_hashes {
            if let Some(t) = st.find_torrent(i) {
                append_file(&mut d, i, t.seeders, t.completed, t.leechers);
            }
        }
    }
    d.push(b'e');
    if st.config.scrape_interval != 0 {
        d.extend_from_slice(b"5:flagsd20:min_request_intervali");
        d.extend_from_slice(st.config.scrape_interval.to_string().as_bytes());
        d.extend_from_slice(b"ee");
    }
    d.push(b'e');
    d
}

/// Port of `debug(const torrent_t&, string&)`.
fn debug_torrent(t: &Torrent, os: &mut String) {
    os.push_str("<tr><th>IPv4<th>IPv6<th>Port<th>UID<th>Seeder<th>Modified<th>Peer ID");
    for (pid, p) in &t.peers {
        os.push_str("<tr>");
        os.push_str("<td>");
        os.push_str(&inet_ntoa4(p.ipv4));
        os.push_str("<td>");
        os.push_str(&inet_ntoa16(p.ipv6));
        os.push_str("<td class=ar>");
        os.push_str(&p.port.to_string());
        os.push_str("<td class=ar>");
        os.push_str(&p.uid.to_string());
        os.push_str("<td class=ar>");
        os.push_str(if !p.left { "1" } else { "0" });
        os.push_str("<td class=ar>");
        os.push_str(&duration2a((srv_time() - p.mtime) as f32));
        os.push_str(" ago");
        os.push_str("<td>");
        os.push_str(&hex_encode(pid));
    }
}

/// Port of `srv_debug`.
pub fn srv_debug(st: &State, ti: &TrackerInput) -> String {
    let mut os = String::new();
    os.push_str("<!DOCTYPE HTML><meta http-equiv=refresh content=60><title>XBT Tracker</title>");
    os.push_str("<table>");
    if ti.info_hash.is_empty() {
        for (hash, t) in &st.torrents {
            if t.leechers == 0 && t.seeders == 0 {
                continue;
            }
            os.push_str("<tr><td class=ar>");
            os.push_str(&t.tid.to_string());
            os.push_str("<td><a href=\"?info_hash=");
            os.push_str(&uri_encode(hash));
            os.push_str("\">");
            os.push_str(&hex_encode(hash));
            os.push_str("</a>");
            os.push_str("<td>");
            // C++ quirk: `os << (dirty ? '*' : ' ')` streams the ternary as
            // an int (character constants are ints in C++), printing 42/32.
            os.push_str(if t.dirty { "42" } else { "32" });
            os.push_str("<td class=ar>");
            os.push_str(&t.leechers.to_string());
            os.push_str("<td class=ar>");
            os.push_str(&t.seeders.to_string());
        }
    } else if let Some(t) = st.find_torrent(&ti.info_hash) {
        debug_torrent(t, &mut os);
    }
    os.push_str("</table>");
    os
}

/// Port of `srv_statistics`.
pub fn srv_statistics(st: &State) -> String {
    let mut os = String::new();
    os.push_str("<!DOCTYPE HTML><meta http-equiv=refresh content=60><title>XBT Tracker</title>");
    os.push_str("<style>.ar { text-align: right }</style>");
    let mut leechers = 0i64;
    let mut seeders = 0i64;
    let mut torrents = 0i64;
    for t in st.torrents.values() {
        leechers += t.leechers as i64;
        seeders += t.seeders as i64;
        torrents += (t.leechers != 0 || t.seeders != 0) as i64;
    }
    let peers = leechers + seeders;
    let t = srv_time();
    let up_time = std::cmp::max(1, t - st.stats.start_time);
    let g = &st.stats;
    os.push_str("<table>");
    os.push_str("<tr><td>peers<td class=ar>");
    os.push_str(&peers.to_string());
    if peers != 0 {
        os.push_str("<tr><td>seeders<td class=ar>");
        os.push_str(&n2a(seeders));
        os.push_str("<td class=ar>");
        os.push_str(&(seeders * 100 / peers).to_string());
        os.push_str(" %<tr><td>leechers<td class=ar>");
        os.push_str(&n2a(leechers));
        os.push_str("<td class=ar>");
        os.push_str(&(leechers * 100 / peers).to_string());
        os.push_str(" %");
    }
    os.push_str("<tr><td>torrents<td class=ar>");
    os.push_str(&n2a(torrents));
    os.push_str("<tr><td>");
    os.push_str("<tr><td>accepted tcp<td class=ar>");
    os.push_str(&n2a(g.accepted_tcp));
    os.push_str("<td class=ar>");
    os.push_str(&(g.accepted_tcp / up_time).to_string());
    os.push_str(" /s<tr><td>slow tcp<td class=ar>");
    os.push_str(&n2a(g.slow_tcp));
    os.push_str("<td class=ar>");
    os.push_str(&(g.slow_tcp / up_time).to_string());
    os.push_str(" /s<tr><td>accept errors<td class=ar>");
    os.push_str(&n2a(g.accept_errors));
    os.push_str("<tr><td>received udp<td class=ar>");
    os.push_str(&n2a(g.received_udp));
    os.push_str("<td class=ar>");
    os.push_str(&(g.received_udp / up_time).to_string());
    os.push_str(" /s<tr><td>sent udp<td class=ar>");
    os.push_str(&n2a(g.sent_udp));
    os.push_str("<td class=ar>");
    os.push_str(&(g.sent_udp / up_time).to_string());
    os.push_str(" /s");
    if g.announced() != 0 {
        os.push_str("<tr><td>announced<td class=ar>");
        os.push_str(&n2a(g.announced()));
        os.push_str("<td class=ar>");
        os.push_str(&(g.announced() * 100 / g.accepted_tcp).to_string());
        os.push_str(" %<tr><td>announced http <td class=ar>");
        os.push_str(&n2a(g.announced_http));
        os.push_str("<td class=ar>");
        os.push_str(&(g.announced_http * 100 / g.announced()).to_string());
        os.push_str(" %<tr><td>announced udp<td class=ar>");
        os.push_str(&n2a(g.announced_udp));
        os.push_str("<td class=ar>");
        os.push_str(&(g.announced_udp * 100 / g.announced()).to_string());
        os.push_str(" %");
    }
    os.push_str("<tr><td>scraped full<td class=ar>");
    os.push_str(&n2a(g.scraped_full));
    os.push_str("<tr><td>scraped multi<td class=ar>");
    os.push_str(&n2a(g.scraped_multi));
    if g.scraped() != 0 {
        os.push_str("<tr><td>scraped<td class=ar>");
        os.push_str(&n2a(g.scraped()));
        os.push_str("<td class=ar>");
        os.push_str(&(g.scraped() * 100 / g.accepted_tcp).to_string());
        os.push_str(" %<tr><td>scraped http<td class=ar>");
        os.push_str(&n2a(g.scraped_http));
        os.push_str("<td class=ar>");
        os.push_str(&(g.scraped_http * 100 / g.scraped()).to_string());
        os.push_str(" %<tr><td>scraped udp<td class=ar>");
        os.push_str(&n2a(g.scraped_udp));
        os.push_str("<td class=ar>");
        os.push_str(&(g.scraped_udp * 100 / g.scraped()).to_string());
        os.push_str(" %");
    }
    os.push_str("<tr><td>");
    os.push_str("<tr><td>up time<td class=ar>");
    os.push_str(&duration2a(up_time as f32));
    os.push_str("<tr><td>");
    os.push_str("<tr><td>anonymous announce<td class=ar>");
    os.push_str(&(st.config.anonymous_announce as i32).to_string());
    os.push_str("<tr><td>anonymous scrape<td class=ar>");
    os.push_str(&(st.config.anonymous_scrape as i32).to_string());
    os.push_str("<tr><td>auto register<td class=ar>");
    os.push_str(&(st.config.auto_register as i32).to_string());
    os.push_str("<tr><td>full scrape<td class=ar>");
    os.push_str(&(st.config.full_scrape as i32).to_string());
    os.push_str("<tr><td>read config time<td class=ar>");
    os.push_str(&(t - st.read_config_time).to_string());
    os.push_str(" / ");
    os.push_str(&st.config.read_config_interval.to_string());
    os.push_str("<tr><td>clean up time<td class=ar>");
    os.push_str(&(t - st.clean_up_time).to_string());
    os.push_str(" / ");
    os.push_str(&st.config.clean_up_interval.to_string());
    os.push_str("<tr><td>read db files time<td class=ar>");
    os.push_str(&(t - st.read_db_torrents_time).to_string());
    os.push_str(" / ");
    os.push_str(&st.config.read_db_interval.to_string());
    os.push_str("<tr><td>read db users time<td class=ar>");
    os.push_str(&(t - st.read_db_users_time).to_string());
    os.push_str(" / ");
    os.push_str(&st.config.read_db_interval.to_string());
    os.push_str("<tr><td>write db files time<td class=ar>");
    os.push_str(&(t - st.write_db_torrents_time).to_string());
    os.push_str(" / ");
    os.push_str(&st.config.write_db_interval.to_string());
    os.push_str("<tr><td>write db users time<td class=ar>");
    os.push_str(&(t - st.write_db_users_time).to_string());
    os.push_str(" / ");
    os.push_str(&st.config.write_db_interval.to_string());
    os.push_str("</table>");
    os
}

/// Port of `srv_metrics`.
pub fn srv_metrics(st: &State) -> String {
    let mut os = String::new();
    let mut leechers = 0i64;
    let mut seeders = 0i64;
    let mut torrents = 0i64;
    for t in st.torrents.values() {
        leechers += t.leechers as i64;
        seeders += t.seeders as i64;
        torrents += (t.leechers != 0 || t.seeders != 0) as i64;
    }
    let t = srv_time();
    let up_time = std::cmp::max(1, t - st.stats.start_time);
    let g = &st.stats;
    os.push_str(&format!("xbt_uptime {}\n", up_time));
    os.push_str(&format!("xbt_infohashes_count {}\n", torrents));
    os.push_str(&format!("xbt_peers_count{{type=\"seeders\"}} {}\n", seeders));
    os.push_str(&format!("xbt_peers_count{{type=\"leechers\"}} {}\n", leechers));
    os.push_str(&format!(
        "xbt_connections_count{{proto=\"tcp\",type=\"accepted\"}} {}\n",
        g.accepted_tcp
    ));
    os.push_str(&format!(
        "xbt_connections_count{{proto=\"tcp\",type=\"slow\"}} {}\n",
        g.slow_tcp
    ));
    os.push_str(&format!(
        "xbt_connections_count{{proto=\"tcp\",type=\"accept_errors\"}} {}\n",
        g.accept_errors
    ));
    os.push_str(&format!(
        "xbt_connections_count{{proto=\"udp\",type=\"received\"}} {}\n",
        g.received_udp
    ));
    os.push_str(&format!(
        "xbt_connections_count{{proto=\"udp\",type=\"sent\"}} {}\n",
        g.sent_udp
    ));
    if g.announced() != 0 {
        os.push_str(&format!(
            "xbt_requests_count{{proto=\"http\",method=\"announce\"}} {}\n",
            g.announced_http
        ));
        os.push_str(&format!(
            "xbt_requests_count{{proto=\"udp\",method=\"announce\"}} {}\n",
            g.announced_udp
        ));
    }
    if g.scraped() != 0 {
        os.push_str(&format!(
            "xbt_requests_count{{proto=\"http\",method=\"scrape\"}} {}\n",
            g.scraped_http
        ));
        os.push_str(&format!(
            "xbt_requests_count{{proto=\"udp\",method=\"scrape\"}} {}\n",
            g.scraped_udp
        ));
    }
    os
}

/// Port of `clean_up()` — drops peers not seen for 1.5 * announce_interval.
pub fn clean_up(sh: &Shared) {
    let mut st = sh.lock();
    let time = srv_time() - (1.5 * st.config.announce_interval as f64) as i64;
    for t in st.torrents.values_mut() {
        let mut removed_leechers = 0;
        let mut removed_seeders = 0;
        t.peers.retain(|_, p| {
            if p.mtime < time {
                if p.left {
                    removed_leechers += 1;
                } else {
                    removed_seeders += 1;
                }
                false
            } else {
                true
            }
        });
        if removed_leechers + removed_seeders > 0 {
            t.leechers -= removed_leechers;
            t.seeders -= removed_seeders;
            t.dirty = true;
        }
    }
    st.clean_up_time = srv_time();
}

/// Port of the body of `write_db_users()` — takes the buffers and produces the
/// SQL statements to fire (caller sends them to the DB layer).
pub fn flush_db_users(
    names: &HashMap<String, String>,
    peers_buf: &mut String,
    users_buf: &mut String,
    write_db_users_time: &mut i64,
    execs: &mut Vec<String>,
) {
    *write_db_users_time = srv_time();
    if !peers_buf.is_empty() {
        let mut raw = std::mem::take(peers_buf);
        raw.pop(); // drop trailing ','
        execs.push(make_query(names, SQL_UPSERT_PEERS, &[Arg::Raw(&raw)]));
    }
    execs.push(make_query(names, SQL_DEACTIVATE_PEERS, &[]));
    if !users_buf.is_empty() {
        let mut raw = std::mem::take(users_buf);
        raw.pop();
        execs.push(make_query(names, SQL_UPSERT_USERS, &[Arg::Raw(&raw)]));
    }
}

/// Port of `write_db_users()`.
pub async fn write_db_users(sh: &Shared) {
    let execs = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let mut execs = Vec::new();
        flush_db_users(
            &st.db_names,
            &mut st.torrents_users_updates_buffer,
            &mut st.users_updates_buffer,
            &mut st.write_db_users_time,
            &mut execs,
        );
        execs
    };
    for sql in execs {
        sh.db.exec_fire(sql);
    }
}

/// Port of `write_db_torrents()`.
pub async fn write_db_torrents(sh: &Shared) {
    {
        let mut st = sh.lock();
        st.write_db_torrents_time = srv_time();
    }
    // Insert rows for torrents without tid (auto registration) and collect
    // their insert_ids, like the original's synchronous loop.
    loop {
        let new_hashes: Vec<[u8; 20]> = {
            let st = sh.lock();
            st.torrents
                .iter()
                .filter(|(_, t)| t.dirty && t.tid == 0)
                .map(|(h, _)| *h)
                .collect()
        };
        if new_hashes.is_empty() {
            break;
        }
        let mut failed = false;
        for h in &new_hashes {
            let sql = {
                let st = sh.lock();
                make_query(&st.db_names, SQL_INSERT_TORRENT, &[Arg::Hex(h)])
            };
            match sh.db.insert(sql).await {
                Ok(tid) => {
                    let mut st = sh.lock();
                    if let Some(t) = st.torrents.get_mut(h) {
                        t.tid = tid as i32;
                    }
                }
                Err(_) => {
                    // The original aborts the whole write cycle on a failed
                    // query; the torrent keeps tid == 0 and stays dirty, so it
                    // is retried on the next cycle.
                    failed = true;
                }
            }
        }
        if failed {
            // mirror `catch (bad_query&)`: skip remaining work this cycle
            return;
        }
    }
    // Build the batched count updates + log flushes under the lock.
    let execs = {
        let mut st = sh.lock();
        let mut execs: Vec<String> = Vec::new();
        let mut buffer = String::new();
        let names = st.db_names.clone();
        for t in st.torrents.values_mut() {
            if !t.dirty {
                continue;
            }
            if t.tid == 0 {
                continue; // insert failed above; retry next cycle
            }
            buffer.push_str(&make_query(
                &names,
                "(?,?,?,?),",
                &[
                    Arg::Int(t.leechers as i64),
                    Arg::Int(t.seeders as i64),
                    Arg::Int(t.completed as i64),
                    Arg::Int(t.tid as i64),
                ],
            ));
            t.dirty = false;
            if buffer.len() > 255 << 10 {
                buffer.pop();
                execs.push(make_query(&names, SQL_UPSERT_TORRENTS, &[Arg::Raw(&buffer)]));
                buffer.clear();
            }
        }
        if !buffer.is_empty() {
            buffer.pop();
            execs.push(make_query(&names, SQL_UPSERT_TORRENTS, &[Arg::Raw(&buffer)]));
        }
        if !st.announce_log_buffer.is_empty() {
            let mut raw = std::mem::take(&mut st.announce_log_buffer);
            raw.pop();
            execs.push(make_query(&names, SQL_INSERT_ANNOUNCE_LOG, &[Arg::Raw(&raw)]));
        }
        if !st.scrape_log_buffer.is_empty() {
            let mut raw = std::mem::take(&mut st.scrape_log_buffer);
            raw.pop();
            execs.push(make_query(&names, SQL_INSERT_SCRAPE_LOG, &[Arg::Raw(&raw)]));
        }
        execs
    };
    for sql in execs {
        sh.db.exec_fire(sql);
    }
}

/// Port of `read_db_torrents()`.
pub async fn read_db_torrents(sh: &Shared) {
    {
        let mut st = sh.lock();
        st.read_db_torrents_time = srv_time();
    }
    let (auto_register, sql) = {
        let st = sh.lock();
        (
            st.config.auto_register,
            make_query(&st.db_names, SQL_SELECT_UNREGISTERED, &[]),
        )
    };
    if !auto_register {
        match sh.db.query(sql).await {
            Err(_) => return, // catch (bad_query&)
            Ok(rows) => {
                let mut execs = Vec::new();
                {
                    let mut st = sh.lock();
                let names = st.db_names.clone();
                for row in &rows {
                    let hash = to_array20(row_bytes(row, 0));
                    execs.push(make_query(&names, SQL_DELETE_TORRENT, &[Arg::Int(row_int(row, 1))]));
                    st.torrents.remove(&hash);
                }
                }
                for sql in execs {
                    sh.db.exec_fire(sql);
                }
            }
        }
    }
    {
        let st = sh.lock();
        if st.config.auto_register && !st.torrents.is_empty() {
            return;
        }
    }
    let sql = {
        let st = sh.lock();
        make_query(&st.db_names, SQL_SELECT_TORRENTS, &[Arg::Int(st.tid_end as i64)])
    };
    match sh.db.query(sql).await {
        Err(_) => return,
        Ok(rows) => {
            let mut st = sh.lock();
            for row in &rows {
                let tid = row_int(row, 2) as i32;
                st.tid_end = std::cmp::max(st.tid_end, tid.wrapping_add(1));
                let ih = row_bytes(row, 0);
                if ih.len() != 20 || st.find_torrent(ih).is_some() {
                    continue;
                }
                let mut key = [0u8; 20];
                key.copy_from_slice(ih);
                let t = st.torrents.entry(key).or_default();
                if t.tid != 0 {
                    continue;
                }
                t.completed = row_int(row, 1) as i32;
                t.dirty = false;
                t.tid = tid;
                t.ctime = row_int(row, 3);
            }
        }
    }
}

/// Port of `read_db_users()`.
pub async fn read_db_users(sh: &Shared) {
    let (sql, flags) = {
        let mut st = sh.lock();
        st.read_db_users_time = srv_time();
        let mut q = make_query(&st.db_names, "select @uid", &[]);
        if st.read_users_can_leech {
            q.push_str(", can_leech");
        }
        if st.read_users_peers_limit {
            q.push_str(", peers_limit");
        }
        if st.read_users_torrent_pass {
            q.push_str(", torrent_pass");
        }
        if st.read_users_torrent_pass_version {
            q.push_str(", torrent_pass_version");
        }
        if st.read_users_wait_time {
            q.push_str(", wait_time");
        }
        q.push_str(&make_query(&st.db_names, " from @users", &[]));
        (
            q,
            (
                st.read_users_can_leech,
                st.read_users_peers_limit,
                st.read_users_torrent_pass,
                st.read_users_torrent_pass_version,
                st.read_users_wait_time,
            ),
        )
    };
    let rows = match sh.db.query(sql).await {
        Err(_) => return, // catch (bad_query&)
        Ok(rows) => rows,
    };
    let (f_can_leech, f_peers_limit, f_torrent_pass, f_tpv, f_wait_time) = flags;
    let mut st = sh.lock();
    for u in st.users.values_mut() {
        u.marked = true;
    }
    st.users_torrent_passes.clear();
    for row in &rows {
        let uid = row_int(row, 0) as i32;
        let mut c = 0usize;
        {
            let user = st.users.entry(uid).or_default();
            user.marked = false;
            user.uid = row_int(row, c) as i32;
            c += 1;
            if f_can_leech {
                user.can_leech = row_int(row, c) != 0;
                c += 1;
            }
            if f_peers_limit {
                user.peers_limit = row_int(row, c) as i32;
                c += 1;
            }
            if f_tpv {
                user.torrent_pass_version = row_int(row, c) as i32;
            }
            if f_wait_time {
                user.wait_time = row_int(row, if f_torrent_pass { c + 1 } else { c }) as i32;
            }
        }
        if f_torrent_pass {
            let idx = if f_peers_limit { 3 } else if f_can_leech { 2 } else { 1 };
            let v = row_bytes(row, idx);
            if v.len() == 32 {
                let mut key = [0u8; 32];
                key.copy_from_slice(v);
                st.users_torrent_passes.insert(key, uid);
            }
        }
    }
    st.users.retain(|_, u| !u.marked);
}

/// Port of `read_config()`.
pub async fn read_config(sh: &Shared) {
    let (sql, conf_file) = {
        let st = sh.lock();
        (
            make_query(&st.db_names, SQL_SELECT_CONFIG, &[]),
            st.conf_file.clone(),
        )
    };
    let mut config = Config::default();
    let mut ok = true;
    match sh.db.query(sql).await {
        Err(_) => ok = false, // catch (bad_query&) skips the assignment below
        Ok(rows) => {
            for row in &rows {
                let name = row_bytes(row, 0);
                let value = row_bytes(row, 1);
                if config.set_str(name, value) {
                    eprintln!("unknown config name: {}", String::from_utf8_lossy(name));
                }
            }
        }
    }
    if ok {
        config.load_file(&conf_file); // file overrides DB rows, like the original
        if config.torrent_pass_private_key.is_empty() {
            config.torrent_pass_private_key = generate_random_string(27);
            let sql = {
                let st = sh.lock();
                make_query(
                    &st.db_names,
                    SQL_INSERT_CONFIG_KEY,
                    &[Arg::Str(config.torrent_pass_private_key.as_bytes())],
                )
            };
            // original uses query() (throwing); failure skips the assignment
            if sh.db.query(sql).await.is_err() {
                ok = false;
            }
        }
    }
    if ok {
        let mut st = sh.lock();
        // g_database.set_name(...) calls from read_config
        st.db_names.insert("completed".into(), config.column_torrents_completed.clone());
        st.db_names.insert("leechers".into(), config.column_torrents_leechers.clone());
        st.db_names.insert("seeders".into(), config.column_torrents_seeders.clone());
        st.db_names.insert("tid".into(), config.column_torrents_tid.clone());
        st.db_names.insert("uid".into(), config.column_users_uid.clone());
        let p = st.table_prefix.clone();
        let p = p.as_str();
        st.db_names.insert(
            "announce_log".into(),
            if config.table_announce_log.is_empty() {
                format!("{}announce_log", p)
            } else {
                config.table_announce_log.clone()
            },
        );
        st.db_names.insert(
            "scrape_log".into(),
            if config.table_scrape_log.is_empty() {
                format!("{}scrape_log", p)
            } else {
                config.table_scrape_log.clone()
            },
        );
        st.db_names.insert(
            "torrents".into(),
            if config.table_torrents.is_empty() {
                format!("{}torrents", p)
            } else {
                config.table_torrents.clone()
            },
        );
        st.db_names.insert(
            "torrents_users".into(),
            if config.table_torrents_users.is_empty() {
                format!("{}peers", p)
            } else {
                config.table_torrents_users.clone()
            },
        );
        st.db_names.insert(
            "users".into(),
            if config.table_users.is_empty() {
                format!("{}users", p)
            } else {
                config.table_users.clone()
            },
        );
        st.config = config;
    }
    let mut st = sh.lock();
    if st.config.listen_ports.is_empty() {
        st.config.listen_ports.insert(2710);
    }
    st.read_config_time = srv_time();
}

/// Port of `test_sql()`. Returns true when the schema check passed.
pub async fn test_sql(sh: &Shared) -> bool {
    let (log_announce, log_scrape) = {
        let st = sh.lock();
        (st.config.log_announce, st.config.log_scrape)
    };
    let sqls: Vec<String> = {
        let st = sh.lock();
        let mut v = Vec::new();
        if log_announce {
            v.push(make_query(
                &st.db_names,
                "select id, ipv6, port, event, info_hash, peer_id, downloaded, left0, uploaded, uid, mtime from @announce_log where 0",
                &[],
            ));
        }
        v.push(make_query(&st.db_names, "select name, value from @config where 0", &[]));
        v.push(make_query(
            &st.db_names,
            "select @tid, info_hash, @leechers, @seeders, flags, mtime, ctime from @torrents where 0",
            &[],
        ));
        v.push(make_query(
            &st.db_names,
            "select tid, uid, active, completed, downloaded, `left`, uploaded from @torrents_users where 0",
            &[],
        ));
        if log_scrape {
            v.push(make_query(
                &st.db_names, "select id, ipv6, uid, mtime from @scrape_log where 0", &[],
            ));
        }
        v.push(make_query(
            &st.db_names, "select @uid, torrent_pass, downloaded, uploaded from @users where 0", &[],
        ));
        v
    };
    for sql in sqls {
        if sh.db.query(sql).await.is_err() {
            return false;
        }
    }
    for (col, flag) in [
        ("can_leech", 0),
        ("peers_limit", 1),
        ("torrent_pass", 2),
        ("torrent_pass_version", 3),
        ("wait_time", 4),
    ] {
        let sql = {
            let st = sh.lock();
            make_query(&st.db_names, &format!("show columns from @users like '{}'", col), &[])
        };
        let present = match sh.db.query(sql).await {
            Err(_) => return false,
            Ok(rows) => !rows.is_empty(), // Csql_result::size() -> mysql_num_rows
        };
        let mut st = sh.lock();
        match flag {
            0 => st.read_users_can_leech = present,
            1 => st.read_users_peers_limit = present,
            2 => st.read_users_torrent_pass = present,
            3 => st.read_users_torrent_pass_version = present,
            _ => st.read_users_wait_time = present,
        }
    }
    true
}

/// The `update @torrents set @leechers = 0, @seeders = 0` startup statement.
pub fn sql_update_zero_counts(names: &HashMap<String, String>) -> String {
    make_query(names, SQL_UPDATE_ZERO_COUNTS, &[])
}

/// Compute the torrent pass (32 hex chars) for a user, like the XBT web UI
/// does: hex(uid) + hex(sha1("{key} {version} {uid} {info_hash}")[0..12]).
/// `info_hash` selects the torrent; an empty slice yields the "generic" pass
/// only valid for torrents registered with the same hash input.
pub fn compute_torrent_pass(key: &str, torrent_pass_version: i32, uid: i32, info_hash: &[u8]) -> String {
    let mut s = format!("{} {} {} ", key, torrent_pass_version, uid).into_bytes();
    s.extend_from_slice(info_hash);
    let digest = Sha1::digest(&s);
    format!("{}{}", crate::bt_misc::hex_encode_int(8, uid as i64), hex_encode(&digest[..12]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bt_misc::to_int;
    use crate::tracker_input::{addr_to_ipv6_bytes, E_COMPLETED, E_NONE, E_STARTED};

    fn test_state() -> State {
        let mut st = State::new("xbt_tracker.conf".into(), "xbt_".into(), 0x0123456789abcdef);
        st.config.anonymous_announce = true;
        st.config.anonymous_scrape = true;
        st.config.auto_register = true;
        st.config.full_scrape = true;
        st.config.torrent_pass_private_key = "testkey".into();
        st
    }

    fn ti(hash: [u8; 20], pid: [u8; 20], left: i64, event: i64) -> TrackerInput {
        let mut t = TrackerInput::default();
        t.info_hash = hash.to_vec();
        t.peer_id = pid;
        t.left = left;
        t.event = event;
        t.port = 6881;
        t.ipv6 = addr_to_ipv6_bytes(&"10.0.0.1:1".parse().unwrap());
        t
    }

    #[test]
    fn test_insert_peer_and_select() {
        let mut st = test_state();
        let mut execs = vec![];
        let ih = [1u8; 20];
        let pid = [2u8; 20];
        // announce started, leecher
        let e = srv_insert_peer(&mut st, &ti(ih, pid, 100, E_STARTED), false, None, &mut execs);
        assert_eq!(e, "");
        let t = st.find_torrent(&ih).unwrap();
        assert_eq!(t.leechers, 1);
        assert_eq!(t.seeders, 0);
        assert!(t.dirty);
        // response
        let resp = srv_select_peers(&st, &ti(ih, pid, 100, E_STARTED));
        let mut expect: Vec<u8> = b"d8:completei0e10:incompletei1e8:intervali1800e12:min intervali1800e5:peers6:".to_vec();
        expect.extend_from_slice(&[0x0a, 0x00, 0x00, 0x01, 0x1a, 0xe1]); // 10.0.0.1:6881
        expect.push(b'e');
        assert_eq!(resp, expect);
        // completed -> seeder
        let e = srv_insert_peer(&mut st, &ti(ih, pid, 0, E_COMPLETED), false, None, &mut execs);
        assert_eq!(e, "");
        let t = st.find_torrent(&ih).unwrap();
        assert_eq!(t.leechers, 0);
        assert_eq!(t.seeders, 1);
        assert_eq!(t.completed, 1);
        // stopped removes
        let e = srv_insert_peer(&mut st, &ti(ih, pid, 0, E_STOPPED), false, None, &mut execs);
        assert_eq!(e, "");
        let t = st.find_torrent(&ih).unwrap();
        assert_eq!(t.seeders, 0);
        assert!(t.peers.is_empty());
        let resp = srv_select_peers(&st, &ti(ih, pid, 0, E_STOPPED));
        assert!(resp.ends_with(b"5:peers0:e"));
    }

    #[test]
    fn test_scrape() {
        let mut st = test_state();
        let mut execs = vec![];
        let ih = [3u8; 20];
        srv_insert_peer(&mut st, &ti(ih, [9u8; 20], 0, E_NONE), false, None, &mut execs);
        // single scrape
        let mut t = TrackerInput::default();
        t.info_hash = ih.to_vec();
        t.info_hashes.push(ih.to_vec());
        t.ipv6 = addr_to_ipv6_bytes(&"10.0.0.2:1".parse().unwrap());
        let r = srv_scrape(&mut st, &t, None);
        // seeder -> complete=1 incomplete=0
        {
            let mut expect = Vec::from(b"d5:filesd20:");
            expect.extend_from_slice(&ih);
            expect.extend_from_slice(b"d8:completei1e10:downloadedi0e10:incompletei0eeee");
            assert_eq!(r, expect);
        }
        // full scrape includes only torrents with peers; unknown hash scrape omits
        let mut t2 = TrackerInput::default();
        t2.ipv6 = t.ipv6;
        t2.info_hashes.push([0xff; 20].to_vec());
        let r2 = srv_scrape(&mut st, &t2, None);
        assert_eq!(r2, b"d5:filesdee");
        assert_eq!(st.stats.scraped_http, 2);
        assert_eq!(st.stats.scraped_multi, 0);
        // full scrape (no info_hashes)
        let r3 = srv_scrape(&mut st, &TrackerInput { ipv6: t.ipv6, ..Default::default() }, None);
        assert!(r3.starts_with(b"d5:filesd20:"));
        assert!(r3.ends_with(b"ee"));
        assert_eq!(st.stats.scraped_full, 1);
    }

    #[test]
    fn test_torrent_pass_lookup() {
        let mut st = test_state();
        // dynamic pass for uid 5, version 2
        st.users.insert(
            5,
            User { uid: 5, torrent_pass_version: 2, ..Default::default() },
        );
        let ih = [7u8; 20];
        let pass = compute_torrent_pass("testkey", 2, 5, &ih);
        assert_eq!(pass.len(), 32);
        assert_eq!(st.find_user_by_torrent_pass(pass.as_bytes(), &ih), Some(5));
        // wrong info hash -> no match
        assert_eq!(st.find_user_by_torrent_pass(pass.as_bytes(), &[8u8; 20]), None);
        // static pass map
        st.users.insert(6, User { uid: 6, ..Default::default() });
        let static_pass = [b's'; 32];
        st.users_torrent_passes.insert(static_pass, 6);
        assert_eq!(st.find_user_by_torrent_pass(&static_pass, &ih), Some(6));
        assert_eq!(st.find_user_by_torrent_pass(b"short", &ih), None);
    }

    #[test]
    fn test_access_rules() {
        let mut st = test_state();
        let mut execs = vec![];
        let ih = [4u8; 20];
        // not anonymous
        st.config.anonymous_announce = false;
        assert_eq!(
            srv_insert_peer(&mut st, &ti(ih, [1u8; 20], 1, E_NONE), false, None, &mut execs),
            BTS_UNREGISTERED_TORRENT_PASS
        );
        st.config.anonymous_announce = true;
        // not auto registered
        st.config.auto_register = false;
        assert_eq!(
            srv_insert_peer(&mut st, &ti(ih, [1u8; 20], 1, E_NONE), false, None, &mut execs),
            BTS_UNREGISTERED_TORRENT
        );
        st.config.auto_register = true;
        // user rules
        st.users.insert(
            9,
            User { uid: 9, can_leech: false, wait_time: 0, peers_limit: 0, ..Default::default() },
        );
        assert_eq!(
            srv_insert_peer(&mut st, &ti(ih, [1u8; 20], 1, E_NONE), false, Some(9), &mut execs),
            BTS_CAN_NOT_LEECH
        );
        st.users.insert(
            10,
            User { uid: 10, can_leech: true, wait_time: 3600, ..Default::default() },
        );
        assert_eq!(
            srv_insert_peer(&mut st, &ti(ih, [1u8; 20], 1, E_NONE), false, Some(10), &mut execs),
            BTS_WAIT_TIME
        );
        // peers limit
        st.users.insert(
            11,
            User { uid: 11, can_leech: true, wait_time: 0, peers_limit: 1, ..Default::default() },
        );
        let mut t1 = ti(ih, [1u8; 20], 1, E_NONE);
        t1.left = 5;
        assert_eq!(srv_insert_peer(&mut st, &t1, false, Some(11), &mut execs), "");
        let mut t2 = ti(ih, [2u8; 20], 1, E_NONE);
        t2.left = 5;
        assert_eq!(
            srv_insert_peer(&mut st, &t2, false, Some(11), &mut execs),
            BTS_PEERS_LIMIT_REACHED
        );
        // seeding is allowed despite peers_limit (left == 0)
        let mut t3 = ti(ih, [3u8; 20], 0, E_NONE);
        t3.left = 0;
        assert_eq!(srv_insert_peer(&mut st, &t3, false, Some(11), &mut execs), "");
        // user stat buffers: tid is 0 in-memory only, so nothing buffered yet
        assert!(st.torrents_users_updates_buffer.is_empty());
    }

    #[test]
    fn test_user_accounting_buffers() {
        let mut st = test_state();
        let mut execs = vec![];
        let ih = [5u8; 20];
        st.torrents.entry(ih).or_default().tid = 42;
        st.users.insert(3, User { uid: 3, ..Default::default() });
        let mut a = ti(ih, [6u8; 20], 1000, E_STARTED);
        a.downloaded = 100;
        a.uploaded = 50;
        assert_eq!(srv_insert_peer(&mut st, &a, false, Some(3), &mut execs), "");
        // peers row: active=1, completed=0, dl=0 (no previous), left=1000, ul=0, tid=42, uid=3
        assert!(st.torrents_users_updates_buffer.contains(",1000,0,") || st.torrents_users_updates_buffer.contains(",1000,"));
        let n = st.torrents_users_updates_buffer.matches("),(").count() + 1;
        assert_eq!(n, 1);
        // second announce with increased counters -> deltas buffered
        let mut b = ti(ih, [6u8; 20], 500, E_NONE);
        b.downloaded = 250;
        b.uploaded = 150;
        assert_eq!(srv_insert_peer(&mut st, &b, false, Some(3), &mut execs), "");
        assert!(st.users_updates_buffer.contains("(150,100,3),"));
        // decreasing counters -> no delta
        let mut c = ti(ih, [6u8; 20], 500, E_NONE);
        c.downloaded = 10;
        c.uploaded = 10;
        assert_eq!(srv_insert_peer(&mut st, &c, false, Some(3), &mut execs), "");
        let cnt = st.users_updates_buffer.matches("(0,0,3)").count();
        assert_eq!(cnt, 0);
    }

    #[test]
    fn test_flush_db_users() {
        let mut names = HashMap::new();
        names.insert("torrents_users".into(), "xbt_peers".into());
        names.insert("users".into(), "xbt_users".into());
        names.insert("uid".into(), "uid".into());
        let mut peers_buf = String::from("(1,0,0,100,0,1700000000,42,3),");
        let mut users_buf = String::from("(150,100,3),");
        let mut wtime = 0i64;
        let mut execs = vec![];
        flush_db_users(&names, &mut peers_buf, &mut users_buf, &mut wtime, &mut execs);
        assert_eq!(execs.len(), 3);
        assert!(execs[0].starts_with("insert ignore into xbt_peers (active, completed, downloaded, `left`, uploaded, mtime, tid, uid) values (1,0,0,100,0,1700000000,42,3) on duplicate key update"));
        assert_eq!(execs[1], "update xbt_peers set active = 0 where active = 1 and mtime < unix_timestamp() - 60 * 60");
        assert_eq!(execs[2], "insert ignore into xbt_users (downloaded, uploaded, uid) values (150,100,3) on duplicate key update  downloaded = downloaded + values(downloaded),  uploaded = uploaded + values(uploaded)");
        assert!(peers_buf.is_empty());
        assert!(users_buf.is_empty());
        assert!(wtime > 0);
    }

    #[test]
    fn test_clean_up() {
        let sh_db_placeholder = Db::dummy();
        let sh = Shared {
            state: Mutex::new(test_state()),
            db: sh_db_placeholder,
            access_log: Mutex::new(None),
        };
        let ih = [6u8; 20];
        let mut execs = vec![];
        {
            let mut st = sh.lock();
            st.config.anonymous_announce = true;
            st.config.auto_register = true;
            let mut a = ti(ih, [1u8; 20], 0, E_NONE);
            a.left = 0;
            srv_insert_peer(&mut st, &a, false, None, &mut execs);
            let mut b = ti(ih, [2u8; 20], 10, E_NONE);
            b.left = 10;
            srv_insert_peer(&mut st, &b, false, None, &mut execs);
            // make both peers old
            for p in st.torrents.get_mut(&ih).unwrap().peers.values_mut() {
                p.mtime -= 10_000;
            }
        }
        clean_up(&sh);
        let st = sh.lock();
        let t = st.torrents.get(&ih).unwrap();
        assert_eq!(t.leechers, 0);
        assert_eq!(t.seeders, 0);
        assert!(t.peers.is_empty());
        assert!(t.dirty);
    }

    #[test]
    fn test_to_int_alias() {
        assert_eq!(to_int(b"42"), 42);
    }
}
