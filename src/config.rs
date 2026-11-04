//! Port of Tracker/config.cpp + misc/config_base.h.
//!
//! Config sources, in the same order as the original:
//!   1. defaults
//!   2. `xbt_config` table rows (name, value)
//!   3. the conf file (overrides the DB rows, exactly like the original)

use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct Config {
    // bool attributes
    pub anonymous_announce: bool,
    pub anonymous_scrape: bool,
    pub auto_register: bool,
    pub daemon: bool,
    pub debug: bool,
    pub full_scrape: bool,
    pub gzip_scrape: bool,
    pub log_access: bool,
    pub log_announce: bool,
    pub log_scrape: bool,
    // int attributes
    pub announce_interval: i32,
    pub clean_up_interval: i32,
    pub read_config_interval: i32,
    pub read_db_interval: i32,
    pub scrape_interval: i32,
    pub write_db_interval: i32,
    /// Rust-port extension: TCP port of the MySQL server.
    /// 0 (default) keeps the original behavior: 9306 when the database is
    /// named "sphinx", 3306 otherwise.
    pub mysql_port: i32,
    // string attributes
    pub column_torrents_completed: String,
    pub column_torrents_tid: String,
    pub column_torrents_leechers: String,
    pub column_torrents_seeders: String,
    pub column_users_uid: String,
    pub mysql_database: String,
    pub mysql_host: String,
    pub mysql_password: String,
    pub mysql_table_prefix: String,
    pub mysql_user: String,
    pub offline_message: String,
    pub pid_file: String,
    pub query_log: String,
    pub redirect_url: String,
    pub table_announce_log: String,
    pub table_torrents: String,
    pub table_torrents_users: String,
    pub table_scrape_log: String,
    pub table_users: String,
    pub torrent_pass_private_key: String,
    // listen_port (int set)
    pub listen_ports: BTreeSet<i32>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            anonymous_announce: false,
            anonymous_scrape: false,
            auto_register: false,
            daemon: true,
            debug: false,
            full_scrape: false,
            gzip_scrape: true,
            log_access: false,
            log_announce: false,
            log_scrape: false,
            announce_interval: 1800,
            clean_up_interval: 60,
            read_config_interval: 60,
            read_db_interval: 60,
            scrape_interval: 0,
            write_db_interval: 15,
            mysql_port: 0,
            column_torrents_completed: "completed".into(),
            column_torrents_tid: "tid".into(),
            column_torrents_leechers: "leechers".into(),
            column_torrents_seeders: "seeders".into(),
            column_users_uid: "uid".into(),
            mysql_database: "xbt".into(),
            mysql_host: String::new(),
            mysql_password: String::new(),
            mysql_table_prefix: "xbt_".into(),
            mysql_user: String::new(),
            offline_message: String::new(),
            pid_file: String::new(),
            query_log: String::new(),
            redirect_url: String::new(),
            table_announce_log: String::new(),
            table_torrents: String::new(),
            table_torrents_users: String::new(),
            table_scrape_log: String::new(),
            table_users: String::new(),
            torrent_pass_private_key: String::new(),
            listen_ports: BTreeSet::new(),
        }
    }
}

/// Effective MySQL TCP port (mirrors the original's hardcoded
/// `database == "sphinx" ? 9306 : 0`, plus the new mysql_port option).
pub fn effective_mysql_port(cfg: &Config) -> u16 {
    if cfg.mysql_port != 0 {
        return cfg.mysql_port as u16;
    }
    if cfg.mysql_database == "sphinx" {
        9306
    } else {
        3306
    }
}

fn trim(s: &[u8]) -> &[u8] {
    let is_space = |c: u8| c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' || c == 0x0b || c == 0x0c;
    let mut a = 0usize;
    let mut b = s.len();
    while a < b && is_space(s[a]) {
        a += 1;
    }
    while b > a && is_space(s[b - 1]) {
        b -= 1;
    }
    &s[a..b]
}

impl Config {
    /// Port of `config_t::set(std::string_view name, std::string_view value)`.
    /// Returns true when the name is unknown (original returns non-zero).
    pub fn set_str(&mut self, name: &[u8], value: &[u8]) -> bool {
        // string attributes
        macro_rules! try_str {
            ($($n:literal => $f:ident),* $(,)?) => {
                $(if name == $n.as_bytes() {
                    self.$f = String::from_utf8_lossy(value).into_owned();
                    return false;
                })*
            };
        }
        try_str!(
            "column_files_completed" => column_torrents_completed,
            "column_files_fid" => column_torrents_tid,
            "column_files_leechers" => column_torrents_leechers,
            "column_files_seeders" => column_torrents_seeders,
            "column_users_uid" => column_users_uid,
            "mysql_database" => mysql_database,
            "mysql_host" => mysql_host,
            "mysql_password" => mysql_password,
            "mysql_table_prefix" => mysql_table_prefix,
            "mysql_user" => mysql_user,
            "offline_message" => offline_message,
            "pid_file" => pid_file,
            "query_log" => query_log,
            "redirect_url" => redirect_url,
            "table_announce_log" => table_announce_log,
            "table_files" => table_torrents,
            "table_files_users" => table_torrents_users,
            "table_scrape_log" => table_scrape_log,
            "table_users" => table_users,
            "torrent_pass_private_key" => torrent_pass_private_key,
        );
        self.set_int(name, crate::bt_misc::to_int(value) as i32)
    }

    /// Port of `config_t::set(name, int)`.
    pub fn set_int(&mut self, name: &[u8], value: i32) -> bool {
        macro_rules! try_int {
            ($($n:literal => $f:ident),* $(,)?) => {
                $(if name == $n.as_bytes() {
                    self.$f = value;
                    return false;
                })*
            };
        }
        try_int!(
            "announce_interval" => announce_interval,
            "clean_up_interval" => clean_up_interval,
            "read_config_interval" => read_config_interval,
            "read_db_interval" => read_db_interval,
            "scrape_interval" => scrape_interval,
            "write_db_interval" => write_db_interval,
            "mysql_port" => mysql_port,
        );
        if name == b"listen_port" {
            self.listen_ports.insert(value);
            return false;
        }
        self.set_bool(name, value != 0)
    }

    /// Port of `config_t::set(name, bool)`.
    pub fn set_bool(&mut self, name: &[u8], value: bool) -> bool {
        macro_rules! try_bool {
            ($($n:literal => $f:ident),* $(,)?) => {
                $(if name == $n.as_bytes() {
                    self.$f = value;
                    return false;
                })*
            };
        }
        try_bool!(
            "auto_register" => auto_register,
            "anonymous_announce" => anonymous_announce,
            "anonymous_scrape" => anonymous_scrape,
            "daemon" => daemon,
            "debug" => debug,
            "full_scrape" => full_scrape,
            "gzip_scrape" => gzip_scrape,
            "log_access" => log_access,
            "log_announce" => log_announce,
            "log_scrape" => log_scrape,
        );
        true // unknown
    }

    /// Port of `config_base_t::load(std::istream&)`: each line containing '='
    /// is split into a trimmed key and trimmed value.
    pub fn load_bytes(&mut self, data: &[u8]) {
        for line in data.split(|&c| c == b'\n') {
            if let Some(i) = line.iter().position(|&c| c == b'=') {
                let name = trim(&line[..i]);
                let value = trim(&line[i + 1..]);
                self.set_str(name, value);
            }
        }
    }

    /// Port of `config_base_t::load(const std::string& file)`.
    /// Returns true on failure ("Unable to read" case).
    pub fn load_file(&mut self, file: &str) -> bool {
        match std::fs::read(file) {
            Ok(data) => {
                self.load_bytes(&data);
                false
            }
            Err(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let c = Config::default();
        assert_eq!(c.announce_interval, 1800);
        assert_eq!(c.clean_up_interval, 60);
        assert_eq!(c.read_config_interval, 60);
        assert_eq!(c.read_db_interval, 60);
        assert_eq!(c.scrape_interval, 0);
        assert_eq!(c.write_db_interval, 15);
        assert!(c.daemon);
        assert!(c.gzip_scrape);
        assert!(!c.auto_register);
        assert_eq!(c.mysql_table_prefix, "xbt_");
        assert_eq!(c.mysql_database, "xbt");
        assert_eq!(c.column_torrents_tid, "tid");
    }

    #[test]
    fn test_load_bytes() {
        let mut c = Config::default();
        c.load_bytes(
            b"mysql_host = db.example.com\nmysql_port=3307\nmysql_user = bob\n\
              mysql_password = se cret\nlisten_port = 2710\nlisten_port = 80\n\
              anonymous_announce = 1\nauto_register = 1\ndaemon = 0\n\
              announce_interval = 900\n# comment without equals is ignored\nbogus_key = x\n",
        );
        assert_eq!(c.mysql_host, "db.example.com");
        assert_eq!(c.mysql_port, 3307);
        assert_eq!(c.mysql_user, "bob");
        assert_eq!(c.mysql_password, "se cret");
        assert_eq!(c.listen_ports.iter().copied().collect::<Vec<_>>(), vec![80, 2710]);
        assert!(c.anonymous_announce);
        assert!(c.auto_register);
        assert!(!c.daemon);
        assert_eq!(c.announce_interval, 900);
        assert!(c.set_str(b"bogus_key", b"x"));
        assert_eq!(effective_mysql_port(&c), 3307);
    }

    #[test]
    fn test_int_as_bool_cascade() {
        let mut c = Config::default();
        // unknown to string map -> int map -> bool map: "debug" is bool
        assert!(!c.set_str(b"debug", b"1"));
        assert!(c.debug);
        assert!(!c.set_str(b"debug", b"0"));
        assert!(!c.debug);
        // bool via int value != 0
        assert!(!c.set_str(b"full_scrape", b"2"));
        assert!(c.full_scrape);
        // sphinx default port
        let mut c2 = Config::default();
        c2.set_str(b"mysql_database", b"sphinx");
        assert_eq!(effective_mysql_port(&c2), 9306);
        assert_eq!(effective_mysql_port(&Config::default()), 3306);
    }
}
