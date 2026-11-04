//! Port of misc/database.cpp (`Cdatabase::replace_names`) and
//! misc/xbt/make_query.h (`make_query`, `query_append`).
//!
//! `@name` placeholders are replaced from the names map (tables/columns,
//! set up by read_config), exactly like the original. Terminator characters
//! for a name are ' ', ')' and ',' — the Cdatabase::replace_names variant,
//! which is what every tracker query goes through via make_query.
//!
//! Binary parameters (info_hash, peer_id, ipv6) are emitted as X'..' hex
//! literals instead of escaped raw bytes; the stored bytes are identical and
//! the SQL stays charset-independent. All other parameters use the same
//! escaping as mysql_real_escape_string.

use std::collections::HashMap;

/// Row as returned by the DB layer: raw column bytes, None for SQL NULL.
/// Mirrors Csql_row's str_ref access (numbers arrive in their text form,
/// exactly like libmysqlclient text protocol rows).
pub type DbRow = Vec<Option<Vec<u8>>>;

/// Convert a `mysql::Value` into its text-protocol byte representation
/// (what C++ code would see through MYSQL_ROW).
pub fn value_to_bytes(v: &mysql::Value) -> Option<Vec<u8>> {
    use mysql::Value;
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(b.clone()),
        Value::Int(i) => Some(i.to_string().into_bytes()),
        Value::UInt(u) => Some(u.to_string().into_bytes()),
        Value::Float(f) => Some(f.to_string().into_bytes()),
        Value::Double(d) => Some(d.to_string().into_bytes()),
        #[allow(deprecated)]
        Value::Date(y, mo, d, h, mi, s, us) => Some(
            format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", y, mo, d, h, mi, s, us)
                .into_bytes(),
        ),
        Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
            let mut total_hours = *days as i64 * 24 + *hours as i64;
            let sign = if *is_neg { total_hours = -total_hours; "-" } else { "" };
            Some(
                format!(
                    "{}{:02}:{:02}:{:02}.{:06}",
                    sign,
                    total_hours.abs(),
                    minutes,
                    seconds,
                    micros
                )
                .into_bytes(),
            )
        }
    }
}

/// Convenience helpers mirroring `Csql_row::operator[]` + `.i()` / `.size()`.
pub fn row_bytes(row: &DbRow, i: usize) -> &[u8] {
    row.get(i).and_then(|v| v.as_deref()).unwrap_or(b"")
}

pub fn row_int(row: &DbRow, i: usize) -> i64 {
    crate::bt_misc::to_int(row_bytes(row, i))
}

/// mysql_real_escape_string equivalent (default sql_mode, i.e. with
/// backslash escapes). Output is pure ASCII.
pub fn sql_escape(v: &[u8]) -> String {
    let mut r = String::with_capacity(v.len() + 2);
    r.push('\'');
    for &c in v {
        match c {
            0 => r.push_str("\\0"),
            b'\n' => r.push_str("\\n"),
            b'\r' => r.push_str("\\r"),
            0x1a => r.push_str("\\Z"),
            b'\\' => r.push_str("\\\\"),
            b'\'' => r.push_str("\\'"),
            b'"' => r.push_str("\\\""),
            _ => r.push(c as char), // callers only pass ASCII here; see hex_arg for binary
        }
    }
    r.push('\'');
    r
}

pub fn hex_literal(v: &[u8]) -> String {
    let mut s = String::with_capacity(v.len() * 2 + 3);
    s.push_str("X'");
    s.push_str(&crate::bt_misc::hex_encode(v).to_uppercase());
    s.push('\'');
    s
}

/// A make_query argument.
pub enum Arg<'a> {
    /// integer literal (query_append integral overload)
    Int(i64),
    /// escaped + quoted ASCII string (query_append str_ref overload)
    Str(&'a [u8]),
    /// X'..' hex literal for binary data (port-specific, byte-identical storage)
    Hex(&'a [u8]),
    /// raw() — pre-formatted SQL fragment
    Raw(&'a str),
}

pub fn fmt_arg(a: &Arg) -> String {
    match a {
        Arg::Int(i) => i.to_string(),
        Arg::Str(s) => sql_escape(s),
        Arg::Hex(b) => hex_literal(b),
        Arg::Raw(s) => s.to_string(),
    }
}

/// Port of `Cdatabase::replace_names` + `make_query`: substitutes @names
/// (terminated by ' ', ')' or ',' — the terminator is kept) and '?' with the
/// formatted arguments, left to right.
pub fn make_query(names: &HashMap<String, String>, q: &str, args: &[Arg]) -> String {
    let mut s = String::with_capacity(q.len() + 32);
    let bytes = q.as_bytes();
    let mut i = 0usize;
    let mut arg = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'?' => {
                if arg < args.len() {
                    s.push_str(&fmt_arg(&args[arg]));
                    arg += 1;
                }
                i += 1;
            }
            b'@' => {
                // Cdatabase::replace_names: read_until(v, '@') consumed the '@'
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b' ' && bytes[i] != b')' && bytes[i] != b',' {
                    i += 1;
                }
                let name = &q[start..i];
                match names.get(name) {
                    Some(repl) => s.push_str(repl),
                    None => s.push_str(name),
                }
                // terminator char itself stays and is copied by the default branch
            }
            c => {
                s.push(c as char);
                i += 1;
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names() -> HashMap<String, String> {
        let mut n = HashMap::new();
        n.insert("torrents".into(), "xbt_torrents".into());
        n.insert("tid".into(), "tid".into());
        n.insert("leechers".into(), "leechers2".into());
        n.insert("seeders".into(), "seeders2".into());
        n.insert("completed".into(), "completed2".into());
        n.insert("users".into(), "xbt_users".into());
        n.insert("uid".into(), "uid".into());
        n.insert("announce_log".into(), "xbt_announce_log".into());
        n.insert("config".into(), "xbt_config".into());
        n
    }

    #[test]
    fn test_replace_names() {
        let n = names();
        assert_eq!(
            make_query(&n, "select info_hash, @tid from @torrents where @tid >= ?", &[Arg::Int(5)]),
            "select info_hash, tid from xbt_torrents where tid >= 5"
        );
        assert_eq!(
            make_query(&n, "delete from @torrents where @tid = ?", &[Arg::Int(9)]),
            "delete from xbt_torrents where tid = 9"
        );
        assert_eq!(
            make_query(
                &n,
                "insert ignore into @torrents (@leechers, @seeders, @completed, @tid) values ?\
                 on duplicate key update\
                 @leechers = values(@leechers), @seeders = values(@seeders),\
                 @completed = values(@completed), mtime = unix_timestamp()",
                &[Arg::Raw("(1,2,3,4)")]
            ),
            "insert ignore into xbt_torrents (leechers2, seeders2, completed2, tid) values (1,2,3,4)\
             on duplicate key update\
             leechers2 = values(leechers2), seeders2 = values(seeders2),\
             completed2 = values(completed2), mtime = unix_timestamp()"
        );
        assert_eq!(
            make_query(&n, "select name, value from @config where value is not null", &[]),
            "select name, value from xbt_config where value is not null"
        );
        // unknown names pass through
        assert_eq!(make_query(&n, "select @bogus from t", &[]), "select bogus from t");
    }

    #[test]
    fn test_escape_and_hex() {
        assert_eq!(
            sql_escape(b"pa'ss\\wo\"r\nd\r\0\x1ab"),
            r#"'pa\'ss\\wo\"r\nd\r\0\Zb'"#
        );
        assert_eq!(hex_literal(&[0x00, 0xff, 0x10]), "X'00FF10'");
    }

    #[test]
    fn test_query_with_binary() {
        let n = names();
        let ih = [7u8; 20];
        assert_eq!(
            make_query(
                &n,
                "insert into @torrents (info_hash, mtime, ctime) values (?, unix_timestamp(), unix_timestamp())",
                &[Arg::Hex(&ih)]
            ),
            format!(
                "insert into xbt_torrents (info_hash, mtime, ctime) values (X'{}', unix_timestamp(), unix_timestamp())",
                "07".repeat(20).to_uppercase()
            )
        );
        assert_eq!(
            make_query(
                &n,
                "insert delayed into @announce_log (ipv6, port, event, info_hash, peer_id, downloaded, left0, uploaded, uid, mtime) values ?",
                &[Arg::Raw("(X'7F000001',6881,2,X'00',X'01',0,0,0,0,1700000000)")]
            ),
            "insert delayed into xbt_announce_log (ipv6, port, event, info_hash, peer_id, downloaded, left0, uploaded, uid, mtime) values (X'7F000001',6881,2,X'00',X'01',0,0,0,0,1700000000)"
        );
    }
}
