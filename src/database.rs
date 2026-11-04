//! Port of misc/database.cpp (Cdatabase) on top of the `mysql` crate.
//!
//! Two execution modes share the same async interface:
//!   * `Inline` — the connection is used directly by the calling thread.
//!     Used during the synchronous start-up phase (before daemonizing),
//!     mirroring how the original runs everything on its single thread.
//!   * `Thread` — the connection lives on a dedicated DB thread reached over
//!     an unbounded channel; this keeps a *single* MySQL connection with
//!     FIFO statement ordering, exactly like the original.
//!
//! Like the original (MYSQL_OPT_RECONNECT), a lost connection is reopened
//! before the next statement; the failed statement itself is not retried.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mysql::prelude::*;
use mysql::{Conn, Opts, OptsBuilder, Row};

use crate::sql::{value_to_bytes, DbRow};

pub type DbResult<T> = Result<T, String>;

pub enum DbCmd {
    /// fire-and-forget statement (query_nothrow equivalent)
    Exec(String),
    /// statement producing a result set
    Query(String, tokio::sync::oneshot::Sender<DbResult<Vec<DbRow>>>),
    /// statement producing a last_insert_id
    Insert(String, tokio::sync::oneshot::Sender<DbResult<u64>>),
    /// barrier: responds once all previously queued statements ran
    Flush(tokio::sync::oneshot::Sender<()>),
}

pub struct ConnParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl ConnParams {
    fn opts(&self) -> Opts {
        let mut b = OptsBuilder::new()
            .tcp_port(self.port)
            .read_timeout(Some(Duration::from_secs(10)))
            .write_timeout(Some(Duration::from_secs(10)));
        if !self.host.is_empty() {
            b = b.ip_or_hostname(Some(self.host.clone()));
        }
        if !self.user.is_empty() {
            b = b.user(Some(self.user.clone()));
        }
        if !self.password.is_empty() {
            b = b.pass(Some(self.password.clone()));
        }
        if !self.database.is_empty() {
            b = b.db_name(Some(self.database.clone()));
        }
        b.into()
    }
}

struct Inner {
    conn: Option<Conn>,
    params: ConnParams,
    /// true after a statement failed with what looks like a lost connection
    need_reopen: bool,
    log: Option<File>,
}

fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

impl Inner {
    fn log_query(&mut self, q: &str) {
        if let Some(f) = &mut self.log {
            // Cdatabase::query_nothrow logs q.substr(0, 999) + "\n" (flushed)
            let _ = writeln!(f, "{}", truncate(q, 999));
            let _ = f.flush();
        }
    }

    fn ensure_conn(&mut self) -> DbResult<()> {
        if self.need_reopen || self.conn.is_none() {
            match Conn::new(self.params.opts()) {
                Ok(c) => {
                    self.conn = Some(c);
                    self.need_reopen = false;
                    Ok(())
                }
                Err(e) => {
                    self.conn = None;
                    Err(format!("{}", e))
                }
            }
        } else {
            Ok(())
        }
    }

    fn is_gone(e: &mysql::Error) -> bool {
        use mysql::Error;
        match e {
            Error::IoError(_) | Error::DriverError(_) | Error::CodecError(_) => true,
            Error::MySqlError(m) => matches!(
                m.code,
                // server gone / lost / shutdown family
                1028 | 1040 | 1042 | 1043 | 1053 | 1152 | 1153 | 1154 | 1155 | 1158 | 1159
                    | 1160 | 1161 | 2006 | 2013
            ),
            _ => false,
        }
    }

    fn report(&mut self, e: &mysql::Error, sql: &str) -> String {
        if Self::is_gone(e) {
            self.need_reopen = true;
        }
        // Cdatabase::query_nothrow: cerr << mysql_error << "\n" << q.substr(0, 239)
        let msg = format!("{}\n{}", e, truncate(sql, 239));
        eprintln!("{}", msg);
        msg
    }

    // The connection is taken out of `self` for the duration of each
    // statement (single-threaded use, mirroring Cdatabase), which also
    // keeps the borrow checker happy around error reporting.

    fn exec(&mut self, sql: &str) -> DbResult<()> {
        self.log_query(sql);
        self.ensure_conn()?;
        let mut conn = self.conn.take().unwrap();
        let res = conn.query_drop(sql);
        self.conn = Some(conn);
        res.map_err(|e| self.report(&e, sql))
    }

    fn query(&mut self, sql: &str) -> DbResult<Vec<DbRow>> {
        self.log_query(sql);
        self.ensure_conn()?;
        let mut conn = self.conn.take().unwrap();
        let mut out = Vec::new();
        let mut fail: Option<mysql::Error> = None;
        match conn.query_iter(sql) {
            Ok(mut qr) => {
                while let Some(item) = qr.next() {
                    match item {
                        Ok(row) => out.push(row_to_dbrow(row)),
                        Err(e) => {
                            fail = Some(e);
                            break;
                        }
                    }
                }
            }
            Err(e) => fail = Some(e),
        }
        self.conn = Some(conn);
        match fail {
            Some(e) => Err(self.report(&e, sql)),
            None => Ok(out),
        }
    }

    fn insert(&mut self, sql: &str) -> DbResult<u64> {
        self.log_query(sql);
        self.ensure_conn()?;
        let mut conn = self.conn.take().unwrap();
        let mut fail: Option<mysql::Error> = None;
        match conn.query_iter(sql) {
            Ok(mut qr) => {
                while let Some(item) = qr.next() {
                    if let Err(e) = item {
                        fail = Some(e);
                        break;
                    }
                }
            }
            Err(e) => fail = Some(e),
        }
        let id = if fail.is_none() { conn.last_insert_id() } else { 0 };
        self.conn = Some(conn);
        match fail {
            Some(e) => Err(self.report(&e, sql)),
            None => Ok(id),
        }
    }
}

fn row_to_dbrow(row: Row) -> DbRow {
    row.unwrap().iter().map(value_to_bytes).collect()
}

enum DbState {
    Inline(Inner),
    Thread(tokio::sync::mpsc::UnboundedSender<DbCmd>),
    Closed,
}

/// Cloneable handle to the database.
#[derive(Clone)]
pub struct Db(Arc<Mutex<DbState>>);

impl Db {
    /// Open the (single) MySQL connection; like `Cdatabase::open` this fails
    /// (`Err`) when the server cannot be reached. The query log file, when
    /// configured, is opened once with truncation (the original's
    /// `static ofstream`).
    pub fn open(params: ConnParams, query_log: &str) -> DbResult<Db> {
        let conn = Conn::new(params.opts()).map_err(|e| format!("{}", e))?;
        let log = if !query_log.is_empty() {
            Some(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(query_log)
                    .map_err(|e| format!("query_log: {}", e))?,
            )
        } else {
            None
        };
        Ok(Db(Arc::new(Mutex::new(DbState::Inline(Inner {
            conn: Some(conn),
            params,
            need_reopen: false,
            log,
        })))))
    }

    /// Fire-and-forget handle used by unit tests (never executes anything).
    #[cfg(test)]
    pub fn dummy() -> Db {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DbCmd>();
        Db(Arc::new(Mutex::new(DbState::Thread(tx))))
    }

    /// Move the connection onto its dedicated thread (called after
    /// daemonizing, before the event loop starts).
    pub fn switch_to_thread(&self) {
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let inner = match std::mem::replace(&mut *g, DbState::Closed) {
            DbState::Inline(inner) => inner,
            other => {
                *g = other;
                return;
            }
        };
        drop(g);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DbCmd>();
        std::thread::Builder::new()
            .name("xbt-db".into())
            .spawn(move || {
                let mut inner = inner;
                while let Some(cmd) = rx.blocking_recv() {
                    match cmd {
                        DbCmd::Exec(sql) => {
                            let _ = inner.exec(&sql);
                        }
                        DbCmd::Query(sql, resp) => {
                            let r = inner.query(&sql);
                            let _ = resp.send(r);
                        }
                        DbCmd::Insert(sql, resp) => {
                            let r = inner.insert(&sql);
                            let _ = resp.send(r);
                        }
                        DbCmd::Flush(resp) => {
                            let _ = resp.send(());
                        }
                    }
                }
            })
            .expect("spawn db thread");
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = DbState::Thread(tx);
    }

    fn thread_sender(&self) -> Option<tokio::sync::mpsc::UnboundedSender<DbCmd>> {
        let g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match &*g {
            DbState::Thread(tx) => Some(tx.clone()),
            _ => None,
        }
    }

    /// query_nothrow equivalent: fire and forget, errors logged by the layer.
    pub fn exec_fire(&self, sql: String) {
        if let Some(tx) = self.thread_sender() {
            let _ = tx.send(DbCmd::Exec(sql));
            return;
        }
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *g {
            DbState::Inline(inner) => {
                let _ = inner.exec(&sql);
            }
            _ => {}
        }
    }

    pub async fn exec(&self, sql: String) -> DbResult<()> {
        if let Some(tx) = self.thread_sender() {
            // FIFO channel preserves statement order; errors are logged by
            // the DB thread, like query_nothrow's cerr output.
            return tx.send(DbCmd::Exec(sql)).map_err(|_| "db thread gone".to_string());
        }
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *g {
            DbState::Inline(inner) => inner.exec(&sql),
            _ => Err("db closed".into()),
        }
    }

    pub async fn query(&self, sql: String) -> DbResult<Vec<DbRow>> {
        if let Some(tx) = self.thread_sender() {
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
            tx.send(DbCmd::Query(sql, resp_tx))
                .map_err(|_| "db thread gone".to_string())?;
            return resp_rx.await.map_err(|_| "db thread gone".to_string())?;
        }
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *g {
            DbState::Inline(inner) => inner.query(&sql),
            _ => Err("db closed".into()),
        }
    }

    pub async fn insert(&self, sql: String) -> DbResult<u64> {
        if let Some(tx) = self.thread_sender() {
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
            tx.send(DbCmd::Insert(sql, resp_tx))
                .map_err(|_| "db thread gone".to_string())?;
            return resp_rx.await.map_err(|_| "db thread gone".to_string())?;
        }
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *g {
            DbState::Inline(inner) => inner.insert(&sql),
            _ => Err("db closed".into()),
        }
    }

    /// Wait until all queued statements have been executed (shutdown).
    pub async fn flush(&self) {
        let tx = {
            let g = self.0.lock().unwrap_or_else(|e| e.into_inner());
            match &*g {
                DbState::Thread(tx) => tx.clone(),
                _ => return,
            }
        };
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        if tx.send(DbCmd::Flush(resp_tx)).is_ok() {
            let _ = resp_rx.await;
        }
    }
}
