//! XBT Tracker — faithful Rust port.
//!
//! Port of Tracker/tracker.cpp's main/main1/srv_run start-up sequence:
//!   argv (--conf-file) -> load conf file -> open MySQL -> read_config
//!   (DB rows + file merge) -> test_sql -> bind listeners -> initial DB
//!   sync -> optional daemonize -> event loop -> final DB sync -> unlink
//!   pid file.

mod bt_misc;
mod config;
mod connection;
mod database;
mod gzip;
mod rng;
mod server;
mod sql;
mod stream_int;
mod tracker;
mod tracker_input;
mod transaction;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use config::{effective_mysql_port, Config};
use database::{ConnParams, Db};
use tracker::{clean_up, read_config, read_db_torrents, read_db_users, sql_update_zero_counts, test_sql, write_db_torrents, write_db_users, Shared, State};

/// Minimal executor for the start-up phase (before daemonizing, when the DB
/// handle is still in Inline mode and no future ever actually suspends).
fn block_on_lite<F: Future>(fut: F) -> F::Output {
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::thread::yield_now();
    }
}

fn main() {
    // ---- port of main(argc, argv) (POSIX branch) ----
    let args: Vec<String> = std::env::args().collect();
    let mut conf_file = String::from("xbt_tracker.conf");
    if args.len() >= 2 {
        if (args[1] == "--conf_file" || args[1] == "--conf-file") && args.len() >= 3 {
            conf_file = args[2].clone();
        } else {
            eprintln!("  --conf-file arg (=xbt_tracker.conf)");
            std::process::exit(1);
        }
    }

    // ---- port of main1() ----
    rng::seed_from_time(); // srand(time(NULL))
    let mut config = Config::default();
    if config.load_file(&conf_file) {
        eprintln!("Unable to read {}", conf_file);
    }
    let params = ConnParams {
        host: config.mysql_host.clone(),
        port: effective_mysql_port(&config),
        user: config.mysql_user.clone(),
        password: config.mysql_password.clone(),
        database: config.mysql_database.clone(),
    };
    let db = match Db::open(params, &config.query_log) {
        Ok(db) => db,
        Err(e) => {
            // catch (bad_query& e) { cerr << e.what(); return 1; }
            eprintln!("{}", e);
            std::process::exit(1);
        }
    };
    let table_prefix = config.mysql_table_prefix.clone();

    // ---- port of srv_run() ----
    let secret = tracker::generate_secret();
    let sh = Arc::new(Shared {
        state: std::sync::Mutex::new(State::new(conf_file.clone(), table_prefix, secret)),
        db,
        access_log: std::sync::Mutex::new(None),
    });

    // read_config(); if (test_sql()) return 1;
    block_on_lite(read_config(&sh));
    if !block_on_lite(test_sql(&sh)) {
        std::process::exit(1);
    }

    // listeners: systemd socket activation, else bind all listen ports
    let (std_tcp, std_udp) = {
        let sysd = server::systemd_listen_fds();
        if !sysd.is_empty() {
            (sysd, Vec::new())
        } else {
            let ports: Vec<i32> = sh.lock().config.listen_ports.iter().copied().collect();
            let mut tcp = Vec::new();
            let mut udp = Vec::new();
            for p in &ports {
                match server::bind_tcp(*p as u16) {
                    Ok(l) => tcp.push(l),
                    Err(e) => {
                        eprintln!("bind failed: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            for p in &ports {
                match server::bind_udp(*p as u16) {
                    Ok(l) => udp.push(l),
                    Err(e) => {
                        eprintln!("bind failed: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            (tcp, udp)
        }
    };
    for l in &std_tcp {
        let _ = l.set_nonblocking(true);
    }
    for l in &std_udp {
        let _ = l.set_nonblocking(true);
    }

    // query("update @torrents set @leechers = 0, @seeders = 0");
    // (not wrapped in try/catch in the original: failure aborts start-up)
    let sql = {
        let st = sh.lock();
        sql_update_zero_counts(&st.db_names)
    };
    if let Err(e) = block_on_lite(sh.db.exec(sql)) {
        eprintln!("{}", e);
        std::process::exit(1);
    }
    // clean_up(); read_db_torrents(); read_db_users(); write_db_torrents(); write_db_users();
    clean_up(&sh);
    block_on_lite(read_db_torrents(&sh));
    block_on_lite(read_db_users(&sh));
    block_on_lite(write_db_torrents(&sh));
    block_on_lite(write_db_users(&sh));

    // daemon branch: daemon(true, false) + pid file; SIGPIPE is already
    // ignored process-wide by the Rust runtime (MSG_NOSIGNAL equivalent).
    let (daemon, pid_file) = {
        let st = sh.lock();
        (st.config.daemon, st.config.pid_file.clone())
    };
    if daemon && !server::daemonize(&pid_file) {
        std::process::exit(1);
    }

    // From here on: dedicated DB thread + multi-threaded tokio runtime.
    sh.db.switch_to_thread();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    let code = runtime.block_on(serve(sh, std_tcp, std_udp, pid_file));
    std::process::exit(code);
}

async fn serve(
    sh: Arc<Shared>,
    std_tcp: Vec<std::net::TcpListener>,
    std_udp: Vec<std::net::UdpSocket>,
    pid_file: String,
) -> i32 {
    use tokio::signal::unix::{signal, SignalKind};

    let shutdown = Arc::new(AtomicBool::new(false));

    for l in std_tcp {
        let l = tokio::net::TcpListener::from_std(l).expect("tcp listener");
        let sh2 = sh.clone();
        let sd = shutdown.clone();
        tokio::spawn(server::tcp_accept_loop(l, sh2, sd));
    }
    for u in std_udp {
        let u = tokio::net::UdpSocket::from_std(u).expect("udp socket");
        let sh2 = sh.clone();
        let sd = shutdown.clone();
        tokio::spawn(server::udp_loop(u, sh2, sd));
    }
    tokio::spawn(server::timer_loop(sh.clone(), shutdown.clone()));

    // SIGTERM (the original's sig_handler) + SIGINT for convenience
    let mut term = signal(SignalKind::terminate()).expect("sigterm handler");
    let mut int = signal(SignalKind::interrupt()).expect("sigint handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    shutdown.store(true, Ordering::Relaxed);

    // while (!g_sig_term) { ... } write_db_torrents(); write_db_users();
    write_db_torrents(&sh).await;
    write_db_users(&sh).await;
    sh.db.flush().await;

    // unlink(g_config.pid_file_.c_str());
    if !pid_file.is_empty() {
        let _ = std::fs::remove_file(&pid_file);
    }
    0
}
