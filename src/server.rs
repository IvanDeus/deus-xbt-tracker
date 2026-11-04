//! Port of the srv_run() event-loop plumbing: dual-stack listen sockets,
//! systemd socket activation, accept loop, UDP loop and the one-action-per-
//! second timer chain of the original main loop.

use std::net::{Ipv6Addr, SocketAddr, TcpListener as StdTcpListener, UdpSocket as StdUdpSocket};
use std::os::unix::io::FromRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::bt_misc::xbt_syslog;
use crate::connection::Connection;
use crate::tracker::{clean_up, read_config, read_db_torrents, read_db_users, srv_time, write_db_torrents, write_db_users, Shared};
use crate::transaction::{Ctransaction, udp_addr16};

/// Port of the TCP bind sequence:
/// open6(SOCK_STREAM) -> IPV6_V6ONLY=false -> SO_REUSEADDR -> bind6(port) -> listen
pub fn bind_tcp(port: u16) -> std::io::Result<StdTcpListener> {
    let s = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    s.set_only_v6(false)?;
    s.set_reuse_address(true)?;
    s.bind(&socket2::SockAddr::from(SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port)))?;
    s.listen(libc::SOMAXCONN)?;
    Ok(StdTcpListener::from(s))
}

/// Port of the UDP bind sequence: open6(SOCK_DGRAM) -> SO_REUSEADDR -> bind6(port)
pub fn bind_udp(port: u16) -> std::io::Result<StdUdpSocket> {
    let s = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    s.set_reuse_address(true)?;
    s.bind(&socket2::SockAddr::from(SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port)))?;
    Ok(StdUdpSocket::from(s))
}

/// Port of the sd_listen_fds(true) block (without libsystemd): when
/// LISTEN_PID matches and LISTEN_FDS > 0, fds 3.. are taken over as TCP
/// listen sockets and no sockets are bound (exactly like the original, which
/// also skips the UDP binds in that case).
pub fn systemd_listen_fds() -> Vec<StdTcpListener> {
    let pid_ok = std::env::var("LISTEN_PID")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .map(|p| p == std::process::id())
        .unwrap_or(false);
    let n: usize = std::env::var("LISTEN_FDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if !pid_ok || n == 0 {
        return Vec::new();
    }
    // sd_listen_fds(true) unsets the environment variables
    std::env::remove_var("LISTEN_PID");
    std::env::remove_var("LISTEN_FDS");
    std::env::remove_var("LISTEN_FDNAMES");
    let mut v = Vec::new();
    for i in 0..n {
        let fd = 3 + i as std::os::unix::io::RawFd;
        unsafe {
            libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
            v.push(StdTcpListener::from_raw_fd(fd));
        }
    }
    v
}

/// The accept loop (port of `void accept(const Csocket&)` + connection_t).
pub async fn tcp_accept_loop(listener: TcpListener, sh: Arc<Shared>, shutdown: Arc<AtomicBool>) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        let (stream, addr) = tokio::select! {
            r = listener.accept() => match r {
                Ok(v) => v,
                Err(e) => {
                    // accept failed: ... (WSAECONNABORTED -> continue; other -> count+log)
                    let code = e.raw_os_error().unwrap_or(0);
                    if code == libc::ECONNABORTED || code == libc::EINTR {
                        continue;
                    }
                    sh.lock().stats.accept_errors += 1;
                    eprintln!("accept failed: {}", e);
                    xbt_syslog(&format!("accept failed: {}", e));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
        };
        sh.lock().stats.accepted_tcp += 1;
        let sh2 = sh.clone();
        let shutdown2 = shutdown.clone();
        tokio::spawn(async move {
            handle_connection(stream, addr, sh2, shutdown2).await;
        });
    }
}

/// Port of connection_t's lifecycle: serve the connection until one of the
/// original's close conditions hits (10 s age, EOF + flushed, headers
/// complete + flushed, socket errors).
async fn handle_connection(stream: TcpStream, addr: SocketAddr, sh: Arc<Shared>, shutdown: Arc<AtomicBool>) {
    use tokio::io::AsyncReadExt;
    let _ = stream.set_nodelay(true);
    let mut conn = Connection::new(&addr, srv_time());
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut readbuf = [0u8; 4096];
    let mut first = true;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        if srv_time() - conn.ctime > 10 {
            return; // srv_time() - ctime_ > 10
        }
        if conn.flushed_and_finished() {
            return;
        }
        let want_write = conn.response.is_some()
            && conn.response_sent < conn.response.as_ref().unwrap().len();
        tokio::select! {
            r = rd.read(&mut readbuf), if !conn.eof_seen => {
                match r {
                    Ok(0) => conn.mark_eof(),
                    Ok(n) => {
                        let mut execs: Vec<String> = Vec::new();
                        let produced = conn.feed(&readbuf[..n], &sh, &mut execs).is_some();
                        for sql in execs {
                            sh.db.exec_fire(sql);
                        }
                        if produced {
                            // the original sends the response immediately
                            // (sendmsg inside read())
                            if flush_response(&mut wr, &mut conn).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(_) => return, // recv failed (ECONNRESET etc.)
                }
            }
            w = flush_response(&mut wr, &mut conn), if want_write => {
                if w.is_err() {
                    return; // send failed
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        if first {
            first = false;
            // the original counts connections that survive the immediate
            // first process_events(EPOLLIN) as "slow"
            let closed_now = conn.flushed_and_finished()
                || (conn.eof_seen && conn.response.is_none());
            if !closed_now {
                sh.lock().stats.slow_tcp += 1;
            }
        }
    }
}

async fn flush_response<W>(wr: &mut W, conn: &mut Connection) -> std::io::Result<()>
where
    W: tokio::io::AsyncWriteExt + Unpin,
{
    if let Some(resp) = conn.response.clone() {
        let rest = &resp[conn.response_sent..];
        match wr.write_all(rest).await {
            Ok(()) => {
                conn.response_sent = resp.len();
                Ok(())
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::ConnectionReset
                    && e.kind() != std::io::ErrorKind::BrokenPipe
                {
                    eprintln!("send failed: {}", e);
                }
                Err(e)
            }
        }
    } else {
        Ok(())
    }
}

/// The UDP loop (port of udp_listen_socket_t::process_events +
/// Ctransaction::recv's drain loop).
pub async fn udp_loop(sock: UdpSocket, sh: Arc<Shared>, shutdown: Arc<AtomicBool>) {
    let mut b = vec![0u8; 2 << 10];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        let (len, src) = tokio::select! {
            r = sock.recv_from(&mut b) => match r {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("recv failed: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
        };
        let t = Ctransaction {
            sh: &sh,
            addr16: udp_addr16(&src),
        };
        let mut execs: Vec<String> = Vec::new();
        let resp = {
            // borrow guard: recv() locks the state internally
            let _ = &mut execs;
            t.recv(&b[..len])
        };
        if let Some(resp) = resp {
            // sendto; the original logs "send failed" and counts sent_udp
            match sock.send_to(&resp, src).await {
                Ok(n) if n == resp.len() => {}
                _ => eprintln!("send failed"),
            }
            sh.lock().stats.sent_udp += 1;
        }
    }
}

/// The one-action-per-second timer chain of the original main loop:
///
/// ```c++
/// if (srv_time() - g_read_config_time > g_config.read_config_interval_) read_config();
/// else if (... clean_up_interval_) clean_up();
/// else if (... read_db_interval_) read_db_torrents();
/// else if (... read_db_interval_) read_db_users();
/// else if (write_db_interval_ && ... ) write_db_torrents();
/// else if (write_db_interval_ && ... ) write_db_users();
/// ```
pub async fn timer_loop(sh: Arc<Shared>, shutdown: Arc<AtomicBool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        let now = srv_time();
        enum Op {
            ReadConfig,
            CleanUp,
            ReadDbTorrents,
            ReadDbUsers,
            WriteDbTorrents,
            WriteDbUsers,
            None,
        }
        let op = {
            let st = sh.lock();
            let c = &st.config;
            if now - st.read_config_time > c.read_config_interval as i64 {
                Op::ReadConfig
            } else if now - st.clean_up_time > c.clean_up_interval as i64 {
                Op::CleanUp
            } else if now - st.read_db_torrents_time > c.read_db_interval as i64 {
                Op::ReadDbTorrents
            } else if now - st.read_db_users_time > c.read_db_interval as i64 {
                Op::ReadDbUsers
            } else if c.write_db_interval != 0 && now - st.write_db_torrents_time > c.write_db_interval as i64 {
                Op::WriteDbTorrents
            } else if c.write_db_interval != 0 && now - st.write_db_users_time > c.write_db_interval as i64 {
                Op::WriteDbUsers
            } else {
                Op::None
            }
        };
        match op {
            Op::ReadConfig => read_config(&sh).await,
            Op::CleanUp => clean_up(&sh),
            Op::ReadDbTorrents => read_db_torrents(&sh).await,
            Op::ReadDbUsers => read_db_users(&sh).await,
            Op::WriteDbTorrents => write_db_torrents(&sh).await,
            Op::WriteDbUsers => write_db_users(&sh).await,
            Op::None => {}
        }
    }
}

/// Port of `daemon(true, false)` + pid file + signal setup (the daemon branch
/// of srv_run). Double fork + setsid + std fds to /dev/null, no chdir.
pub fn daemonize(pid_file: &str) -> bool {
    unsafe {
        let pid = libc::fork();
        if pid == -1 {
            eprintln!("daemon failed");
            return false;
        }
        if pid != 0 {
            libc::_exit(0);
        }
        if libc::setsid() == -1 {
            eprintln!("daemon failed");
            return false;
        }
        let pid = libc::fork();
        if pid == -1 {
            eprintln!("daemon failed");
            return false;
        }
        if pid != 0 {
            libc::_exit(0);
        }
        // noclose == false: redirect std fds to /dev/null
        if let Ok(fd) = std::fs::File::open("/dev/null") {
            use std::os::unix::io::AsRawFd;
            libc::dup2(fd.as_raw_fd(), 0);
            libc::dup2(fd.as_raw_fd(), 1);
            libc::dup2(fd.as_raw_fd(), 2);
        }
    }
    if !pid_file.is_empty() {
        // ofstream(g_config.pid_file_) << getpid() << endl
        if let Ok(mut f) = std::fs::File::create(pid_file) {
            use std::io::Write;
            let _ = writeln!(f, "{}", std::process::id());
        }
    }
    true
}
