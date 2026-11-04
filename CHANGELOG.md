# Changelog

All notable changes to the XBT Tracker Rust port are documented here.

## [1.0.0] — 2026-11-03

Initial release: a complete, behavior-faithful Rust port of the XBT
Tracker (`github.com/OlafvdSpek/xbt`, `Tracker/` component) with a
MySQL 5.5 backend.

### Added

- Full HTTP tracker surface: `/announce` (compact IPv4 + IPv6 peers,
  events, `numwant`, `ip=` override, `/{torrent_pass}/` URL prefixes),
  `/scrape` (single, multi-hash, full), `/statistics`, `/metrics`,
  `/debug`, `redirect_url` 302s, `offline_message`.
- Full UDP tracker (BEP-15): connect, announce, scrape, silent handling
  of bad connection ids / unknown actions, matching the original's
  2 KB buffer and 10000-datagram batch bound.
- MySQL 5.5 persistence via the pure-Rust `mysql` crate (no
  libmysqlclient dependency): batched `INSERT IGNORE … ON DUPLICATE KEY
  UPDATE` flushes for torrents/peers/user deltas, `insert delayed`
  announce/scrape logs, live config reload from the `xbt_config` table,
  schema probe (`test_sql`), 255 KB packet-size flush threshold,
  manual reconnect on lost connections.
- Config file support for every key of the original (`config.cpp`),
  including table/column name overrides and multi-valued `listen_port`;
  DB-row + file merge order identical to the original (file wins).
- gzip responses with the original's `compressed + 24 < raw` rule
  (manual gzip wrapper around raw deflate, matching the original's
  header/OS bytes and trailer layout).
- Daemon mode (double fork, `setsid`, `pid_file`), graceful
  SIGTERM/SIGINT shutdown with final DB flush, systemd socket
  activation (`LISTEN_PID`/`LISTEN_FDS`).
- New `mysql_port` configuration option (the original hardcodes
  3306/9306); host/user/password/database were already configurable and
  remain so.
- 38 unit tests (`cargo test`) covering codecs, config parsing, SQL
  building, tracker state transitions, the HTTP connection state
  machine and the UDP transaction layer.
- Differential test harness (`tests/run_difftest.sh`,
  `tests/difftest.py`): runs an identical 20-probe HTTP sequence plus
  UDP checks against the C++ reference binary and this port on two
  identically seeded databases, compares normalized responses and the
  full resulting database contents — 26/26 checks passing.
- Docker assets: `docker/docker-compose.yml` (mysql:5.5 + tracker),
  multi-stage `docker/Dockerfile`, `docker/init-logs.sql` (MyISAM log
  tables so `insert delayed` works as in the original), sample config.
- README with build/run/configuration/endpoint documentation and a
  full fidelity-notes list.

### Fixed (relative to the upstream C++ implementation)

- **UDP replies are delivered.** Upstream passes
  `sizeof(sockaddr_in)` (16) as `msg_namelen` to `sendto` on an
  `AF_INET6` socket; modern Linux kernels reject this with `EINVAL`, so
  the stock tracker silently never answers a single UDP packet. The port
  replies to the real source address.
- **Deterministic UDP connection id.** Upstream's IPv6 migration left
  `write_int(8, s, secret)` at offset 0 and then `memcpy(s, addr, 16)`
  over it, hashing 8 bytes of uninitialized stack (undefined behavior
  that only "works" within one process by luck of stack reuse). The port
  implements the clearly intended `sha1(secret_be(8) || addr16)[0..8]`.
- `insert delayed` log inserts no longer abort the whole batch on the
  first statement error (the original's `async_query` reports and
  continues; reconnect and error reporting behave the same, but a lost
  connection mid-cycle no longer wedges the DB thread).

### Changed (platform adaptations, behavior preserved)

- epoll/select event loop → tokio multi-thread runtime; all tracker
  state behind a single mutex; SQL execution serialized through one
  dedicated DB thread (FIFO), preserving the original's statement
  ordering. The pre-daemonize startup phase runs the same DB operations
  inline, exactly like the original's synchronous startup.
- `srand(time(NULL))`/`rand` → seeded xorshift64\* (same role: secrets
  and random strings, non-cryptographic).
- Binary SQL parameters emitted as `X'…'` hex literals instead of
  escaped raw bytes (identical stored bytes; `query_log` text differs).
- Empty `mysql_host` connects to `localhost` over TCP (libmysqlclient
  would use a Unix domain socket).
- Linux-only; the Windows service/`epoll` alternatives are out of scope.

### Preserved quirks (verified against the reference binary)

- Bare-LF request headers leave the connection state machine at state 3
  (do/while scan loop exits on zero net state change) — the response is
  still produced and sent.
- `/stats` (and any request whose 7th absolute byte is `t` under the
  `s` route) serves the statistics page.
- HTTP announces with `port=0` are accepted and inserted (only the UDP
  path rejects port 0); a missing `peer_id` inserts an all-zero id.
- `/debug` prints the dirty flag as `42`/`32` (the original streams a
  `char` ternary through `operator<<`, promoting it to `int`).
- Scrape responses end with four `e`s per the original's string
  building; empty full scrapes return `d5:filesdee`.
- Announce-log rows are written before validation, so rejected
  announces (invalid port, wait time, …) appear in the log too.
- `query_log` lines truncate at 999 characters; stderr SQL error format
  matches `Cdatabase::query_nothrow`.

[1.0.0]: https://github.com/OlafvdSpek/xbt
