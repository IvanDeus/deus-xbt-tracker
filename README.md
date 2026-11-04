# deus-xbt-tracker

OlafvdSpek's XBT torrent tracker recreated in Rust

# XBT Tracker — Rust port

A faithful Rust port of the [XBT Tracker](https://github.com/OlafvdSpek/xbt)
(the `Tracker` component of Olaf van der Spek's XBT tools), targeting
**MySQL 5.5** as the database backend (could work with more recent versions). The MySQL connection — host, port,
user, password and database — is fully configurable through the same
`xbt_tracker.conf` file format as the original.

The port follows the C++ implementation (`Tracker/` + the parts of `misc/`
it uses) function by function, including its quirks, its SQL statements and
its on-the-wire response formats. A differential test harness
(`tests/run_difftest.sh`) runs an identical probe sequence against the
original C++ binary and this port and compares HTTP responses byte-for-byte
(as well as the resulting database contents) — see [Testing](#testing).

## Features

- **HTTP tracker**: `/announce` (compact IPv4 + IPv6 peer lists, `numwant`,
  events, `&event=stopped/completed/started`), `/scrape` (single,
  multi-hash and full scrape), per-torrent-pass URL prefixes
  (`/{32-hex-pass}/announce…`)
- **UDP tracker (BEP-15)**: connect / announce / scrape / error, with the
  original's `sha1(secret || address)` connection-id scheme
- **Admin endpoints**: `/statistics` (HTML), `/metrics` (Prometheus-style
  text), `/debug` (when `debug = 1`), `redirect_url` / `offline_message`
  support
- **MySQL 5.5 storage**: torrents, peers and user transfer deltas are
  buffered and flushed in batch upserts exactly like the original
  (`INSERT IGNORE … ON DUPLICATE KEY UPDATE`), optional `announce_log` /
  `scrape_log` tables, live-reloadable configuration from a `config` table
- **gzip** responses when compression pays off (`compressed + 24 < raw`,
  the original's rule)
- **torrent_pass** authentication (SHA-1 based, compatible with the XBT
  web UI scheme) and anonymous/auto-register modes
- Daemon mode (double fork, `pid_file`), graceful `SIGTERM`/`SIGINT`
  shutdown with final DB flush, systemd socket activation
 (`sd_listen_fds`-compatible `LISTEN_PID`/`LISTEN_FDS` parsing)
- Pure-Rust MySQL client (`mysql` crate) — **no libmysqlclient needed**;
  single self-contained static-ish binary (~2.8 MB stripped)

### Manual database setup

```sql
CREATE DATABASE xbt;
USE xbt;
SOURCE xbt_tracker.sql;          -- shipped in this repo (upstream schema)
CREATE USER 'xbt'@'%' IDENTIFIED BY 'xbtpass';
GRANT ALL PRIVILEGES ON xbt.* TO 'xbt'@'%';
```

## Building

Requires Rust ≥ 1.75 (edition 2021) — [rustup](https://rustup.rs):

```sh
cargo build --release        # binary: target/release/xbt_tracker
cargo test                   # 38 unit tests
```

## Running

```sh
./xbt_tracker --conf_file /path/to/xbt_tracker.conf
```

`--conf_file` (also accepted as `--conf-file`) is mandatory, exactly like
the original; without it the tracker prints a usage line and exits 1.
Startup order mirrors the C++ `main()`/`srv_run()`: load config file →
connect MySQL → `read_config` (DB rows, then file overrides) → `test_sql`
(schema probe) → bind listen ports → zero torrent counters → initial
DB read/write cycles → daemonize (if `daemon = 1`) → serve.

`SIGTERM`/`SIGINT` trigger the graceful path: final `write_db_torrents` +
`write_db_users`, DB flush, `pid_file` removal.

### systemd

```ini
[Unit]
Description=XBT Tracker (Rust)
After=network.target mysql.service

[Service]
ExecStart=/usr/local/bin/xbt_tracker --conf_file /etc/xbt_tracker.conf
Restart=on-failure
# set "daemon = 0" in the config when running under systemd

[Install]
WantedBy=multi-user.target
```

Socket activation is supported: if `LISTEN_FDS` is set by systemd, the
inherited descriptors are used instead of binding `listen_port`.

## Configuration

`key = value` lines (whitespace around `=` and trailing whitespace are
ignored; `#`/`;` are **not** comments — unknown keys log
`unknown config name: …` and are skipped, like the original). Values are
read first from the `xbt_config` table, then from the file — **the file
wins**. All keys of the original are supported:

| Key | Default | Meaning |
|---|---|---|
| `mysql_host` | *(empty → localhost)* | MySQL server host/IP |
| `mysql_port` | *(unset → 3306)* | **new in this port**: TCP port (original hardcodes 3306, or 9306 when `mysql_database = sphinx`) |
| `mysql_user` | *(empty)* | MySQL user |
| `mysql_password` | *(empty)* | MySQL password |
| `mysql_database` | `xbt` | database name |
| `mysql_table_prefix` | `xbt_` | prefix for default table names |
| `table_files`, `table_files_users`, `table_users`, `table_announce_log`, `table_scrape_log` | *(empty → prefix + …)* | explicit table names (`table_files_users` defaults to `{prefix}peers`) |
| `column_files_fid`, `column_files_leechers`, `column_files_seeders`, `column_files_completed`, `column_users_uid` | `tid`, `leechers`, `seeders`, `completed`, `uid` | explicit column names |
| `listen_port` | `2710` | TCP+UDP port; may appear multiple times |
| `announce_interval` | `1800` | announce interval sent to clients (also `min interval`) |
| `scrape_interval` | `0` | when non-zero, advertised via `5:flagsd20:min_request_interval` in scrape responses |
| `clean_up_interval` | `60` | expired-peer cleanup period (peers older than `announce_interval` are dropped) |
| `read_config_interval` | `60` | config reload period |
| `read_db_interval` | `60` | torrent/user re-read period |
| `write_db_interval` | `15` | DB flush period |
| `anonymous_announce` | `0` | allow announces without a valid torrent pass |
| `anonymous_scrape` | `0` | allow scrapes without a valid torrent pass |
| `auto_register` | `0` | register unknown info hashes on announce |
| `daemon` | `1` | double-fork into background, write `pid_file` |
| `debug` | `0` | enable `/debug` |
| `full_scrape` | `0` | allow `/scrape` without `info_hash` |
| `gzip_scrape` | `1` | allow gzip for hash-less (full) scrapes |
| `log_access` | `0` | raw request log (`xbt_tracker_raw.log` in the working dir) |
| `log_announce` | `0` | log announces to `table_announce_log` |
| `log_scrape` | `0` | log scrapes to `table_scrape_log` |
| `offline_message` | *(empty)* | when set, every announce fails with this bencoded message |
| `pid_file` | *(empty)* | pid file path (daemon mode / shutdown unlink) |
| `query_log` | *(empty)* | log every SQL statement (truncated to 999 chars, like the original) |
| `redirect_url` | *(empty)* | 302 target for requests that produce no response and carry no `info_hash` |
| `torrent_pass_private_key` | *(auto-generated)* | 27-char key for torrent-pass derivation; generated and stored in `xbt_config` on first run |

## HTTP interface

Identical to the original, including response bytes:

| Path | Behaviour |
|---|---|
| `/announce?info_hash=…&peer_id=…&port=…&uploaded=…&downloaded=…&left=…[&event=…][&numwant=…][&ip=…]` | bencoded announce response (`complete`/`incomplete`/`interval`/`min interval`/`peers`[/`peers6`]) |
| `/{32-hex torrent_pass}/announce…` | announce authenticated as that pass's user |
| `/scrape?info_hash=…[&info_hash=…]` | per-hash scrape; hash-less full scrape requires `full_scrape = 1` |
| `/statistics` (and any `/s…` path whose 7th byte is `t`, e.g. `/stats`) | HTML statistics page |
| `/metrics` | `xbt_uptime`, `xbt_infohashes_count`, `xbt_peers_count`, `xbt_connections_count`, `xbt_requests_count` lines |
| `/debug` | HTML torrent/peer dump (`debug = 1`); `?info_hash=…` shows the peer table |
| anything else | `HTTP/1.0 404 Not Found` (or 302 to `redirect_url` when applicable) |

Responses are `HTTP/1.0`, the connection is closed after the reply, the
request is capped at 4 KB and idle connections time out after 10 s —
matching the original.

## UDP interface (BEP-15)

Standard connect → announce/scrape flow on the same `listen_port`. The
connection id is derived from the client address and a per-process random
8-byte secret, as in the original (see deviation #2 below for the exact
input bytes).

## Fidelity notes and intentional deviations

Behaviour verified identical to the C++ original (including quirks):

- bare-`LF` header parsing leaves the connection state machine at state 3
  (the original's do/while scan loop exits early) — response still sent
- `/stats` routes to statistics through the absolute `v[6] == 't'` check
- HTTP announces with `port=0` are accepted and inserted (only UDP
  rejects port 0); a missing `peer_id` inserts an all-zero peer id
- `/debug` prints the dirty flag as the integers `42`/`32` — the
  original streams a `char` ternary as `int`
- scrape entries end `…incompletei{N}e` + `e` per file + `ee`
- gzip only when `compressed + 24 < raw`; `announce_log` rows are written
  before validation (failed announces appear in the log too)
- SQL statements are byte-identical to the original's (`@name`
  substitution, `values(col)` upserts, `unix_timestamp()`, batched
  `INSERT IGNORE … ON DUPLICATE KEY UPDATE`, `insert delayed` for logs)

Intentional deviations (documented bug fixes / platform adaptations):

1. **UDP replies actually work.** The original passes
   `sizeof(sockaddr_in)` (16) as the address length to `sendto` on an
   `AF_INET6` socket; Linux ≥ 4.x rejects that with `EINVAL`, so the
   stock binary never delivers a UDP reply. This port sends to the real
   source address.
2. **Deterministic connection id.** The original's IPv6 refactor left
   `write_int(8, s, secret)` at offset 0 and then `memcpy(s, addr, 16)`
   over it — hashing 8 bytes of *uninitialized stack* (UB). This port
   uses the clearly intended `sha1(secret_be(8) || addr16)[0..8]`
   (matching the pre-IPv6 layout).
3. **`mysql_port` option added** (the original hardcodes 3306, or 9306
   for `mysql_database = sphinx`).
4. **Binary SQL parameters are emitted as `X'…'` hex literals** instead
   of escaped raw bytes. Stored bytes are identical; `query_log` text
   differs accordingly.
5. Empty `mysql_host` connects to `localhost` over TCP (the original's
   libmysqlclient would use a Unix socket).
6. `srand`/`rand` replaced by a seeded xorshift64\* PRNG (secrets and
   random strings; same non-cryptographic strength as the original).
7. Linux-only, single-binary: the epoll/select loop becomes a tokio
   runtime; all tracker state lives behind one mutex, and a dedicated DB
   thread owns the MySQL connection with FIFO command ordering — the
   concurrency-visible behaviour (timers, 10 s connection deadline,
   2 KB×10000 UDP batch bound, write-cycle ordering) is preserved.
8. Reconnect-on-lost-connection is implemented manually (the `mysql`
   crate has no `MYSQL_OPT_RECONNECT` equivalent); error detection covers
   the usual gone-away codes.

## Testing

```sh
cargo test
```

38 unit tests cover the codec helpers (`n2a`, hex/URI codecs, bencode
framing), config parsing, query building, peer selection, scrape/announce
state transitions, torrent-pass derivation, the connection state machine
(including the original's quirks) and the UDP transaction layer.

### Differential test vs. the C++ original

`tests/run_difftest.sh` is an end-to-end harness that:

1. resets two identically seeded databases (`xbt`, `xbt_rs`)
2. starts the reference C++ `xbt_tracker` on :2710 and this port on :2711
3. runs the same 20-probe HTTP sequence against both (announces with all
   event types, torrent-pass URLs, auto-registration, error paths,
   single/multi/full scrape, statistics, metrics, debug, gzip) and
   compares responses after normalizing only inherently unstable aspects
   (unordered-map iteration order, uptime/rate counters, per-process
   secrets, deflate encoder differences)
4. waits for both write cycles, then diffs the full database contents
   (torrents, peers, users, announce/scrape logs, config)
5. exercises the UDP protocol on the Rust side (the reference cannot
   reply — deviation #1 — which the harness asserts)

```sh
tests/run_difftest.sh [path/to/cxx/xbt_tracker] [path/to/rust/xbt_tracker]
# defaults: /tmp/ref/xbt_tracker and target/release/xbt_tracker
```

Requirements: a local MySQL-compatible server (`mysql` CLI reachable as
root via unix socket) and Python 3. Current status: **26/26 checks pass,
database contents identical**.

## Project layout

| Rust module | Ports |
|---|---|
| `src/main.rs` | `Tracker/main.cpp` (`main1`, `srv_run`) |
| `src/tracker.rs` | `Tracker/tracker.cpp` (+ `tracker.h`) |
| `src/connection.rs` | `Tracker/connection.cpp` |
| `src/transaction.rs` | `Tracker/transaction.cpp` (UDP/BEP-15) |
| `src/tracker_input.rs` | `Tracker/tracker_input.cpp` |
| `src/config.rs` | `Tracker/config.cpp` + `misc/config_base.h` |
| `src/database.rs` | `misc/database.cpp` (+ daemon/threading from `epoll.cpp`/`main.cpp`) |
| `src/sql.rs` | `misc/xbt/make_query.h`, `Cdatabase::replace_names`, `Csql_row` |
| `src/server.rs` | `Tracker/epoll.cpp` loop, listeners, `daemon()` |
| `src/bt_misc.rs`, `src/stream_int.rs`, `src/gzip.rs`, `src/rng.rs` | `misc/bt_misc.cpp`, `misc/xbt/stream_int.h`, `misc/xcc_z.cpp`, `srand/rand` |

--

2026 [ ivan deus ]
