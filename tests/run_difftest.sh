#!/bin/bash
# Differential test driver: resets two identically-seeded databases
# (xbt for the C++ reference, xbt_rs for the Rust port), starts both
# trackers, runs tests/difftest.py and finally diffs the DB contents.
#
# Usage: tests/run_difftest.sh [path/to/xbt_tracker_cxx] [path/to/xbt_tracker_rs]
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
REF_BIN="${1:-/tmp/ref/xbt_tracker}"
RS_BIN="${2:-$ROOT/target/release/xbt_tracker}"
REF_DIR=/tmp/difftest/ref
RS_DIR=/tmp/difftest/rs
MYSQL="mysql -h127.0.0.1 -uxbt -pxbtpass"
MYSQL_ROOT="mysql"

mkdir -p "$REF_DIR" "$RS_DIR"

# ---------------------------------------------------------------- databases
$MYSQL_ROOT -e "
DROP DATABASE IF EXISTS xbt; CREATE DATABASE xbt;
DROP DATABASE IF EXISTS xbt_rs; CREATE DATABASE xbt_rs;
GRANT ALL PRIVILEGES ON xbt.* TO 'xbt'@'%';
GRANT ALL PRIVILEGES ON xbt_rs.* TO 'xbt'@'%';
FLUSH PRIVILEGES;" || { echo "DB reset failed"; exit 2; }

for db in xbt xbt_rs; do
  $MYSQL_ROOT "$db" < "$ROOT/xbt_tracker.sql" || { echo "schema import failed for $db"; exit 2; }
  $MYSQL_ROOT "$db" -e "
CREATE TABLE IF NOT EXISTS xbt_announce_log (
  id int(11) NOT NULL AUTO_INCREMENT,
  ipv6 varbinary(16) NOT NULL,
  port int(11) NOT NULL,
  event int(11) NOT NULL,
  info_hash varbinary(20) NOT NULL,
  peer_id varbinary(20) NOT NULL,
  downloaded bigint(20) unsigned NOT NULL,
  left0 bigint(20) unsigned NOT NULL,
  uploaded bigint(20) unsigned NOT NULL,
  uid int(11) NOT NULL,
  mtime int(11) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM;
CREATE TABLE IF NOT EXISTS xbt_scrape_log (
  id int(11) NOT NULL AUTO_INCREMENT,
  ipv6 varbinary(16) NOT NULL,
  uid int(11) NOT NULL,
  mtime int(11) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM;
INSERT INTO xbt_config (name, value) VALUES ('torrent_pass_private_key', 'DIFFTESTKEY0123456789abcdef');
INSERT INTO xbt_users (uid, torrent_pass, downloaded, uploaded) VALUES (1, REPEAT('0', 32), 0, 0);" \
    || { echo "seed failed for $db"; exit 2; }
done

# ------------------------------------------------------------------- confs
cat > "$REF_DIR/xbt_tracker.conf" <<EOF
mysql_host = 127.0.0.1
mysql_port = 3306
mysql_user = xbt
mysql_password = xbtpass
mysql_database = xbt
listen_port = 2710
daemon = 0
debug = 1
full_scrape = 1
log_announce = 1
log_scrape = 1
anonymous_announce = 1
anonymous_scrape = 1
auto_register = 1
EOF
sed 's/^mysql_database = xbt$/mysql_database = xbt_rs/; s/^listen_port = 2710$/listen_port = 2711/' \
  "$REF_DIR/xbt_tracker.conf" > "$RS_DIR/xbt_tracker.conf"

# ----------------------------------------------------------------- servers
pkill -f "xbt_tracker --conf_file" 2>/dev/null
sleep 0.5
(cd "$REF_DIR" && setsid nohup "$REF_BIN" --conf_file xbt_tracker.conf >stdout.log 2>&1 &)
(cd "$RS_DIR" && setsid nohup "$RS_BIN" --conf_file xbt_tracker.conf >stdout.log 2>&1 &)

fail=0
# HTTP phase first, then DB comparison, then UDP (the reference cannot
# answer UDP at all, so its DB never sees the UDP announce; keeping UDP
# out of the DB-diff phase makes the comparison exact)
python3 "$HERE/difftest.py" --ref-port 2710 --rs-port 2711 --phase http || fail=1

# Wait for both servers to flush (15s write-db cycles; the reference's
# epoll loop can defer timers by up to 5s): poll until the dumps are
# non-trivial and stable across two consecutive samples.
dump_count() {
  { mysql -h127.0.0.1 -uxbt -pxbtpass -N -B xbt -e "SELECT COUNT(*) FROM xbt_torrents"
    mysql -h127.0.0.1 -uxbt -pxbtpass -N -B xbt_rs -e "SELECT COUNT(*) FROM xbt_torrents"; } 2>/dev/null
}
stable=0
prev=""
for i in $(seq 1 30); do
  sleep 3
  cur="$(dump_count | tr -s ' \n' '  ')"
  read -r n1 n2 _ <<< "$cur"
  if [ "$cur" = "$prev" ] && [ "${n1:-0}" -gt 0 ] && [ "${n2:-0}" -gt 0 ]; then
    stable=1
    break
  fi
  prev="$cur"
done
[ "$stable" = 1 ] || echo "WARNING: databases not stable after 90s"

# --------------------------------------------------------------- db diff
dump() { # $1 = database
  # tid values are excluded: both servers iterate unordered maps when
  # flushing new torrents, so auto-increment assignment order differs
  # run to run (in both implementations); content is compared per hash.
  for spec in \
    "xbt_torrents|SELECT HEX(info_hash), seeders, leechers, completed, flags FROM xbt_torrents ORDER BY info_hash" \
    "xbt_peers|SELECT HEX(t.info_hash), p.uid, p.active, p.completed, p.downloaded, p.\`left\`, p.uploaded FROM xbt_peers p LEFT JOIN xbt_torrents t USING (tid) ORDER BY 1, 2" \
    "xbt_users|SELECT uid, torrent_pass, downloaded, uploaded FROM xbt_users ORDER BY uid" \
    "xbt_announce_log|SELECT HEX(ipv6), port, event, HEX(info_hash), HEX(peer_id), downloaded, left0, uploaded, uid FROM xbt_announce_log ORDER BY id" \
    "xbt_scrape_log|SELECT HEX(ipv6), uid FROM xbt_scrape_log ORDER BY id" \
    "xbt_config|SELECT name, value FROM xbt_config ORDER BY name"
  do
    echo "-- ${spec%%|*}"
    $MYSQL -h127.0.0.1 -uxbt -pxbtpass -N -B "$1" -e "${spec#*|}" 2>/dev/null
  done
}
dump xbt > /tmp/difftest/db_ref.txt
dump xbt_rs > /tmp/difftest/db_rs.txt
if diff -u /tmp/difftest/db_ref.txt /tmp/difftest/db_rs.txt > /tmp/difftest/db.diff; then
  echo "PASS db: identical contents"
else
  echo "FAIL db: differences:"
  head -50 /tmp/difftest/db.diff
  fail=1
fi

# UDP phase (response checks only; excluded from the DB diff above)
python3 "$HERE/difftest.py" --ref-port 2710 --rs-port 2711 --phase udp || fail=1

if [ "$fail" -ne 0 ]; then
  echo "--- reference stdout (tail) ---"; tail -5 "$REF_DIR/stdout.log" 2>/dev/null
  echo "--- rust stdout (tail) ---"; tail -20 "$RS_DIR/stdout.log" 2>/dev/null
fi
pkill -f "xbt_tracker --conf_file" 2>/dev/null
exit $fail
