#!/usr/bin/env python3
"""Differential test: C++ xbt_tracker (reference, port 2710) vs the Rust
port (port 2711), both backed by identically seeded MySQL databases.

An identical probe sequence is run against both servers. Responses are
compared after normalizing:
  * peer/file list order  (both use unordered maps -> unspecified order)
  * uptime / per-second rates in statistics & debug pages (numbers masked)
  * xbt_uptime and slow-tcp counters in metrics (timing dependent)
  * UDP connection_id (derived from a per-process random secret)

The reference server cannot send UDP replies on modern Linux kernels
(sendto with msg_namelen=16 on an AF_INET6 socket -> EINVAL, a known
upstream bug), so for UDP the harness verifies that the reference stays
silent while the Rust port answers with structurally valid BEP-15.

Usage: difftest.py [--ref-port 2710] [--rs-port 2711]
Exit code 0 = all comparisons passed.
"""

import argparse
import hashlib
import re
import socket
import struct
import sys
import time
import urllib.parse

HOST = "127.0.0.1"
PASS_KEY = b"DIFFTESTKEY0123456789abcdef"  # seeded into xbt_config on both DBs
HASH_A = b"\x02" * 20
HASH_B = b"\x05" * 20
HASH_C = b"\x06" * 20

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(("PASS " if ok else "FAIL ") + name + ((" | " + detail) if detail and not ok else ""))
    return ok


def enc(b):
    return urllib.parse.quote_from_bytes(b, safe="")


def torrent_pass(uid, version, info_hash, key=PASS_KEY):
    s = key + b" " + str(version).encode() + b" " + str(uid).encode() + b" " + info_hash
    return ("%08x" % uid).encode() + hashlib.sha1(s).digest()[:12].hex().encode()


def tcp_raw(port, req, timeout=12):
    """Send raw request bytes, return the complete response (read to EOF)."""
    s = socket.create_connection((HOST, port), timeout=timeout)
    s.settimeout(timeout)
    s.sendall(req)
    out = []
    try:
        while True:
            b = s.recv(65536)
            if not b:
                break
            out.append(b)
    except socket.timeout:
        out.append(b"<<TIMEOUT>>")
    s.close()
    return b"".join(out)


def http_get(port, path, extra_headers=b""):
    return tcp_raw(port, b"GET " + path + b" HTTP/1.1\r\nHost: t\r\nConnection: close\r\n"
                   + extra_headers + b"\r\n")


def split_head_body(resp):
    i = resp.find(b"\r\n\r\n")
    if i < 0:
        return resp, b""
    return resp[: i + 4], resp[i + 4:]


def norm_peers(resp):
    """Sort the compact peer lists (order is unspecified in both impls)."""
    head, body = split_head_body(resp)
    for key, n in ((b"5:peers", 6), (b"6:peers6", 18)):
        j = body.find(key)
        if j < 0:
            continue
        m = re.match(rb"(\d+):", body[j + len(key):])
        if not m:
            continue
        ln = int(m.group(1))
        start = j + len(key) + m.end()
        data = body[start:start + ln]
        body = body[:start] + b"".join(sorted(data[i:i + n] for i in range(0, ln, n))) + body[start + ln:]
    return head + body


def norm_scrape(resp):
    """Sort per-file entries of a (full) scrape response."""
    resp = maybe_gunzip(resp)
    head, body = split_head_body(resp)
    entries = re.findall(
        rb"20:(.{20}d8:completei\d+e10:downloadedi\d+e10:incompletei\d+ee)", body, re.S)
    if len(entries) > 1:
        m = re.match(rb"d5:filesd(.*)ee$", body, re.S)
        if m and b"".join(b"20:" + e for e in entries) == m.group(1):
            body = b"d5:filesd" + b"".join(b"20:" + e for e in sorted(entries)) + b"ee"
    return head + body


def mask_nums(b):
    return re.sub(rb"\d+", b"#", b)


def maybe_gunzip(resp):
    head, body = split_head_body(resp)
    if b"Content-Encoding: gzip" in head:
        import gzip as gz
        try:
            body = gz.decompress(body)
        except OSError:
            pass
        head = head.replace(b"Content-Encoding: gzip\r\n", b"")
    return head + body


def norm_debug(resp):
    """Sort the per-torrent <tr> rows (unordered-map iteration order) and
    mask volatile numbers (tid, counters)."""
    resp = maybe_gunzip(resp)
    head, body = split_head_body(resp)
    m = re.match(rb"(.*?<table>)(.*)(</table>\s*)$", body, re.S)
    if m:
        prefix, mid, suffix = m.groups()
        rows = sorted(mid.split(b"<tr>"))
        body = prefix + b"<tr>".join(rows) + suffix
    return head + mask_nums(body)


def norm_metrics(resp):
    head, body = split_head_body(resp)
    body = re.sub(rb"(xbt_uptime )\d+", rb"\1#", body)
    body = re.sub(rb'(xbt_connections_count\{proto="tcp",type="slow"\} )\d+', rb"\1#", body)
    return head + body


def announce_path(info_hash, peer_id, port, left, uploaded=0, downloaded=0, event=None, extra=""):
    p = ("/announce?info_hash=" + enc(info_hash) + "&peer_id=" + enc(peer_id)
         + "&port=" + str(port) + "&uploaded=" + str(uploaded)
         + "&downloaded=" + str(downloaded) + "&left=" + str(left))
    if event:
        p += "&event=" + event
    return (p + extra).encode()


# The probe sequence: (name, path or (path, extra_headers), normalizer)
def build_probes():
    pid = lambda n: ("PID%d" % n).ljust(20, "-").encode()
    return [
        ("root 404", b"/", lambda r: mask_nums(maybe_gunzip(r))),
        ("announce A start p1", announce_path(HASH_A, pid(1), 1111, 100, event="started"), norm_peers),
        ("announce A p2 seeder", announce_path(HASH_A, pid(2), 2222, 0), norm_peers),
        ("announce A p1 completed", announce_path(HASH_A, pid(1), 1111, 0, uploaded=50,
                                                  downloaded=100, event="completed"), norm_peers),
        ("announce A p2 stopped", announce_path(HASH_A, pid(2), 2222, 0, event="stopped"), norm_peers),
        ("scrape A", b"/scrape?info_hash=" + enc(HASH_A).encode(), norm_scrape),
        ("scrape full", b"/scrape", norm_scrape),
        ("scrape multi", b"/scrape?info_hash=" + enc(HASH_A).encode() + b"&info_hash="
         + enc(HASH_B).encode(), norm_scrape),
        ("announce B autoreg", announce_path(HASH_B, pid(3), 3333, 50), norm_peers),
        ("announce port 0", announce_path(HASH_A, pid(4), 0, 10), norm_peers),
        ("announce no peer_id", b"/announce?info_hash=" + enc(HASH_A).encode() + b"&port=1&left=1", norm_peers),
        ("announce pass path", announce_path(HASH_C, pid(5), 5555, 10).replace(
            b"/announce?", b"/" + torrent_pass(1, 0, HASH_C) + b"/announce?"), norm_peers),
        ("announce bad pass", announce_path(HASH_A, pid(6), 6666, 10).replace(
            b"/announce?", b"/" + b"f" * 32 + b"/announce?"), norm_peers),
        ("announce empty query", b"/announce", norm_peers),
        ("announce numwant 1", announce_path(HASH_A, pid(7), 8888, 30, extra="&numwant=1"), norm_peers),
        ("statistics", b"/statistics", lambda r: mask_nums(maybe_gunzip(r))),
        ("stats v6 quirk", b"/stats", lambda r: mask_nums(maybe_gunzip(r))),
        ("metrics", b"/metrics", norm_metrics),
        ("debug", b"/debug", norm_debug),
        ("scrape gzip", (b"/scrape", b"Accept-Encoding: gzip\r\n"), lambda r: norm_scrape(maybe_gunzip(r))),
    ]


def run_http(port):
    out = {}
    for name, path, _ in build_probes():
        extra = b""
        if isinstance(path, tuple):
            path, extra = path
        out[name] = http_get(port, path, extra)
    return out


def udp_exchange(port, payload, timeout=2.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(payload, (HOST, port))
        data, _ = s.recvfrom(4096)
        return data
    except socket.timeout:
        return None
    finally:
        s.close()


def udp_probes(port):
    """Full BEP-15 flow; returns dict of probe -> parsed structural result."""
    res = {}
    conn = udp_exchange(port, struct.pack(">QII", 0x41727101980, 0, 0x11223344))
    if conn is None or len(conn) < 16:
        res["connect"] = None
        return res
    action, tid, conn_id = struct.unpack(">IIQ", conn[:16])
    res["connect"] = (action, tid)
    pid = b"UDPPID1UDPPID1UDPP1-"[:20]
    # conn(16) + hash(20) + pid(20) + dl/left/up(24) + event/ip/key/want(16) + port(2)
    ann = struct.pack(">QII", conn_id, 1, 0x55667788) + HASH_A + pid \
        + struct.pack(">qqq", 0, 42, 0) + struct.pack(">iiiiH", 0, 0, -1, 0, 7777)
    assert len(ann) == 98, len(ann)
    a = udp_exchange(port, ann)
    if a and len(a) >= 20:
        act, atid, interval, leech, seed = struct.unpack(">IIIII", a[:20])
        peers = a[20:]
        res["announce"] = (act, atid, interval, leech, seed,
                           b"".join(sorted(peers[i:i + 6] for i in range(0, len(peers), 6))))
    else:
        res["announce"] = None
    scr = struct.pack(">QII", conn_id, 2, 0x77889900) + HASH_A + HASH_B
    sc = udp_exchange(port, scr)
    if sc and len(sc) >= 8:
        act, stid = struct.unpack(">II", sc[:8])
        res["scrape"] = (act, stid, sc[8:])
    else:
        res["scrape"] = None
    bad = struct.pack(">QII", 0xDEADBEEF, 1, 0x0BADF00D) + HASH_A + pid \
        + struct.pack(">qqq", 0, 1, 0) + struct.pack(">iiiiH", 0, 0, -1, 0, 1)
    assert len(bad) == 98
    res["bad_conn_id"] = udp_exchange(port, bad, timeout=1.5)
    badproto = udp_exchange(port, struct.pack(">QII", 0x1, 0, 0x33), timeout=1.5)
    res["bad_proto"] = badproto
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref-port", type=int, default=2710)
    ap.add_argument("--rs-port", type=int, default=2711)
    ap.add_argument("--phase", choices=("http", "udp", "all"), default="all")
    args = ap.parse_args()

    # wait for both servers
    for port in (args.ref_port, args.rs_port):
        for _ in range(100):
            try:
                socket.create_connection((HOST, port), timeout=1).close()
                break
            except OSError:
                time.sleep(0.2)
        else:
            print("server on port %d not reachable" % port)
            return 2

    if args.phase in ("http", "all"):
        ref = run_http(args.ref_port)
        rs = run_http(args.rs_port)

        for name, _, norm in build_probes():
            a2, b2 = norm(ref[name]), norm(rs[name])
            ok = a2 == b2
            detail = ""
            if not ok:
                detail = "norm_ref=%r norm_rs=%r" % (a2[:400], b2[:400])
            check("http: " + name, ok, detail)

    if args.phase == "http":
        failed = [n for n, ok, _ in results if not ok]
        print("\n%d/%d checks passed" % (len(results) - len(failed), len(results)))
        return 1 if failed else 0

    ua, ub = udp_probes(args.ref_port), udp_probes(args.rs_port)

    # Known upstream bug: reference cannot send UDP replies (sendto EINVAL
    # because msg_namelen=16 on AF_INET6). Verify silence vs valid replies.
    check("udp: reference silent (known upstream sendto bug)",
          ua.get("connect") is None,
          "reference answered UDP: %r" % (ua.get("connect"),))
    check("udp: rust connect reply", ub.get("connect") == (0, 0x11223344), repr(ub.get("connect")))
    if ub.get("announce"):
        act, tid, interval, leech, seed, peers = ub["announce"]
        check("udp: rust announce reply", act == 1 and tid == 0x55667788
              and interval == 1800 and leech + seed >= 1 and len(peers) % 6 == 0,
              repr(ub["announce"]))
    else:
        check("udp: rust announce reply", False, "no reply")
    if ub.get("scrape"):
        act, tid, body = ub["scrape"]
        check("udp: rust scrape reply", act == 2 and tid == 0x77889900 and len(body) == 24,
              repr(ub["scrape"]))
    else:
        check("udp: rust scrape reply", False, "no reply")
    check("udp: rust bad conn id -> silence (C++ returns silently)",
          ub.get("bad_conn_id") is None, repr(ub.get("bad_conn_id")))
    check("udp: rust bad proto silent", ub.get("bad_proto") is None, repr(ub.get("bad_proto")))

    failed = [n for n, ok, _ in results if not ok]
    print("\n%d/%d checks passed" % (len(results) - len(failed), len(results)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
