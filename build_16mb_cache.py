#!/usr/bin/env python3
"""
build_16mb_cache.py — extract network stats from 16MB PCAP files and write
.cache.json + .cache.npz next to each one.

Run from the terminal (not Jupyter) to avoid kernel memory limits:
    cd mqtt_benchmark
    python3 build_16mb_cache.py

Once caches exist the notebook loads them in seconds without invoking tshark.
Processes one file at a time; safe to interrupt and re-run (already-cached
files are skipped automatically).
"""

import gc
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

TSHARK = shutil.which("tshark") or "/usr/bin/tshark"
RESULTS_DIR = Path(__file__).parent / "results"


# ── Helpers (mirrors notebook cell 0e200d36) ─────────────────────────────────

def _cache_paths(pcap):
    base = Path(pcap).with_suffix("")
    return base.with_suffix(".cache.json"), base.with_suffix(".cache.npz")


def load_cache(pcap):
    jpath, npath = _cache_paths(pcap)
    if not (jpath.exists() and npath.exists()):
        return None
    with open(jpath) as f:
        d = json.load(f)
    npz = np.load(npath)
    d["rtt_arr"] = npz["rtt_arr"]
    d["ovh_arr"] = npz["ovh_arr"]
    offsets = npz["tl_sec"].astype(int)
    d["timeline"] = {
        int(s): {"bytes": int(b), "retrans": int(r), "zw": int(z), "pub": int(p)}
        for s, b, r, z, p in zip(
            offsets, npz["tl_bytes"], npz["tl_retrans"], npz["tl_zw"], npz["tl_pub"]
        )
    }
    return d


def save_cache(pcap, stats):
    jpath, npath = _cache_paths(pcap)
    scalars = {k: v for k, v in stats.items()
               if k not in ("rtt_arr", "ovh_arr", "timeline")}
    with open(jpath, "w") as f:
        json.dump(scalars, f)
    tl = stats["timeline"]
    secs = sorted(tl)
    np.savez_compressed(
        npath,
        rtt_arr    = stats["rtt_arr"].astype(np.float32),
        ovh_arr    = stats["ovh_arr"].astype(np.float32),
        tl_sec     = np.array(secs, dtype=np.int32),
        tl_bytes   = np.array([tl[s]["bytes"]   for s in secs], dtype=np.int64),
        tl_retrans = np.array([tl[s]["retrans"] for s in secs], dtype=np.int32),
        tl_zw      = np.array([tl[s]["zw"]      for s in secs], dtype=np.int32),
        tl_pub     = np.array([tl[s]["pub"]      for s in secs], dtype=np.int32),
    )


def extract_all(pcap):
    cmd = [
        TSHARK, "-r", str(pcap),
        "-Y", "tcp.port == 1883",
        "-T", "fields", "-E", "separator=\t",
        "-e", "frame.time_epoch",
        "-e", "tcp.stream",
        "-e", "tcp.len",
        "-e", "tcp.flags.syn",
        "-e", "tcp.flags.ack",
        "-e", "tcp.flags.reset",
        "-e", "tcp.analysis.retransmission",
        "-e", "tcp.analysis.zero_window",
        "-e", "tcp.analysis.ack_rtt",
        "-e", "mqtt.msgtype",
    ]

    t_min = t_max = None
    total_bytes = retrans = zero_win = resets = pub_count = n_packets = 0
    rtt_list  = []
    syn_t     = {}
    first_pub = {}
    timeline  = {}

    last_report = time.time()

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          text=True, bufsize=1 << 20) as proc:
        for raw in proc.stdout:
            parts = raw.rstrip("\n").split("\t")
            if len(parts) < 10:
                continue

            try:
                ts = float(parts[0])
            except ValueError:
                continue

            if t_min is None:
                t_min = ts
            t_max = ts

            sec = int(ts - t_min)
            bkt = timeline.setdefault(sec, {"bytes": 0, "retrans": 0, "zw": 0, "pub": 0})

            try:
                nb = int(parts[2]) if parts[2] else 0
            except ValueError:
                nb = 0
            total_bytes += nb
            bkt["bytes"] += nb

            if parts[6]:
                retrans += 1; bkt["retrans"] += 1
            if parts[7]:
                zero_win += 1; bkt["zw"] += 1
            if parts[5] == "1":
                resets += 1

            if parts[8]:
                try:
                    rtt_list.append(float(parts[8]) * 1000)
                except ValueError:
                    pass

            stream = parts[1]
            if parts[9] == "3":
                pub_count += 1; bkt["pub"] += 1
                if stream and stream not in first_pub:
                    first_pub[stream] = ts

            if parts[3] == "1" and parts[4] != "1":
                if stream and stream not in syn_t:
                    syn_t[stream] = ts

            n_packets += 1

            # Progress every 30 s
            now = time.time()
            if now - last_report >= 30:
                elapsed = now - (last_report - 30 + 30)
                print(f"    ... {n_packets:,} packets  {pub_count} publishes  "
                      f"{retrans} retrans  elapsed {int(time.time() - last_report + 30)}s",
                      flush=True)
                last_report = now

    duration = (t_max - t_min) if (t_min and t_max) else 1

    ovh_list = []
    for s, st in syn_t.items():
        pt = first_pub.get(s)
        if pt and pt > st:
            ovh_list.append((pt - st) * 1000)

    rtt_arr = np.array(rtt_list, dtype=np.float32)
    ovh_arr = np.array(ovh_list, dtype=np.float32)

    def pct(arr, q):
        return round(float(np.percentile(arr, q)), 2) if len(arr) else None

    return {
        "duration_s":           round(duration, 1),
        "total_bytes":          total_bytes,
        "n_packets":            n_packets,
        "mqtt_publishes":       pub_count,
        "throughput_KB/s":      round(total_bytes / max(duration, 1) / 1024, 1),
        "rtt_p50_ms":           pct(rtt_arr, 50),
        "rtt_p95_ms":           pct(rtt_arr, 95),
        "rtt_p99_ms":           pct(rtt_arr, 99),
        "conn_overhead_p50_ms": pct(ovh_arr, 50),
        "conn_overhead_p95_ms": pct(ovh_arr, 95),
        "retransmissions":      retrans,
        "retrans_rate_%":       round(100 * retrans / max(n_packets, 1), 3),
        "zero_window_events":   zero_win,
        "tcp_resets":           resets,
        "rtt_arr":  rtt_arr,
        "ovh_arr":  ovh_arr,
        "timeline": timeline,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    pcaps = sorted(RESULTS_DIR.glob("16mb/captures/*_s200.pcap"))
    if not pcaps:
        # Fall back to unstripped originals if no _s200 copies exist
        pcaps = sorted(RESULTS_DIR.glob("16mb/captures/*.pcap"))
        pcaps = [p for p in pcaps if "_s200" not in p.name and not p.name.startswith("_")]

    if not pcaps:
        print(f"No 16MB PCAPs found under {RESULTS_DIR}/16mb/captures/")
        sys.exit(1)

    print(f"Found {len(pcaps)} 16MB PCAP(s)\n")

    for pcap in pcaps:
        label = re.match(r"([a-zA-Z]+_[^_]+)_qos", pcap.stem)
        label = label.group(1) if label else pcap.stem[:20]
        mb = pcap.stat().st_size / 1024 ** 2

        jpath, _ = _cache_paths(pcap)
        if jpath.exists():
            print(f"[skip]   {label}  ({mb:.0f} MB)  — cache already exists")
            continue

        print(f"[tshark] {label}  ({mb:.0f} MB)  — this will take ~30 min ...", flush=True)
        t0 = time.time()
        stats = extract_all(pcap)
        elapsed = time.time() - t0

        save_cache(pcap, stats)
        print(f"         done in {elapsed/60:.1f} min  —  "
              f"pub={stats['mqtt_publishes']}  "
              f"retrans={stats['retransmissions']}  "
              f"resets={stats['tcp_resets']}  "
              f"rtt_p50={stats['rtt_p50_ms']}ms  "
              f"conn_p50={stats['conn_overhead_p50_ms']}ms")
        print(f"         cached → {jpath.name}\n")

        del stats
        gc.collect()

    print("All done. Re-run the notebook — 16MB files will load from cache instantly.")


if __name__ == "__main__":
    main()
