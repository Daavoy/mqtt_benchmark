#!/usr/bin/env python3
"""
build_16mb_cache.py — extract network stats from large PCAP files and write
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

# If packet count doesn't advance by at least this many in STALL_WINDOW seconds,
# tshark is considered stuck and will be killed (partial results are saved).
STALL_MIN_PACKETS = 1_000
STALL_WINDOW_S    = 120

# Progress report interval in seconds
PROGRESS_INTERVAL_S = 30


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cache_paths(pcap):
    p = Path(pcap)
    # Strip both suffixes for .pcap.gz (Path.with_suffix only removes one)
    base = p.with_suffix("") if p.suffix != ".gz" else p.with_suffix("").with_suffix("")
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

    stalled = False

    # ── timing state ──────────────────────────────────────────────────────────
    run_start       = time.time()
    last_report_t   = run_start      # wall time of last progress print
    last_stall_t    = run_start      # wall time of last stall-check reset
    last_stall_n    = 0              # packet count at last stall-check reset

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

            now = time.time()

            # ── progress report (every PROGRESS_INTERVAL_S wall-clock seconds) ──
            if now - last_report_t >= PROGRESS_INTERVAL_S:
                elapsed_total = now - run_start
                print(
                    f"    ... {n_packets:,} packets  {pub_count} publishes  "
                    f"{retrans} retrans  "
                    f"elapsed {int(elapsed_total)}s",
                    flush=True,
                )
                last_report_t = now

            # ── stall detection (checked every STALL_WINDOW_S seconds) ──────────
            if now - last_stall_t >= STALL_WINDOW_S:
                gained = n_packets - last_stall_n
                if gained < STALL_MIN_PACKETS:
                    print(
                        f"\n    *** STALL: only {gained} new packets in "
                        f"{STALL_WINDOW_S}s — killing tshark and saving partial results ***",
                        flush=True,
                    )
                    proc.kill()
                    stalled = True
                    break
                last_stall_n = n_packets
                last_stall_t = now

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
        "partial":              stalled,   # flag so notebook can warn if needed
        "rtt_arr":              rtt_arr,
        "ovh_arr":              ovh_arr,
        "timeline":             timeline,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Fixed runs: prefer _s200 trimmed copies, fall back to originals
    pcaps = sorted(RESULTS_DIR.glob("16mb_*/captures/*_s200.pcap"))
    if not pcaps:
        pcaps = sorted(RESULTS_DIR.glob("16mb_*/captures/*.pcap"))
        pcaps = [p for p in pcaps if "_s200" not in p.name]
    # First run: .pcap.gz in results/16mb/captures/ (no _fixed/_rerun suffix)
    pcaps += sorted((RESULTS_DIR / "16mb" / "captures").glob("*.pcap.gz"))

    if not pcaps:
        print(f"No 16MB PCAPs found under {RESULTS_DIR}/16mb/captures/")
        sys.exit(1)

    total_mb = sum(p.stat().st_size for p in pcaps) / 1024 ** 2
    print(f"Found {len(pcaps)} 16MB PCAP(s)  ({total_mb:,.0f} MB total)\n")

    for pcap in pcaps:
        label = re.match(r"([a-zA-Z]+_[^_]+)_qos", pcap.stem)
        label = label.group(1) if label else pcap.stem[:20]
        mb = pcap.stat().st_size / 1024 ** 2

        jpath, _ = _cache_paths(pcap)
        if jpath.exists():
            print(f"[skip]   {label}  ({mb:,.0f} MB)  — cache already exists")
            continue

        if mb > 500:
            print(
                f"[warn]   {label} is {mb:,.0f} MB — large payloads fragment into many TCP\n"
                f"         segments so this is expected, but tshark will take a long time.\n"
                f"         Stall detection will auto-save and move on if tshark gets stuck.\n",
                flush=True,
            )

        print(f"[tshark] {label}  ({mb:,.0f} MB)  — processing ...", flush=True)
        t0 = time.time()
        stats = extract_all(pcap)
        elapsed = time.time() - t0

        save_cache(pcap, stats)

        partial_tag = "  *** PARTIAL (stalled) ***" if stats.get("partial") else ""
        print(
            f"         done in {elapsed/60:.1f} min{partial_tag}\n"
            f"         pub={stats['mqtt_publishes']}  "
            f"retrans={stats['retransmissions']}  "
            f"resets={stats['tcp_resets']}  "
            f"rtt_p50={stats['rtt_p50_ms']}ms  "
            f"conn_p50={stats['conn_overhead_p50_ms']}ms\n"
            f"         cached → {jpath.name}\n"
        )

        del stats
        gc.collect()

    print("All done. Re-run the notebook — files will load from cache instantly.")


if __name__ == "__main__":
    main()