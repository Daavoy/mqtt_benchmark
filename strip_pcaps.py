#!/usr/bin/env python3
"""
strip_pcaps.py — create snaplen-200 copies of oversized PCAP captures.

Finds every .pcap under results/ that is larger than SIZE_THRESHOLD_MB and
produces a sibling file with _s200 inserted before .pcap:

  results/125kb/captures/emqx_125kb_qos0_*.pcap
  → results/125kb/captures/emqx_125kb_qos0_*_s200.pcap

Originals are NEVER modified or deleted.
Already-stripped files (_s200.pcap) and preflight files (_preflight.pcap)
are skipped automatically.
"""

import subprocess
import sys
from pathlib import Path

RESULTS_DIR       = Path(__file__).parent / "results"
SNAPLEN           = 200          # bytes — keeps all TCP/IP/MQTT headers, strips payload
SIZE_THRESHOLD_MB = 500          # only process files larger than this


def strip(src: Path) -> Path:
    # Strip both suffixes for .pcap.gz so output is <name>_s200.pcap
    stem = src.stem if src.suffix != ".gz" else Path(src.stem).stem
    dst = src.with_name(stem + "_s200.pcap")
    if dst.exists():
        print(f"  [skip]  {dst.name} already exists")
        return dst

    src_mb = src.stat().st_size / 1024**2
    print(f"  [{src_mb:,.0f} MB]  {src.name}")
    print(f"         → {dst.name}", flush=True)

    result = subprocess.run(
        ["editcap", "-s", str(SNAPLEN), str(src), str(dst)],
        capture_output=True, text=True,
    )

    if result.returncode != 0:
        print(f"         ERROR: {result.stderr.strip()}")
        if dst.exists():
            dst.unlink()   # remove partial output only — original untouched
        return None

    dst_mb = dst.stat().st_size / 1024**2
    saved  = src_mb - dst_mb
    print(f"         done   {dst_mb:,.0f} MB  (saved {saved:,.0f} MB, "
          f"{100*saved/src_mb:.0f}% reduction)")
    return dst


def main():
    pcaps = sorted(RESULTS_DIR.glob("**/captures/*.pcap"))
    pcaps += sorted(RESULTS_DIR.glob("**/captures/*.pcap.gz"))
    candidates = [
        p for p in pcaps
        if not p.name.startswith("_")           # skip preflight
        and "_s200" not in p.name               # skip already-stripped
        and p.stat().st_size > SIZE_THRESHOLD_MB * 1024**2
    ]

    if not candidates:
        print(f"No PCAPs larger than {SIZE_THRESHOLD_MB} MB found under {RESULTS_DIR}")
        sys.exit(0)

    total_mb = sum(p.stat().st_size for p in candidates) / 1024**2
    print(f"Found {len(candidates)} oversized PCAPs ({total_mb:,.0f} MB total)")
    print(f"Stripping to snaplen={SNAPLEN} bytes — originals are never touched\n")

    saved_total = 0
    for pcap in candidates:
        dst = strip(pcap)
        if dst:
            saved_total += (pcap.stat().st_size - dst.stat().st_size) / 1024**2

    print(f"\nTotal space recovered by stripped copies: {saved_total:,.0f} MB")
    print("Originals are intact. Verify the _s200 files work in the notebook,")
    print("then delete the originals manually if you're satisfied.")


if __name__ == "__main__":
    main()
