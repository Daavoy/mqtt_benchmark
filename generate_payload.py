#!/usr/bin/env python3
"""
Generate synthetic MQTT payload files for broker benchmark experiments.

Creates JSON files padded to a precise byte size. Files are stored under
publisher/data/synthetic/<size_label>/ and are used via the
publisher/configs/data_synthetic_provider{1,2}.yml configs.

Usage:
    python generate_payload.py --size 1kb
    python generate_payload.py --sizes 100b 1kb 10kb 1mb
    python generate_payload.py --preset small
    python generate_payload.py --preset all
    python generate_payload.py --list

Preset groups:
    small   64b 128b 256b 512b 1kb 2kb 4kb
    medium  8kb 16kb 32kb 64kb 128kb 256kb 512kb
    large   1mb 2mb 5mb 10mb
    all     all of the above
"""
import argparse
import json
import os
import random
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(_HERE, "publisher", "data", "synthetic")

# FileVUWSN (virtualuwsn) requires TESTDATA_PATH to contain subdirectories — one per
# gateway. Files sit one level deeper (each file becomes a HubNode). The structure must be:
#
#   <size_label>/
#       gateway1/            ← scanned as a Gateway by FileVUWSN
#           payload_000.json ← FileHubNode ("hub1"), content sent as MQTT payload
#           payload_001.json ← FileHubNode ("hub2"), ...
#
# When mounted at /app/data/synthetic in the container, FileVUWSN finds "gateway1" as the
# only subfolder, creates one Gateway named "synthetic.gateway1", and one HubNode per file.
# Topics published: site1/gateway1/hub1, site1/gateway1/hub2, ...
DEFAULT_NUM_FILES = 2  # number of gateways (and payload files); each gateway has exactly one file/HubNode

PRESETS = {
    "small":  ["64b", "128b", "256b", "512b", "1kb", "2kb", "4kb"],
    "medium": ["8kb", "16kb", "32kb", "64kb", "128kb", "256kb", "512kb"],
    "large":  ["1mb", "2mb", "5mb", "10mb"],
    "all":    ["64b", "128b", "256b", "512b", "1kb", "2kb", "4kb",
               "8kb", "16kb", "32kb", "64kb", "128kb", "256kb", "512kb",
               "1mb", "2mb", "5mb", "10mb"],
}


def parse_size(size_str: str) -> int:
    """Convert a human-readable size string to an integer byte count.

    Accepts: 64b, 1kb, 2.5mb, 1gb (case-insensitive). Plain integers treated as bytes.
    """
    s = size_str.strip().lower()
    for suffix, mult in [("gb", 1024**3), ("mb", 1024**2), ("kb", 1024), ("b", 1)]:
        if s.endswith(suffix):
            return max(1, int(float(s[:-len(suffix)]) * mult))
    return int(s)


def make_label(size_bytes: int) -> str:
    """Return a canonical, filesystem-safe label for a byte count (e.g. 1024 → '1kb')."""
    if size_bytes >= 1024**3:
        return f"{size_bytes // 1024**3}gb"
    if size_bytes >= 1024**2:
        return f"{size_bytes // 1024**2}mb"
    if size_bytes >= 1024:
        return f"{size_bytes // 1024}kb"
    return f"{size_bytes}b"


def _build_payload(target_bytes: int, index: int) -> bytes:
    """Return a JSON payload as bytes whose length is as close to target_bytes as possible.

    The base structure is a minimal sensor-reading object. A '_pad' field is added
    and sized so the serialised JSON reaches exactly target_bytes. For very small
    targets (< ~80 bytes) the JSON is truncated, which is intentional — the publisher
    sends raw file bytes, so the subscriber will still record the correct payload_size.
    """
    base = {
        "source": "benchmark-synthetic",
        "source_id": f"synth{index:03d}",
        "format": "BENCHMARK_V1",
        "values": [{"timestamp": 0, "value": round(random.uniform(-10.0, 40.0), 4)}],
    }

    base_raw = json.dumps(base, separators=(",", ":")).encode()

    # Bytes consumed by: ,"_pad":"" — exact overhead is 10 chars (9 ASCII + comma)
    PAD_OVERHEAD = len(b',"_pad":""')
    padding_needed = target_bytes - len(base_raw) - PAD_OVERHEAD

    if padding_needed > 0:
        base["_pad"] = "x" * padding_needed
    # If padding_needed <= 0 the base already meets or exceeds target — skip the pad field.

    raw = json.dumps(base, separators=(",", ":")).encode()

    # Single-byte correction loop (rarely needs more than one pass)
    for _ in range(4):
        diff = target_bytes - len(raw)
        if diff == 0:
            break
        if "_pad" in base and len(base["_pad"]) + diff >= 0:
            base["_pad"] = "x" * (len(base["_pad"]) + diff)
            raw = json.dumps(base, separators=(",", ":")).encode()
        else:
            break

    # For very small targets: hard truncate (subscriber measures actual received bytes)
    if len(raw) > target_bytes:
        raw = raw[:target_bytes]

    return raw


def generate(
    sizes: list,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    num_files: int = DEFAULT_NUM_FILES,
) -> dict:
    """Generate payload files for every size in the list.

    Returns a dict: { label -> {"target_bytes": int, "dir": str, "files": [(path, actual_bytes), ...]} }
    """
    results = {}
    for size_str in sizes:
        size_bytes = parse_size(size_str)
        label = make_label(size_bytes)
        size_dir = os.path.join(output_dir, label)

        # One gateway directory per payload file.
        # FileVUWSN creates one Gateway per subdirectory and one HubNode per file inside it.
        # With one file per gateway, each gateway gets its own MQTTPublisher — no shared-client
        # race condition where hub N+1 tries to publish before the client reconnects from hub N.
        #
        # Structure:
        #   <size>/
        #       gateway1/payload_000.json  → Gateway "synthetic.gateway1", 1 hub → topic site1/gateway1/hub1
        #       gateway2/payload_001.json  → Gateway "synthetic.gateway2", 1 hub → topic site1/gateway2/hub1
        #       ...
        files = []
        for i in range(num_files):
            gateway_dir = os.path.join(size_dir, f"gateway{i + 1}")
            os.makedirs(gateway_dir, exist_ok=True)
            path = os.path.join(gateway_dir, "payload_000.json")
            payload = _build_payload(size_bytes, i)
            with open(path, "wb") as f:
                f.write(payload)
            actual = os.path.getsize(path)
            files.append((path, actual))

        results[label] = {"target_bytes": size_bytes, "dir": size_dir, "files": files}

        actual_sample = files[0][1] if files else 0
        diff = actual_sample - size_bytes
        diff_str = f" (diff {diff:+d}B)" if diff != 0 else ""
        print(f"  [{label:>8}]  target={size_bytes}B  actual={actual_sample}B{diff_str}  → {size_dir}/gateway{{1..{num_files}}}/")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic MQTT payload files for broker benchmarking.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--size",   metavar="SIZE",
                       help="Single size, e.g. 64b, 1kb, 2.5mb")
    group.add_argument("--sizes",  metavar="SIZE", nargs="+",
                       help="Space-separated sizes, e.g. 100b 1kb 10mb")
    group.add_argument("--preset", choices=PRESETS.keys(),
                       help="Named size range (see --list)")
    group.add_argument("--list",   action="store_true",
                       help="Print available presets and exit")

    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, metavar="DIR",
                        help=f"Root output directory (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--num-files", type=int, default=DEFAULT_NUM_FILES, metavar="N",
                        help=f"Distinct files per size bucket (default: {DEFAULT_NUM_FILES})")
    args = parser.parse_args()

    if args.list:
        print("Available presets:")
        for name, sizes in PRESETS.items():
            print(f"  {name:<8}  {' '.join(sizes)}")
        sys.exit(0)

    sizes = PRESETS[args.preset] if args.preset else (args.sizes if args.sizes else [args.size])

    print(f"Generating {len(sizes)} size(s) into {args.output_dir}/")
    generate(sizes, output_dir=args.output_dir, num_files=args.num_files)
    print("Done.")


if __name__ == "__main__":
    main()
