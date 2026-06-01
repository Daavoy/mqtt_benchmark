#!/usr/bin/env python3
"""
Extended benchmark runner: orchestrates broker lifecycle, load generation,
packet capture (tcpdump → PCAP), and Prometheus metric collection for
multi-broker, multi-payload-size AUT0 experiments.

Network data captured:
  - tcpdump on the Docker benchmark network interface filtered to MQTT port 1883.
  - Saved to captures/<experiment_id>.pcap for analysis in network_analysis.ipynb.
  - Captures TCP retransmissions, zero-window events, RTT, and MQTT frame types —
    the signals needed to explain observed latency and reliability differences.

Usage:
    python run_load_updated.py \\
        --broker emqx \\
        --payload-size 1kb \\
        --qos 0 \\
        --numexecs 10 \\
        --stats results/emqx_1kb_qos0.csv

    # Real sensor data (no --payload-size):
    python run_load_updated.py --broker hivemq --numexecs 10 --stats results/hivemq_real.csv

    # Generate payloads first, then run a series:
    python generate_payload.py --preset small
    for size in 64b 1kb 10kb 100kb 1mb; do
        python run_load_updated.py --broker emqx --payload-size $size \\
            --numexecs 10 --stats results/emqx_${size}.csv
    done

Log path produced: aut0_<broker>_<size_label>/qos<N>
Add to data_analysis.ipynb: auts = [..., 'aut0_emqx_1kb']
"""
import argparse
import csv
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone

import requests

from generate_payload import generate, make_label, parse_size

SUBSCRIBER_COMPOSE = "docker-compose-subscriber.yml"

# ── Broker registry ──────────────────────────────────────────────────────────────
# prom_scrape_port / prom_scrape_path: Prometheus pull endpoint on the host.
# prom_metrics: counter names to snapshot from Prometheus after each execution.
# rest_metrics_url: JSON REST alternative used when Prometheus is unavailable (NanoMQ).
BROKER_CONFIG = {
    "hivemq": {
        "image":            "hivemq/hivemq-ce",
        "overlay":          "docker-compose-aut0-hivemq.yml",
        "prom_scrape_port": 9399,
        "prom_scrape_path": "/metrics",
        "prom_metrics": [
            # Actual names exposed by hivemq-prometheus-extension — verified against
            # transformer/logs/aut1_15b/qos0/brokers_incomming_msgs.json
            "com_hivemq_messages_incoming_publish_count",
            "com_hivemq_messages_outgoing_publish_count",
            "com_hivemq_networking_bytes_read_total",
            "com_hivemq_networking_bytes_written_total",
            "com_hivemq_subscriptions_overall_current",
        ],
        "rest_metrics_url": None,
    },
    "emqx": {
        "image":            "emqx/emqx:latest",
        "overlay":          "docker-compose-aut0-emqx.yml",
        "prom_scrape_port": 18083,
        "prom_scrape_path": "/api/v5/prometheus/stats",
        "prom_metrics": [
            "emqx_messages_received",
            "emqx_messages_sent",
            "emqx_bytes_received",
            "emqx_bytes_sent",
            "emqx_connections_count",
        ],
        "rest_metrics_url": None,
    },
    "mosquitto": {
        "image":            "eclipse-mosquitto:2",
        "overlay":          "docker-compose-aut0-mosquitto.yml",
        "prom_scrape_port": 9234,   # mosquitto-exporter sidecar (sapcc/mosquitto-exporter)
        "prom_scrape_path": "/metrics",
        "prom_metrics": [
            # sapcc/mosquitto-exporter uses broker_* prefix (from $SYS topics)
            "broker_messages_received",
            "broker_messages_sent",
            "broker_bytes_received",
            "broker_bytes_sent",
            "broker_clients_connected",
        ],
        "rest_metrics_url": None,
    },
    "nanomq": {
        "image":            "emqx/nanomq:latest",
        "overlay":          "docker-compose-aut0-nanomq.yml",
        "prom_scrape_port": 8083,
        "prom_scrape_path": "/api/v4/prometheus",
        "prom_metrics": [
            "nanomq_messages_received",
            "nanomq_messages_sent",
            "nanomq_connections_count",
            "nanomq_memory_usage",
            "nanomq_cpu_usage",
        ],
        "rest_metrics_url": None,
    },
    "rabbitmq": {
        "image":            "rabbitmq:3-management",
        "overlay":          "docker-compose-aut0-rabbitmq.yml",
        "prom_scrape_port": 15692,
        "prom_scrape_path": "/metrics",
        "prom_metrics": [
            "rabbitmq_channel_messages_published_total",
            "rabbitmq_channel_messages_delivered_total",
            "rabbitmq_connection_incoming_bytes_total",
            "rabbitmq_connection_outgoing_bytes_total",
            "rabbitmq_connections",
        ],
        "rest_metrics_url": None,
    },
}

AUT_BASE_COMPOSE   = "docker-compose-aut0.yml"
CLIENTS_COMPOSE    = "docker-compose-clients.yml"
SYNTHETIC_DATA_DIR = "publisher/data/synthetic"
PAYLOAD_OVERRIDE   = ".docker-compose-payload-override.yml"   # temp file, gitignored

# Per-broker client credential overrides.  RabbitMQ authenticates all MQTT clients
# against a single user database, so publishers must use the same password (PASSWORD2)
# as the subscriber rather than the default PASSWORD1 used for all other brokers.
BROKER_CLIENT_OVERRIDES = {
    "rabbitmq": "docker-compose-aut0-rabbitmq-clients.yml",
}


# ── Docker compose helpers ───────────────────────────────────────────────────────

def _file_args(files: list) -> list:
    args = []
    for f in files:
        args += ["-f", f]
    return args


def _compose_up(files: list, env: dict):
    """Run compose up and block until containers exit (clients mode)."""
    cmd = ["docker", "compose"] + _file_args(files) + ["up", "--force-recreate"]
    with subprocess.Popen(cmd, env=env) as proc:
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"docker compose up failed (rc={proc.returncode})")


def _compose_up_detached(files: list, env: dict):
    cmd = ["docker", "compose"] + _file_args(files) + ["up", "-d", "--force-recreate"]
    subprocess.run(cmd, env=env, check=True)


def _compose_down(files: list, env: dict):
    cmd = ["docker", "compose"] + _file_args(files) + ["down"]
    subprocess.run(cmd, env=env, check=False)


# ── Payload compose override ─────────────────────────────────────────────────────

def write_payload_override(size_label: str) -> str:
    """Write a compose override that swaps publisher config and data volume for synthetic payloads."""
    content = (
        "# Auto-generated by run_load_updated.py — do not edit.\n"
        "services:\n"
        "    provider1:\n"
        "        volumes:\n"
        f"            - ./publisher/configs/data_synthetic_provider1.yml:/app/config.yml\n"
        f"            - ./publisher/data/synthetic/{size_label}:/app/data/synthetic\n"
        "    provider2:\n"
        "        volumes:\n"
        f"            - ./publisher/configs/data_synthetic_provider2.yml:/app/config.yml\n"
        f"            - ./publisher/data/synthetic/{size_label}:/app/data/synthetic\n"
    )
    with open(PAYLOAD_OVERRIDE, "w") as f:
        f.write(content)
    return PAYLOAD_OVERRIDE


# ── Network capture: tcpdump → PCAP ─────────────────────────────────────────────

_CAPTURE_CONTAINER = "pcap-capture"
_CAPTURE_IMAGE     = "nicolaka/netshoot"


def start_capture(pcap_path: str) -> subprocess.Popen:
    """Capture MQTT traffic via a sidecar container sharing the broker network namespace.

    Uses nicolaka/netshoot with --net=container:broker so tshark sees the broker
    eth0 directly. Works in WSL2/Docker Desktop where bridge interfaces are not
    exposed on the host.

    A SIGTERM trap inside the container shell chmods the pcap to 644 before exit
    so the host user can read it without sudo.

    Returns the Popen handle, or None if the sidecar fails to start.
    """
    pcap_dir_rel = os.path.dirname(pcap_path) or "."
    os.makedirs(pcap_dir_rel, exist_ok=True)
    try:
        os.chmod(pcap_dir_rel, 0o777)
    except PermissionError:
        subprocess.run(["sudo", "chmod", "777", pcap_dir_rel], check=False)
    abs_pcap   = os.path.abspath(pcap_path)
    pcap_dir   = os.path.dirname(abs_pcap)
    pcap_fname = os.path.basename(abs_pcap)

    subprocess.run(["docker", "rm", "-f", _CAPTURE_CONTAINER],
                   capture_output=True, check=False)

    # Shell wrapper: trap SIGTERM (docker stop) -> kill tshark cleanly -> chmod pcap
    shell_cmd = (
        f"trap 'kill $tpid; wait $tpid; chmod 644 /captures/{pcap_fname}' TERM INT; "
        f"tshark -i eth0 -w /captures/{pcap_fname} -f 'port 1883' -s 200 -q & "
        f"tpid=$!; wait $tpid"
    )
    cmd = [
        "docker", "run", "--rm",
        "--name", _CAPTURE_CONTAINER,
        "--net", "container:broker",
        "-v", f"{pcap_dir}:/captures",
        "--entrypoint", "sh",
        _CAPTURE_IMAGE,
        "-c", shell_cmd,
    ]
    print(f"  Starting packet capture (sidecar) -> {pcap_path}")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    time.sleep(2)   # give tshark time to open the interface

    if proc.poll() is not None:
        err = proc.stderr.read().decode(errors="replace").strip()
        print(f"  [warn] Capture sidecar exited immediately (rc={proc.returncode}).")
        if err:
            print(f"  [warn] {err[:300]}")
        return None

    proc.stderr.close()
    return proc


def stop_capture(proc) -> None:
    """Stop the capture sidecar and wait for tshark to flush the PCAP file.

    docker stop sends SIGTERM to the shell wrapper, which kills tshark cleanly,
    chmods the pcap to 644, then exits. Accepts None (capture disabled).
    """
    if proc is None:
        return
    subprocess.run(["docker", "stop", _CAPTURE_CONTAINER],
                   capture_output=True, timeout=15, check=False)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def preflight_capture_test(captures_dir: str = "captures") -> bool:
    """Verify the capture sidecar can start and write packets while the broker is running.

    Call this after the broker is up. Returns True if a non-empty PCAP was written.
    """
    test_pcap = os.path.join(captures_dir, "_preflight.pcap")
    proc = start_capture(test_pcap)
    if proc is None:
        print("  [preflight] FAIL — sidecar did not start.")
        return False
    time.sleep(3)
    stop_capture(proc)
    size = os.path.getsize(test_pcap) if os.path.exists(test_pcap) else 0
    # A valid PCAP has at least a 24-byte global header; >100 bytes means real packets.
    ok = size > 24
    print(f"  [preflight] Capture {'OK' if ok else 'FAIL — empty pcap'} ({size} bytes)")
    if os.path.exists(test_pcap):
        os.remove(test_pcap)
    return ok


# ── Prometheus helpers ───────────────────────────────────────────────────────────

def check_prometheus_reachable(prometheus_url: str) -> bool:
    """Return True if the Prometheus API responds. Warn and return False otherwise."""
    try:
        resp = requests.get(f"{prometheus_url}/-/ready", timeout=5)
        if resp.status_code == 200:
            return True
    except Exception:
        pass
    print(f"  [warn] Prometheus not reachable at {prometheus_url}.")
    print(f"         Start the monitoring stack first:")
    print(f"           docker compose -f docker-compose-monitoring.yml up -d")
    print(f"         Prometheus metrics will be missing from the CSV.")
    return False


def query_prometheus(prometheus_url: str, metric_names: list) -> dict:
    """Snapshot the current value of each metric from Prometheus instant query API.

    Returns {metric_name: float_or_None}. Sums across all label combinations.
    """
    values = {}
    for metric in metric_names:
        try:
            resp = requests.get(
                f"{prometheus_url}/api/v1/query",
                params={"query": metric},
                timeout=5,
            )
            data = resp.json()
            if data.get("status") == "success" and data["data"]["result"]:
                values[metric] = sum(
                    float(r["value"][1]) for r in data["data"]["result"]
                )
            else:
                values[metric] = None
        except Exception:
            values[metric] = None
    return values


def query_prometheus_range(prometheus_url: str, metric_names: list,
                           start_ts: float, end_ts: float, step: str = "15s") -> dict:
    """Query Prometheus range API over [start_ts, end_ts] (Unix timestamps).

    Returns {metric_name: data dict (Prometheus 'data' envelope) or None}.
    Scrape interval in prometheus.yml is 15 s, so step='15s' gives one point per scrape.
    """
    results = {}
    for metric in metric_names:
        try:
            resp = requests.get(
                f"{prometheus_url}/api/v1/query_range",
                params={"query": metric, "start": start_ts, "end": end_ts, "step": step},
                timeout=15,
            )
            data = resp.json()
            results[metric] = data["data"] if data.get("status") == "success" else None
        except Exception as e:
            print(f"  [warn] Prometheus range query failed for {metric}: {e}")
            results[metric] = None
    return results


def save_prometheus_range_json(filepath: str, experiment_id: str, broker: str,
                                payload_size: str, qos: int,
                                start_ts: float, end_ts: float, range_data: dict):
    """Write Prometheus range query results to a JSON file."""
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w") as f:
        json.dump({
            "experiment_id": experiment_id,
            "broker":        broker,
            "payload_size":  payload_size,
            "qos":           qos,
            "start":         start_ts,
            "end":           end_ts,
            "metrics":       range_data,
        }, f, indent=2)
    print(f"  Prometheus range data → {filepath}")


def get_clock_offset_ms() -> float:
    """Return estimated host clock offset in ms (positive = fast, negative = slow).

    Tries chronyc, ntpq, then timedatectl (available in WSL2/systemd).
    Returns None if no tool is available.
    Clock offset is logged per-run to bound the systematic bias in end-to-end
    latency measurements caused by publisher/subscriber clock skew.
    """
    try:
        result = subprocess.run(["chronyc", "tracking"],
                                capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            if "System time" in line:
                parts = line.split(":")
                if len(parts) >= 2:
                    tokens = parts[1].strip().split()
                    if tokens:
                        sign = -1.0 if "slow" in line else 1.0
                        return sign * float(tokens[0]) * 1000.0
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        pass
    try:
        result = subprocess.run(["ntpq", "-p"],
                                capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            if line.startswith("*"):
                parts = line.split()
                if len(parts) >= 9:
                    return float(parts[8])
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        pass
    try:
        result = subprocess.run(["timedatectl", "show-timesync", "--property=NTPMessage"],
                                capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            if "offset=" in line:
                # e.g. "offset=+0.123456s" or within NTPMessage=...
                import re
                m = re.search(r'offset=([+-]?[\d.]+)s', line)
                if m:
                    return float(m.group(1)) * 1000.0
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        pass
    return None


def query_nanomq_rest(url: str) -> dict:
    """Query NanoMQ JSON metrics REST endpoint. Returns flat {name: value} dict.

    NanoMQ /api/v4/metrics returns {"code":0,"data":[{"name":"X","value":N},...]}
    """
    try:
        resp = requests.get(url, timeout=5)
        data = resp.json()
        items = data.get("data", [])
        if isinstance(items, list):
            return {
                item["name"].replace(".", "_").replace("/", "_"): item["value"]
                for item in items
                if "name" in item and "value" in item
            }
        if isinstance(items, dict):
            return {k.replace(".", "_"): v for k, v in items.items()}
    except Exception as e:
        print(f"  [warn] NanoMQ metrics query failed: {e}")
    return {}


# ── Docker stats snapshot ────────────────────────────────────────────────────────

def snapshot_docker_stats() -> list:
    """Return per-container resource dicts from docker stats --no-stream."""
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}},{{.CPUPerc}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}"],
            capture_output=True, text=True, timeout=30,
        )
        rows = []
        for line in result.stdout.strip().splitlines():
            parts = line.split(",")
            if len(parts) < 6:
                continue
            name, cpu, mem, net_io, blk_io, pids = parts
            net_parts = net_io.split("/")
            blk_parts = blk_io.split("/")
            rows.append({
                "name":    name.strip(),
                "cpu_pct": cpu.strip().rstrip("%"),
                "mem_pct": mem.strip().rstrip("%"),
                "net_rx":  net_parts[0].strip() if net_parts else "",
                "net_tx":  net_parts[1].strip() if len(net_parts) > 1 else "",
                "blk_r":   blk_parts[0].strip() if blk_parts else "",
                "blk_w":   blk_parts[1].strip() if len(blk_parts) > 1 else "",
                "pids":    pids.strip(),
            })
        return rows
    except Exception as e:
        print(f"  [warn] docker stats snapshot failed: {e}")
        return []


# ── Log directory pre-creation ───────────────────────────────────────────────

def _pre_create_log_dirs(log_path: str):
    """Pre-create host log directories with world-write so Docker bind-mounts succeed.

    When Docker creates a bind-mount host directory that doesn't exist it creates it
    as root, and the container's non-root user can't write to it.  Creating the
    directories here (as the current user) with 0o777 avoids that race.

    If the directory already exists and is owned by root (e.g. from a prior Docker run),
    os.chmod fails — we fall back to sudo chmod.
    """
    dirs = [
        os.path.join("subscriber", "logs", log_path),
        os.path.join("publisher",  "logs", log_path),
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        try:
            os.chmod(d, 0o777)
        except PermissionError:
            # Directory is root-owned from a previous Docker run; use sudo to fix it.
            result = subprocess.run(["sudo", "chmod", "777", d], check=False)
            if result.returncode != 0:
                print(f"  [warn] Cannot fix permissions on {d}.")
                print(f"         Run manually: sudo chmod 777 {d}")
        print(f"  Pre-created log dir: {d}")


# ── CSV output ───────────────────────────────────────────────────────────────────

def append_csv_row(filepath: str, row: dict):
    """Append one row to a CSV. Writes header on first call."""
    is_new = not os.path.isfile(filepath)
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()), extrasaction="ignore")
        if is_new:
            writer.writeheader()
        writer.writerow(row)


# ── Main orchestration ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Multi-broker MQTT benchmark runner with packet capture (AUT0).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--broker", choices=BROKER_CONFIG.keys(), required=True,
                        help="Broker under test")
    parser.add_argument("--payload-size", default=None, metavar="SIZE",
                        help="Synthetic payload, e.g. 1kb, 100kb, 2mb. "
                             "Omit to use real sensor data.")
    parser.add_argument("--qos", type=int, default=0, choices=[0, 1, 2],
                        help="MQTT QoS level (default: 0)")
    parser.add_argument("--numexecs", type=int, default=10,
                        help="Load executions per run (default: 10)")
    parser.add_argument("--stats", default=None, metavar="FILE",
                        help="Output CSV file (default: results/<broker>/<size>/qos<N>/metrics.csv)")
    parser.add_argument("--prometheus-url", default=None, metavar="URL",
                        help="Prometheus URL (default: $PROMETHEUS_URL or http://localhost:9090)")
    parser.add_argument("--experiment-id", default=None, metavar="ID",
                        help="Label written to every CSV row (default: auto-generated)")
    parser.add_argument("--sleep-between", type=int, default=60, metavar="SECS",
                        help="Seconds between load executions (default: 60)")
    parser.add_argument("--run-suffix", default=None, metavar="SUFFIX",
                        help="Append a label to the aut_label and log path, e.g. 'test' "
                             "produces aut0_emqx_1kb_test/qos0. Keeps test runs "
                             "isolated from production data.")
    parser.add_argument("--no-manage-broker", action="store_true",
                        help="Skip broker start/stop (assume already running)")
    args = parser.parse_args()

    broker_cfg = BROKER_CONFIG[args.broker]
    prom_url   = args.prometheus_url or os.getenv("PROMETHEUS_URL", "http://localhost:9090")
    ts_start   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    experiment_id = (
        args.experiment_id
        or f"{args.broker}_{args.payload_size or 'real'}_qos{args.qos}_{ts_start}"
    )

    # ── Output directory: results/<broker>/<size>/qos<N>/ ───────────────────────
    size_label_for_path = args.payload_size or "real"
    run_dir = os.path.join(
        "results", args.broker, size_label_for_path, f"qos{args.qos}"
    )
    stats_file    = args.stats or os.path.join(run_dir, "metrics.csv")
    stats_raw     = os.path.splitext(stats_file)[0] + "_docker_stats.txt"
    captures_dir  = os.path.join(os.path.dirname(stats_file), "captures")

    print(f"\n{'='*62}")
    print(f"  Broker        : {args.broker}  ({broker_cfg['image']})")
    print(f"  Payload size  : {args.payload_size or 'real sensor data'}")
    print(f"  QoS           : {args.qos}")
    print(f"  Executions    : {args.numexecs}")
    print(f"  Experiment ID : {experiment_id}")
    print(f"  Output dir    : {os.path.dirname(stats_file)}/")
    print(f"{'='*62}\n")

    # ── 1. Synthetic payload ─────────────────────────────────────────────────────
    payload_overlay = None
    size_label = None
    if args.payload_size:
        size_label = make_label(parse_size(args.payload_size))
        data_dir = os.path.join(SYNTHETIC_DATA_DIR, size_label)
        # Expect at least gateway1/ with payload_000.json — one gateway per payload.
        first_gateway = os.path.join(data_dir, "gateway1")
        if not os.path.isfile(os.path.join(first_gateway, "payload_000.json")):
            print(f"Generating synthetic payloads for {size_label}...")
            generate([args.payload_size], output_dir=SYNTHETIC_DATA_DIR)
        else:
            print(f"Using existing synthetic payloads: {data_dir}/gateway{{1..N}}/")
        payload_overlay = write_payload_override(size_label)

    # ── 2. Log path (maps to data_analysis directory structure) ─────────────────
    # Structure mirrors existing experiments: subscriber/logs/{aut}/{qos}/subscriber.log
    # The payload size is encoded in the aut label (like aut1_15b, aut1_29b in the paper),
    # so data_analysis.ipynb can load it by adding e.g. 'aut0_emqx_1kb' to the auts list.
    aut_label = f"aut0_{args.broker}" + (f"_{size_label}" if size_label else "")
    if args.run_suffix:
        aut_label += f"_{args.run_suffix}"
    log_path  = f"{aut_label}/qos{args.qos}"

    # Pre-create log directories with correct permissions before any container starts
    # (prevents Docker from creating them as root, which would block container writes).
    _pre_create_log_dirs(log_path)

    # Prometheus data directory must be writable by nobody (uid 65534).
    # Docker creates it as root if it doesn't exist, causing Prometheus to panic.
    prom_data = "monitoring/prometheus_data"
    os.makedirs(prom_data, exist_ok=True)
    try:
        os.chmod(prom_data, 0o777)
    except PermissionError:
        subprocess.run(["sudo", "chmod", "777", prom_data], check=False)

    # ── 3. Compose environment ───────────────────────────────────────────────────
    env = os.environ.copy()
    env.update({
        "BROKER_IMAGE": broker_cfg["image"],
        "BROKER_TYPE":  args.broker,
        "LOG_PATH":     log_path,
        "QOS":          str(args.qos),
    })

    # ── 4. Broker lifecycle ──────────────────────────────────────────────────────
    aut_files = [AUT_BASE_COMPOSE, broker_cfg["overlay"]]
    if not args.no_manage_broker:
        print(f"Starting broker: {args.broker}...")
        _compose_down(aut_files, env)      # clean slate
        _compose_up_detached(aut_files, env)
        print("Waiting 30 s for broker to become ready...")
        time.sleep(30)

    # Verify Prometheus is reachable before committing to a full run.
    if broker_cfg["prom_metrics"]:
        check_prometheus_reachable(prom_url)

    # Pre-flight: verify capture sidecar can write packets before committing to a full run.
    print("Running capture pre-flight check...")
    if not preflight_capture_test(captures_dir):
        print("  [warn] Capture will be skipped for this run.")

    # ── 5. Client compose file list ──────────────────────────────────────────────
    client_files = [CLIENTS_COMPOSE]
    if args.broker in BROKER_CLIENT_OVERRIDES:
        client_files.append(BROKER_CLIENT_OVERRIDES[args.broker])
    if payload_overlay:
        client_files.append(payload_overlay)

    # ── 6. Subscriber (persistent across all executions) ────────────────────────
    print("Starting subscriber...")
    _compose_down([SUBSCRIBER_COMPOSE], env)
    _compose_up_detached([SUBSCRIBER_COMPOSE], env)
    time.sleep(5)   # brief pause — subscriber connects before first publish batch

    # ── 7. PCAP capture (entire experiment duration) ─────────────────────────────
    pcap_path = os.path.join(captures_dir, f"{experiment_id}.pcap")
    capture_proc = start_capture(pcap_path)

    # Record clock offset and experiment start time for range query and skew logging.
    clock_offset_ms = get_clock_offset_ms()
    if clock_offset_ms is not None:
        print(f"  Clock offset (NTP): {clock_offset_ms:+.3f} ms")
    else:
        print("  [warn] Clock offset unavailable (chronyc/ntpq not found).")
    t_experiment_start = time.time()

    # ── 8. Background docker stats (record_stats.sh compatible) ──────────────────
    os.makedirs(os.path.dirname(stats_file) or ".", exist_ok=True)
    stats_proc = subprocess.Popen(
        ["/bin/bash", "record_stats.sh", stats_raw],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    print(f"Docker stats   → {stats_raw}")
    print(f"PCAP capture   → {pcap_path}")

    # ── 9. Execution loop ────────────────────────────────────────────────────────
    try:
        for exec_num in range(args.numexecs):
            print(f"\n[{exec_num + 1}/{args.numexecs}] Running load...")

            t_start = time.time()
            _compose_up(client_files, env)
            elapsed = round(time.time() - t_start, 2)

            # Broker metrics snapshot (Prometheus or REST)
            prom_values: dict = {}
            if broker_cfg["prom_metrics"]:
                prom_values = query_prometheus(prom_url, broker_cfg["prom_metrics"])
            elif broker_cfg["rest_metrics_url"]:
                prom_values = query_nanomq_rest(broker_cfg["rest_metrics_url"])

            # Docker stats snapshot for the broker container
            docker_stats = snapshot_docker_stats()
            broker_stats = next(
                (s for s in docker_stats if s.get("name") == "broker"), {}
            )

            # Build CSV row
            row: dict = {
                "experiment_id":   experiment_id,
                "broker":          args.broker,
                "payload_size":    size_label_for_path,
                "qos":             args.qos,
                "execution":       exec_num,
                "timestamp":       datetime.now(timezone.utc).isoformat(),
                "elapsed_s":       elapsed,
                "clock_offset_ms": clock_offset_ms,
                "pcap_file":       pcap_path,
            }
            for k, v in prom_values.items():
                row[f"prom_{k}"] = v
            for k, v in broker_stats.items():
                if k != "name":
                    row[f"broker_{k}"] = v

            append_csv_row(stats_file, row)

            prom_summary = ", ".join(
                f"{k.split('_')[-1]}={v}" for k, v in prom_values.items() if v is not None
            )
            print(f"  elapsed={elapsed}s  broker_metrics=[{prom_summary}]")

            if exec_num < args.numexecs - 1:
                print(f"  Sleeping {args.sleep_between}s...")
                time.sleep(args.sleep_between)

    except KeyboardInterrupt:
        print("\nInterrupted — running cleanup (Ctrl+C is disabled until done)...")
    finally:
        # Mask SIGINT for the entire cleanup block so a second Ctrl+C doesn't
        # interrupt docker compose down mid-flight and leave containers running.
        # One Ctrl+C is enough: the loop above catches it, then we clean up fully.
        old_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            t_experiment_end = time.time()
            stats_proc.kill()
            stop_capture(capture_proc)
            if capture_proc is not None:
                print(f"\nPacket capture saved  → {pcap_path}")
                print(f"Analyse with network_analysis.ipynb (update PCAP_FILE to point to it).")

            print("Stopping subscriber...")
            _compose_down([SUBSCRIBER_COMPOSE], env)

            if not args.no_manage_broker:
                print(f"Stopping broker: {args.broker}...")
                _compose_down(aut_files, env)

            if payload_overlay and os.path.exists(payload_overlay):
                os.remove(payload_overlay)

            # Save full Prometheus time-series for this experiment window.
            # Broker is stopped by now but Prometheus retains the data.
            if broker_cfg["prom_metrics"]:
                print("Saving Prometheus range data...")
                range_data = query_prometheus_range(
                    prom_url, broker_cfg["prom_metrics"],
                    t_experiment_start, t_experiment_end,
                )
                if any(v is not None for v in range_data.values()):
                    prom_json = os.path.join(run_dir, f"{experiment_id}_prometheus.json")
                    save_prometheus_range_json(
                        prom_json, experiment_id, args.broker,
                        size_label_for_path, args.qos,
                        t_experiment_start, t_experiment_end, range_data,
                    )
        finally:
            signal.signal(signal.SIGINT, old_sigint)

    print(f"\nOutput dir    → {os.path.dirname(stats_file)}/")
    print(f"Results CSV   → {stats_file}")
    print(f"Docker stats  → {stats_raw}")
    print(f"Log path (data_analysis.ipynb auts list): '{log_path}'")


if __name__ == "__main__":
    main()
