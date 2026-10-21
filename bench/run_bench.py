#!/usr/bin/env python3
"""Throughput and memory benchmarks: redis-clone vs real Redis.

Runs the unmodified `redis-benchmark` against both servers with identical
settings, repeats every configuration and reports the median. Also measures
memory per key after loading 1M small keys.

Usage:
    python3 bench/run_bench.py                 # full suite
    python3 bench/run_bench.py --quick         # one repeat, fewer requests
    python3 bench/run_bench.py --memory-only

Coordination with other benchmark jobs on the same machine: every benchmark
step holds the lock directory $BENCH_LOCK (default: ../.bench.lock) (atomic mkdir) and
waits for the 1-minute load average to drop below 4 before starting.

Raw redis-benchmark CSV output, machine details and a summary are written to
bench/results/<timestamp>/.
"""

import argparse
import contextlib
import csv
import io
import json
import os
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCK = Path(os.environ.get("BENCH_LOCK", str(ROOT.parent / ".bench.lock")))
TESTS = "set,get,incr,lpush,lpop,sadd,hset,zadd,lrange_100,mset"
TEST_NAMES = ["SET", "GET", "INCR", "LPUSH", "LPOP", "SADD", "HSET", "ZADD", "LRANGE_100 (first 100 elements)", "MSET (10 keys)"]


def log(msg):
    print(f"[bench {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def loadavg():
    out = subprocess.run(["sysctl", "-n", "vm.loadavg"], capture_output=True, text=True).stdout
    return float(out.strip().strip("{}").split()[0])


MAX_LOAD = 7.5
MAX_WAIT_S = 300
LOADS = []  # (step label, 1-minute load average when the step started)


@contextlib.contextmanager
def bench_lock(max_load=None):
    """Atomic mkdir lock + quiet-machine check, released even on failure.

    Waits at most MAX_WAIT_S for the load average to drop below the threshold
    (the user's own desktop apps keep a baseline load of ~5); after that it
    proceeds anyway, still holding the lock so no other benchmark overlaps.
    """
    max_load = max_load or MAX_LOAD
    while True:
        try:
            LOCK.mkdir()
            break
        except FileExistsError:
            time.sleep(5)
    try:
        start = time.time()
        while loadavg() >= max_load and time.time() - start < MAX_WAIT_S:
            log(f"load average {loadavg():.2f} >= {max_load}; waiting (holding the lock)")
            time.sleep(15)
        yield
    finally:
        with contextlib.suppress(FileNotFoundError):
            LOCK.rmdir()


def step(label):
    la = loadavg()
    LOADS.append((label, la))
    log(f"{label} (load {la:.2f})")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(port, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as s:
                s.sendall(b"PING\r\n")
                if s.recv(16).startswith(b"+PONG"):
                    return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server on {port} did not start")


class ServerProc:
    def __init__(self, kind, extra=()):
        self.kind = kind
        self.port = free_port()
        self.dir = tempfile.mkdtemp(prefix=f"bench-{kind}-", dir=ROOT / "target")
        if kind == "redis":
            cmd = ["redis-server", "--port", str(self.port), "--save", "", "--appendonly", "no",
                   "--dir", self.dir, *extra]
        else:
            cmd = [str(ROOT / "target/release/redis-clone"), "--port", str(self.port), "--save", "",
                   "--dir", self.dir, *extra]
        self.cmd = cmd
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_ready(self.port)

    def info(self, section="memory"):
        out = subprocess.run(["redis-cli", "-p", str(self.port), "INFO", section], capture_output=True, text=True).stdout
        return dict(l.split(":", 1) for l in out.splitlines() if ":" in l and not l.startswith("#"))

    def stop(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()
        shutil.rmtree(self.dir, ignore_errors=True)


def run_benchmark(port, clients, pipeline, requests, raw_dir, label, tests=TESTS):
    """One redis-benchmark process (single-threaded client, its default)."""
    cmd = ["redis-benchmark", "-p", str(port), "-c", str(clients), "-n", str(requests), "-P", str(pipeline),
           "-r", "1000000", "-t", tests, "--csv"]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=1800).stdout
    (raw_dir / f"{label}.csv").write_text(" ".join(cmd) + "\n" + out)
    return parse_csv(out)


def parse_csv(out):
    rows = {}
    for row in csv.reader(io.StringIO(out)):
        if not row or row[0] == "test":
            continue
        try:
            rows[row[0]] = float(row[1])
        except ValueError:
            continue
    return rows


def run_parallel(port, procs, clients, pipeline, requests, raw_dir, label, test):
    """`procs` concurrent redis-benchmark processes running the same single
    test; returns the summed requests/second (the server's aggregate rate)."""
    cmd = ["redis-benchmark", "-p", str(port), "-c", str(clients), "-n", str(requests), "-P", str(pipeline),
           "-r", "1000000", "-t", test, "--csv"]
    ps = [subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True) for _ in range(procs)]
    outs = [p.communicate(timeout=1800)[0] for p in ps]
    (raw_dir / f"{label}.csv").write_text(f"{procs} x " + " ".join(cmd) + "\n" + "\n".join(outs))
    totals = {}
    for o in outs:
        for name, rps in parse_csv(o).items():
            totals[name] = totals.get(name, 0.0) + rps
    return totals


def server_variants(args, with_io_threads):
    v = [("redis", "redis", [])]
    if with_io_threads and args.redis_io_threads:
        v.append((f"redis-io{args.redis_io_threads}", "redis", ["--io-threads", str(args.redis_io_threads)]))
    v.append(("redis-clone", "clone", ["--threads", str(args.server_threads)]))
    return v


def throughput(args, out_dir):
    """Single benchmark client, all tests: the standard redis-benchmark setup."""
    configs = [(50, 1, args.n1), (50, 16, args.n16), (500, 1, args.n1), (500, 16, args.n16)]
    raw = out_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    results = {}
    for rep in range(args.repeats):
        # One lock acquisition (and quiet-machine check) per repetition: our
        # own previous step would otherwise keep the 1-minute load above the
        # threshold between steps.
        with bench_lock():
            for clients, pipeline, requests in configs:
                for label, kind, extra in server_variants(args, False):
                    srv = ServerProc(kind, extra)
                    try:
                        tag = f"{label}_c{clients}_P{pipeline}_rep{rep}"
                        step(tag)
                        rows = run_benchmark(srv.port, clients, pipeline, requests, raw, tag)
                    finally:
                        srv.stop()
                    for name, rps in rows.items():
                        results.setdefault(f"c{clients}_P{pipeline}", {}).setdefault(label, {}).setdefault(name, []).append(rps)
    return finish(results, out_dir / "throughput.json")


def scaling(args, out_dir):
    """`--procs` concurrent benchmark clients per test, to measure what the
    server can do when one client process is no longer the bottleneck."""
    raw = out_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    results = {}
    for rep in range(args.repeats):
        with bench_lock():
            for pipeline, requests in [(1, args.n1), (16, args.n16)]:
                for label, kind, extra in server_variants(args, True):
                    srv = ServerProc(kind, extra)
                    try:
                        step(f"scaling {label} P{pipeline} rep{rep}")
                        for test in TESTS.split(","):
                            tag = f"scale_{label}_{args.procs}x50_P{pipeline}_{test}_rep{rep}"
                            per_proc = requests // args.procs * 2
                            rows = run_parallel(srv.port, args.procs, 50, pipeline, per_proc, raw, tag, test)
                            for name, rps in rows.items():
                                if name.startswith("LPUSH (needed"):
                                    continue
                                cfg = f"{args.procs}x50_P{pipeline}"
                                results.setdefault(cfg, {}).setdefault(label, {}).setdefault(name, []).append(rps)
                    finally:
                        srv.stop()
    return finish(results, out_dir / "scaling.json")


def finish(results, path):
    summary = {}
    for cfg, by_server in results.items():
        summary[cfg] = {s: {t: statistics.median(v) for t, v in tests.items()} for s, tests in by_server.items()}
    path.write_text(json.dumps({"runs": results, "median": summary}, indent=2))
    return summary


def gen_protocol(n, path):
    with open(path, "wb") as f:
        for i in range(n):
            k = f"key:{i:012d}".encode()
            f.write(b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$3\r\nxxx\r\n" % (len(k), k))


def memory(args, out_dir):
    proto = ROOT / "target" / "bench-1m-keys.resp"
    if not proto.exists():
        gen_protocol(args.keys, proto)
    res = {}
    for label, kind, extra in [("redis", "redis", []), ("redis-clone", "clone", ["--threads", str(args.server_threads)])]:
        with bench_lock():
            srv = ServerProc(kind, extra)
            try:
                step(f"memory {label}")
                time.sleep(0.5)
                before = srv.info()
                with open(proto, "rb") as f:
                    subprocess.run(["redis-cli", "-p", str(srv.port), "--pipe"], stdin=f, capture_output=True, check=True)
                time.sleep(1.0)
                after = srv.info()
                keys = int(subprocess.run(["redis-cli", "-p", str(srv.port), "DBSIZE"], capture_output=True, text=True).stdout)
                used = int(after["used_memory"]) - int(before["used_memory"])
                rss = int(after["used_memory_rss"]) - int(before["used_memory_rss"])
                res[label] = {
                    "keys": keys,
                    "used_memory_delta": used,
                    "rss_delta": rss,
                    "used_per_key": used / keys,
                    "rss_per_key": rss / keys,
                    "used_memory_before": int(before["used_memory"]),
                    "used_memory_after": int(after["used_memory"]),
                    "rss_after": int(after["used_memory_rss"]),
                    "allocator": after.get("mem_allocator", "?"),
                }
                log(f"memory {label}: {res[label]}")
            finally:
                srv.stop()
    (out_dir / "memory.json").write_text(json.dumps(res, indent=2))
    return res


def machine():
    def sh(*cmd):
        return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()
    return {
        "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string"),
        "cores": sh("sysctl", "-n", "hw.ncpu"),
        "memory_bytes": sh("sysctl", "-n", "hw.memsize"),
        "os": platform.platform(),
        "redis_server": sh("redis-server", "--version"),
        "redis_benchmark": sh("redis-benchmark", "--version"),
        "rustc": sh(os.path.expanduser("~/.cargo/bin/rustc"), "--version"),
        "redis_clone_commit": "workspace build",
    }


def markdown(summary, mem, meta):
    lines = []
    for cfg in summary:
        servers = list(summary[cfg].keys())
        lines.append(f"\n### {cfg}\n")
        lines.append("| test | " + " | ".join(servers) + " | clone / redis |")
        lines.append("|---|" + "---:|" * (len(servers) + 1))
        for t in TEST_NAMES:
            vals = [summary[cfg][s].get(t) for s in servers]
            if vals[0] is None:
                continue
            ratio = summary[cfg].get("redis-clone", {}).get(t, 0) / summary[cfg]["redis"][t] if summary[cfg]["redis"].get(t) else 0
            lines.append(f"| {t.split(' (')[0]} | " + " | ".join(f"{v:,.0f}" if v else "-" for v in vals) + f" | {ratio:.2f}x |")
    if mem:
        lines.append("\n### Memory, 1M keys (`SET key:%012d xxx`)\n")
        lines.append("| server | used_memory / key | RSS / key |")
        lines.append("|---|---:|---:|")
        for s, m in mem.items():
            lines.append(f"| {s} | {m['used_per_key']:.1f} B | {m['rss_per_key']:.1f} B |")
    return "\n".join(lines)


def main():
    # Make SIGTERM/SIGINT unwind through `finally` so the lock is released.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--n1", type=int, default=300_000, help="requests per test without pipelining")
    ap.add_argument("--n16", type=int, default=2_000_000, help="requests per test with -P 16")
    ap.add_argument("--procs", type=int, default=4, help="concurrent benchmark processes for the scaling runs")
    ap.add_argument("--server-threads", type=int, default=4, help="redis-clone worker threads")
    ap.add_argument("--redis-io-threads", type=int, default=4, help="also run Redis with --io-threads N in the scaling runs (0 = skip)")
    ap.add_argument("--no-scaling", action="store_true")
    ap.add_argument("--keys", type=int, default=1_000_000)
    ap.add_argument("--max-load", type=float, default=7.5, help="wait until the 1-minute load average is below this")
    ap.add_argument("--phases", default="throughput,scaling,memory", help="comma-separated subset to run")
    ap.add_argument("--out", help="results directory (default: bench/results/<timestamp>)")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--memory-only", action="store_true")
    ap.add_argument("--no-memory", action="store_true")
    args = ap.parse_args()
    global MAX_LOAD
    MAX_LOAD = args.max_load
    if args.quick:
        args.repeats, args.n1, args.n16 = 1, 100_000, 1_000_000
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = Path(args.out) if args.out else ROOT / "bench" / "results" / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    phases = set(args.phases.split(","))
    if args.memory_only:
        phases = {"memory"}
    if args.no_scaling:
        phases.discard("scaling")
    if args.no_memory:
        phases.discard("memory")
    meta = machine()
    meta["args"] = vars(args)
    summary = {}
    if "throughput" in phases:
        summary.update(throughput(args, out_dir))
    elif (out_dir / "throughput.json").exists():
        summary.update(json.loads((out_dir / "throughput.json").read_text())["median"])
    if "scaling" in phases:
        summary.update(scaling(args, out_dir))
    mem = memory(args, out_dir) if "memory" in phases else None
    prev = out_dir / "machine.json"
    if prev.exists():
        meta["step_load_averages"] = json.loads(prev.read_text()).get("step_load_averages", [])
    meta["step_load_averages"] = meta.get("step_load_averages", []) + LOADS
    (out_dir / "machine.json").write_text(json.dumps(meta, indent=2))
    md = markdown(summary, mem, meta)
    (out_dir / "summary.md").write_text(md + "\n")
    print(md)
    log(f"results in {out_dir}")


if __name__ == "__main__":
    main()
