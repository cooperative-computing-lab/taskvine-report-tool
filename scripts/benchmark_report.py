#!/usr/bin/env python3
"""Concurrent identity and latency benchmark for a running report server."""

import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.parse
import urllib.request


def get_json(url, timeout):
    started = time.monotonic()
    with urllib.request.urlopen(url, timeout=timeout) as response:
        payload = json.load(response)
        headers = dict(response.headers)
        status = response.status
    return status, payload, headers, time.monotonic() - started


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:9122")
    parser.add_argument("--run", action="append", dest="runs")
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    if args.clients < 1 or args.requests < 1:
        parser.error("clients and requests must be positive")

    _, catalog, _, _ = get_json(f"{base}/api/v1/runs", args.timeout)
    available = catalog["data"]["runs"]
    runs = args.runs or available[:2]
    if not runs:
        parser.error("the server exposes no parsed runs")
    missing = sorted(set(runs) - set(available))
    if missing:
        parser.error(f"run not found: {missing[0]}")

    revisions = {}
    for run in runs:
        encoded = urllib.parse.quote(run, safe="")
        _, payload, _, _ = get_json(f"{base}/api/v1/runs/{encoded}", args.timeout)
        revisions[run] = payload["revision"]

    resources = (
        "summary",
        "charts/task-concurrency",
        "charts/worker-concurrency",
        "charts/file-sizes",
    )
    workload = []
    for index in range(args.requests):
        run = runs[index % len(runs)]
        revision = revisions[run]
        resource = resources[index % len(resources)]
        encoded = urllib.parse.quote(run, safe="")
        workload.append((
            run,
            revision,
            f"{base}/api/v1/runs/{encoded}/revisions/{revision}/{resource}",
        ))

    def execute(item):
        run, revision, url = item
        try:
            status, payload, headers, latency = get_json(url, args.timeout)
            valid = (
                status == 200
                and payload.get("run_id") == run
                and payload.get("revision") == revision
                and headers.get("X-TaskVine-Run") == run
                and headers.get("X-TaskVine-Revision") == revision
            )
            return valid, latency, None if valid else "identity_mismatch"
        except Exception as error:
            return False, None, f"{type(error).__name__}: {error}"

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        results = list(pool.map(execute, workload))
    elapsed = time.monotonic() - started
    latencies = [latency for _, latency, _ in results if latency is not None]
    failures = [error for valid, _, error in results if not valid]
    output = {
        "schema_version": 1,
        "base_url": base,
        "runs": revisions,
        "clients": args.clients,
        "requests": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "wall_seconds": round(elapsed, 3),
        "throughput_requests_per_second": round(len(results) / elapsed, 3),
        "latency_seconds": {
            "p50": round(statistics.median(latencies), 4) if latencies else None,
            "p95": round(percentile(latencies, 0.95), 4) if latencies else None,
            "max": round(max(latencies), 4) if latencies else None,
        },
        "first_failures": failures[:5],
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    raise SystemExit(bool(failures))


if __name__ == "__main__":
    main()
