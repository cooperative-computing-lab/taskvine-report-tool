#!/usr/bin/env python3
"""Correctness and wall-time benchmark for reference and native debug paths."""

import argparse
import contextlib
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import cloudpickle

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.vine_parse.data_parser import DataParser


def parse_once(run, native):
    parser = DataParser(run)
    available = parser.debug_classifier.available
    if not native:
        parser.debug_classifier._native = None
    started = time.perf_counter()
    cpu_started = time.process_time()
    with open(os.devnull, "w") as sink:
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            parser.parse_debug()
    wall_seconds = time.perf_counter() - started
    cpu_seconds = time.process_time() - cpu_started
    shapes = "\n".join(sorted(parser.diagnostics._shapes)).encode("utf-8")
    model = cloudpickle.dumps(
        (parser.manager, parser.tasks, parser.workers, parser.files, parser.subgraphs)
    )
    return {
        "mode": "native" if native else "reference",
        "native_available": available,
        "wall_seconds": round(wall_seconds, 6),
        "cpu_seconds": round(cpu_seconds, 6),
        "format_fingerprint_sha256": hashlib.sha256(shapes).hexdigest(),
        "model_sha256": hashlib.sha256(model).hexdigest(),
        "lines": parser.diagnostics.data["source"]["total_lines"],
        "handled": parser.diagnostics.data["source"]["handled_lines"],
        "unhandled": parser.diagnostics.data["source"]["unhandled_lines"],
        "tasks": len(parser.tasks),
        "workers": len(parser.workers),
        "files": len(parser.files),
    }


def main():
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("run")
    argument_parser.add_argument(
        "--mode", choices=("reference", "native", "both"), default="both"
    )
    argument_parser.add_argument("--expected-fingerprint")
    args = argument_parser.parse_args()
    modes = (False, True) if args.mode == "both" else (args.mode == "native",)
    results = [parse_once(args.run, native) for native in modes]
    fingerprints = {result["format_fingerprint_sha256"] for result in results}
    model_hashes = {result["model_sha256"] for result in results}
    passed = len(fingerprints) == 1 and len(model_hashes) == 1
    if args.expected_fingerprint:
        passed = passed and fingerprints == {args.expected_fingerprint}
    if any(result["mode"] == "native" and not result["native_available"] for result in results):
        passed = False
    print(json.dumps({"schema_version": 2, "passed": passed, "results": results}, indent=2))
    raise SystemExit(not passed)


if __name__ == "__main__":
    main()
