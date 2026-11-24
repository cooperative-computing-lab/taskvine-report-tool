#!/usr/bin/env python3
"""Run the example TaskVine workloads against real builds, parse their logs, and check the result.

For each workload in examples/workloads the runner:

1. starts the workload with the chosen producer's Python bindings and vine_worker (the workload starts and SIGKILLs
   its own workers by PID, in their own session; the runner kills any PID left in its pid file afterwards),
2. runs vine_parse on the run directory with this checkout's parser,
3. compares the parser's model and CSVs with ground truth read independently of the parser, from the transactions
   log, from a few anchored regular expressions over the debug log, and from what the workload itself recorded
   (<template>.expected.json).

Producers:
  upstream    cooperative-computing-lab/cctools master installed under --upstream-prefix
  vine-graph  the fork checkout at --fork-root, used as built (bindings in test_support, worker in taskvine/src)

A workload whose source holds a "# producers: vine-graph" line runs only on that producer and is reported SKIP
for the others.

Example:
  python scripts/run_examples.py --producer both --out /tmp/taskvine-report-examples
"""

import argparse
import glob
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

TOOL_ROOT = Path(__file__).resolve().parent.parent
WORKLOAD_DIR = TOOL_ROOT / "examples" / "workloads"
PRODUCER_RE = re.compile(r"^# producers:\s*(.+)$", re.MULTILINE)
RECOVERY_RE = re.compile(r"Submitted recovery task (\d+) ")
CHECKPOINT_START_RE = re.compile(r"vine: checkpoint started: (\S+) from \S+ size=(\d+) .*\binline=(\d)\b")
CHECKPOINT_END_RE = re.compile(r"vine: checkpoint (complete|failed|cancelled): (\S+) vault_used=")
PUT_RE = re.compile(r"vine: tx to \S+ \(\S+\): put (\S+) \d+ (\d+)$")
PUTURL_RE = re.compile(r"vine: tx to \S+ \(\S+\): puturl(?:_now)? (\S+) ")
LOSE_RE = re.compile(r"vine: tx to \S+ \(\S+\): lose \d+$")
TIMESTAMP_RE = re.compile(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+ ")


def producer_config(args, name):
    if name == "upstream":
        prefix = Path(args.upstream_prefix)
        python_paths = sorted(glob.glob(str(prefix / "lib" / "python3*" / "site-packages")))
        return {
            "pythonpath": ":".join(python_paths),
            "worker": str(prefix / "bin" / "vine_worker"),
            "path_prefix": str(prefix / "bin"),
        }
    root = Path(args.fork_root)
    return {
        "pythonpath": str(root / "test_support" / "python_modules" / "python3"),
        "worker": str(root / "taskvine" / "src" / "worker" / "vine_worker"),
        # resource_monitor for enable_monitoring() comes from the same build.
        "path_prefix": str(root / "taskvine" / "src" / "worker") + ":" + str(root / "resource_monitor" / "src"),
    }


def workload_producers(path):
    match = PRODUCER_RE.search(path.read_text())
    if not match:
        return {"upstream", "vine-graph"}
    return {item.strip() for item in match.group(1).split(",")}


def kill_pids(pid_file):
    if not pid_file.exists():
        return
    for line in pid_file.read_text().split():
        try:
            os.kill(int(line), signal.SIGKILL)
        except (ProcessLookupError, ValueError):
            pass


def run_workload(args, producer, workload, port):
    config = producer_config(args, producer)
    root = Path(args.out) / producer
    root.mkdir(parents=True, exist_ok=True)
    template = workload.stem
    run_dir = root / template
    for stale in (run_dir, root / f"{template}.scratch"):
        if stale.exists():
            shutil.rmtree(str(stale.resolve()))
    for stale in (root / f"{template}.expected.json", root / f"{template}.pids", root / f"{template}.workload.log"):
        if stale.exists():
            stale.unlink()
    env = dict(os.environ)
    env.update({
        "PYTHONPATH": config["pythonpath"] + ":" + str(WORKLOAD_DIR),
        "PATH": config["path_prefix"] + ":" + env.get("PATH", ""),
        "VINE_EX_PORT": str(port),
        "VINE_EX_WORKER": config["worker"],
        "VINE_EX_RUN_ROOT": str(root),
        "VINE_EX_TEMPLATE": template,
        "VINE_EX_PRODUCER": producer,
        "TMPDIR": str(root / f"{template}.scratch"),
        "PYTHONDONTWRITEBYTECODE": "1",
    })
    (root / f"{template}.scratch").mkdir(parents=True, exist_ok=True)
    started = time.time()
    with open(root / f"{template}.workload.log", "w") as log:
        process = subprocess.Popen([args.python, str(workload)], cwd=str(root / f"{template}.scratch"), env=env,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            code = "timeout"
    elapsed = time.time() - started
    kill_pids(root / f"{template}.pids")
    expected = {}
    expected_path = root / f"{template}.expected.json"
    if expected_path.exists():
        expected = json.loads(expected_path.read_text())
    return {"code": code, "elapsed": elapsed, "root": root, "run_dir": run_dir, "expected": expected}


def parse_run(args, run):
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    env["TASKVINE_CHECK_UPDATES"] = "0"
    command = [args.parser_python, "-m", "src.vine_parse.parse", "--logs-dir", str(run["root"]),
               "--templates", run["run_dir"].name, "--checkpoint-pkl-files"]
    with open(run["root"] / f"{run['run_dir'].name}.parse.log", "w") as log:
        return subprocess.call(command, cwd=str(TOOL_ROOT), env=env, stdout=log, stderr=subprocess.STDOUT)


def read_transactions(path):
    """Ground truth from the transactions log, read without the parser's code."""
    truth = {
        "running": defaultdict(list),   # task id -> [(worker id, allocated cores)]
        "done": {},                     # task id -> (status, exit code)
        "ready": set(),
        "libraries_started": 0,
        "library_ids": set(),
        "worker_connections": [],
        "cache_updates": 0,
        "manager_end": False,
    }
    with open(path, errors="replace") as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.split()
            if len(fields) < 5:
                continue
            entity, ident, event = fields[2], fields[3], fields[4]
            if entity == "TASK":
                task_id = int(ident)
                if event == "RUNNING":
                    payload = line[line.index("{"):] if "{" in line else "{}"
                    cores = json.loads(payload).get("cores", [None])[0]
                    truth["running"][task_id].append((fields[5], cores))
                elif event == "DONE":
                    truth["done"][task_id] = (fields[5], int(fields[6]) if len(fields) > 6 else None)
                elif event == "READY":
                    truth["ready"].add(task_id)
            elif entity == "LIBRARY":
                truth["library_ids"].add(int(ident))
                if event == "STARTED":
                    truth["libraries_started"] += 1
            elif entity == "WORKER":
                if event == "CONNECTION":
                    truth["worker_connections"].append(ident)
                elif event == "CACHE_UPDATE":
                    truth["cache_updates"] += 1
            elif entity == "MANAGER" and event == "END":
                truth["manager_end"] = True
    return truth


def read_debug(path):
    """Counts from anchored debug patterns that the checks need and transactions lack."""
    truth = {"recovery_ids": set(), "checkpoints_started": 0, "checkpoints_inline": 0, "checkpoint_ends": Counter(),
             "put_bytes": 0, "lose": 0, "puturl": Counter()}
    with open(path, errors="replace") as handle:
        for line in handle:
            if not TIMESTAMP_RE.match(line):
                continue
            match = RECOVERY_RE.search(line)
            if match:
                truth["recovery_ids"].add(int(match.group(1)))
                continue
            match = CHECKPOINT_START_RE.search(line)
            if match:
                truth["checkpoints_started"] += 1
                truth["checkpoints_inline"] += match.group(3) == "1"
                continue
            match = CHECKPOINT_END_RE.search(line)
            if match:
                truth["checkpoint_ends"][match.group(1)] += 1
                continue
            match = PUT_RE.search(line.rstrip("\n"))
            if match:
                truth["put_bytes"] += int(match.group(2))
                continue
            match = PUTURL_RE.search(line)
            if match:
                source = match.group(1)
                kind = "peer" if source.startswith("workerip://") else "manager_vault" if source.startswith("manager://") else "url"
                truth["puturl"][kind] += 1
                continue
            if LOSE_RE.search(line.rstrip("\n")):
                truth["lose"] += 1
    return truth


def load_model(run_dir):
    sys.path.insert(0, str(TOOL_ROOT))
    import cloudpickle
    model = {}
    for name in ("tasks", "workers", "files", "manager"):
        with open(run_dir / "pkl-files" / f"{name}.pkl", "rb") as handle:
            model[name] = cloudpickle.load(handle)
    return model


def parser_status_name(tx_status):
    if tx_status == "SUCCESS":
        return "successful"
    return "unsuccessful-" + tx_status.lower().replace("_", "-")


def check_run(run):
    checks = []

    def check(name, ok, detail=""):
        checks.append((name, bool(ok), detail))

    expected = run["expected"]
    logs = run["run_dir"] / "vine-logs"
    diagnostics_path = logs / "parse-diagnostics.json"
    if not diagnostics_path.exists():
        check("parsed", False, "no parse-diagnostics.json")
        return checks
    diagnostics = json.loads(diagnostics_path.read_text())
    want_status = expected.get("expect_status", "PASS")
    reasons = diagnostics.get("partial_reasons", [])
    check("status", diagnostics["status"] == want_status,
          f"{diagnostics['status']} (want {want_status}) reasons={reasons} errors={diagnostics.get('errors', [])[:2]}")
    for reason in expected.get("expect_partial_reasons", []):
        check(f"partial_reason:{reason}", reason in reasons, str(reasons))
    if want_status == "PASS":
        check("no_unexpected_partial", not reasons, str(reasons))
    if diagnostics["status"] == "FAIL":
        return checks

    tx = read_transactions(logs / "transactions")
    dbg = read_debug(logs / "debug")
    model = load_model(run["run_dir"])
    tasks, workers = model["tasks"], model["workers"]
    hash_of = {entry: w.hash for entry, w in workers.items()}

    # Dispatches: every RUNNING transaction is one attempt the parser attributes to the same worker.
    tx_dispatch = {tid: sorted(w for w, _ in runs) for tid, runs in tx["running"].items()}
    parser_dispatch = defaultdict(list)
    for (tid, _), task in tasks.items():
        if task.when_running is not None and task.worker_entry is not None:
            parser_dispatch[tid].append(hash_of.get(tuple(task.worker_entry)))
    parser_dispatch = {tid: sorted(v) for tid, v in parser_dispatch.items()}
    tx_total = sum(len(v) for v in tx_dispatch.values())
    parser_total = sum(len(v) for v in parser_dispatch.values())
    check("dispatch_count", tx_total == parser_total, f"transactions={tx_total} parser={parser_total}")
    wrong = [tid for tid in set(tx_dispatch) | set(parser_dispatch) if tx_dispatch.get(tid) != parser_dispatch.get(tid)]
    check("dispatch_attribution", not wrong, f"{len(wrong)} tasks differ, e.g. {sorted(wrong)[:5]}")

    # Final outcomes: the last attempt of every task that the transactions log calls DONE.
    last = {}
    for (tid, try_id), task in tasks.items():
        if tid not in last or try_id > last[tid].task_try_id:
            last[tid] = task
    bad_status, bad_exit = [], []
    for tid, (status, exit_code) in tx["done"].items():
        if tid in tx["library_ids"]:
            # The parser labels a library instance's end itself (library-exit).
            continue
        task = last.get(tid)
        if task is None or task.task_status_name != parser_status_name(status):
            bad_status.append((tid, status, getattr(task, "task_status_name", None)))
        elif status == "SUCCESS" and exit_code is not None and task.exit_status != exit_code:
            bad_exit.append((tid, exit_code, task.exit_status))
    check("final_status", not bad_status, f"{len(bad_status)} differ, e.g. {bad_status[:3]}")
    check("exit_codes", not bad_exit, f"{bad_exit[:3]}")

    libraries = tx["library_ids"]
    recovery = dbg["recovery_ids"]
    user_ids = (tx["ready"] | set(tx["done"])) - libraries - recovery
    if "tasks" in expected and not expected.get("manager_killed"):
        check("task_count", len(user_ids) == expected["tasks"], f"transactions={len(user_ids)} workload={expected['tasks']}")
    if "graph_tasks_min" in expected:
        check("graph_task_count", len(user_ids) >= expected["graph_tasks_min"],
              f"transactions={len(user_ids)} min={expected['graph_tasks_min']}")
    if "outcomes" in expected:
        tx_outcomes = Counter(tx["done"][tid][0] for tid in user_ids if tid in tx["done"])
        check("outcomes", dict(tx_outcomes) == expected["outcomes"], f"transactions={dict(tx_outcomes)} workload={expected['outcomes']}")
    if "exit_codes" in expected:
        wrong = {tid: code for tid, code in expected["exit_codes"].items()
                 if last.get(int(tid)) is None or last[int(tid)].exit_status != code}
        check("workload_exit_codes", not wrong, str(wrong))

    if "workers" in expected:
        check("workers", len(tx["worker_connections"]) == expected["workers"] == len(workers),
              f"transactions={len(tx['worker_connections'])} parser={len(workers)} workload={expected['workers']}")
    else:
        check("workers", len(tx["worker_connections"]) == len(workers),
              f"transactions={len(tx['worker_connections'])} parser={len(workers)}")
    parser_hashes = sorted(str(w.hash) for w in workers.values())
    check("worker_hashes", parser_hashes == sorted(tx["worker_connections"]), f"parser={parser_hashes}")

    library_attempts = sum(1 for (tid, _), t in tasks.items() if tid in libraries and t.worker_entry is not None)
    tx_library_attempts = sum(len(tx["running"][tid]) for tid in libraries)
    check("library_attempts", library_attempts == tx_library_attempts, f"transactions={tx_library_attempts} parser={library_attempts}")
    flagged = {tid for (tid, _), t in tasks.items() if t.is_library_task and t.worker_entry is not None}
    check("library_flags", flagged == {tid for tid in libraries if tx["running"].get(tid)}, f"parser={sorted(flagged)} transactions={sorted(libraries)}")
    if "libraries_min" in expected:
        check("libraries_started", tx["libraries_started"] >= expected["libraries_min"], f"{tx['libraries_started']} >= {expected['libraries_min']}")
    if "function_calls" in expected:
        calls = {tid for (tid, _), t in tasks.items() if t.needs_library}
        check("function_calls", len(calls) == expected["function_calls"], f"parser={len(calls)} workload={expected['function_calls']}")

    parser_recovery = {tid for (tid, _), t in tasks.items() if t.is_recovery_task}
    check("recovery_tasks", parser_recovery == recovery, f"parser={len(parser_recovery)} debug={len(recovery)}")
    if "recovery_min" in expected:
        check("recovery_min", len(recovery) >= expected["recovery_min"], f"{len(recovery)} >= {expected['recovery_min']}")

    # Cores: the parser's request equals what TaskVine allocated, for standard tasks.
    bad_cores = []
    for (tid, try_id), task in tasks.items():
        if task.worker_entry is None or task.when_running is None or task.is_library_task or task.needs_library:
            continue
        allocated = [c for w, c in tx["running"].get(tid, [])]
        if allocated and task.cores_requested not in allocated:
            bad_cores.append((tid, task.cores_requested, allocated))
    check("cores_vs_allocation", not bad_cores, f"{bad_cores[:3]}")
    for tid, cores in expected.get("cores", {}).items():
        got = {t.cores_requested for (i, _), t in tasks.items() if i == int(tid) and t.worker_entry is not None}
        check(f"cores:{tid}", got == {cores}, f"parser={got} workload={cores}")
    for tid, minimum in expected.get("inputs_min", {}).items():
        got = max((len(t.input_files) for (i, _), t in tasks.items() if i == int(tid)), default=0)
        check(f"inputs:{tid}", got >= minimum, f"parser={got} min={minimum}")
    for tid, minimum in expected.get("outputs_min", {}).items():
        got = max((len(t.output_files) for (i, _), t in tasks.items() if i == int(tid)), default=0)
        check(f"outputs:{tid}", got >= minimum, f"parser={got} min={minimum}")

    if "worker_loss_attempts_min" in expected:
        lost = sum(1 for t in tasks.values() if t.task_status_name == "unsuccessful-worker-disconnected")
        check("worker_loss_attempts", lost >= expected["worker_loss_attempts_min"], f"{lost} >= {expected['worker_loss_attempts_min']}")

    # Files and bytes.
    files = model["files"]
    stage_ins = sum(1 for f in files.values() for x in f.get_flattened_transfers() if x.time_stage_in is not None)
    check("cache_updates", stage_ins == tx["cache_updates"], f"parser stage-ins={stage_ins} transactions CACHE_UPDATE={tx['cache_updates']}")
    # Every puturl is one indexed transfer of the parser, by source class.
    parsed_puturl = Counter(getattr(x, "origin", None) for f in files.values() for x in f.indexed_transfers.values())
    for kind in ("url", "peer", "manager_vault"):
        check(f"puturl_{kind}", parsed_puturl.get(kind, 0) == dbg["puturl"].get(kind, 0),
              f"parser={parsed_puturl.get(kind, 0)} debug={dbg['puturl'].get(kind, 0)}")
    if "manager_vault_transfers_min" in expected:
        check("manager_vault_min", dbg["puturl"]["manager_vault"] >= expected["manager_vault_transfers_min"], str(dbg["puturl"]["manager_vault"]))
    if "manager_put_bytes_min" in expected:
        check("manager_put_bytes", dbg["put_bytes"] >= expected["manager_put_bytes_min"], f"{dbg['put_bytes']}")
    checkpoints = getattr(model["manager"], "checkpoints", None)
    if dbg["checkpoints_started"] or "checkpoints_min" in expected:
        got = len(checkpoints) if checkpoints is not None else None
        check("checkpoints", got == dbg["checkpoints_started"], f"parser={got} debug={dbg['checkpoints_started']}")
        if checkpoints is not None:
            inline = sum(1 for c in checkpoints.values() if c.inline)
            check("inline_checkpoints", inline == dbg["checkpoints_inline"], f"parser={inline} debug={dbg['checkpoints_inline']}")
            ends = Counter(c.outcome for c in checkpoints.values() if c.outcome)
            check("checkpoint_outcomes", dict(ends) == dict(dbg["checkpoint_ends"]), f"parser={dict(ends)} debug={dict(dbg['checkpoint_ends'])}")
    if "checkpoints_min" in expected:
        check("checkpoints_min", dbg["checkpoints_started"] >= expected["checkpoints_min"], str(dbg["checkpoints_started"]))
    if "inline_checkpoints_min" in expected:
        check("inline_checkpoints_min", dbg["checkpoints_inline"] >= expected["inline_checkpoints_min"], str(dbg["checkpoints_inline"]))
    if "simulated_losses" in expected or dbg["lose"]:
        got = getattr(model["manager"], "simulated_losses", None)
        check("simulated_losses", got is not None and len(got) == dbg["lose"], f"parser={None if got is None else len(got)} debug={dbg['lose']}")
        if "simulated_losses" in expected:
            check("simulated_losses_min", dbg["lose"] >= expected["simulated_losses"], str(dbg["lose"]))

    # Projection: the task CSV holds one row per dispatched attempt.
    csv_path = run["run_dir"] / "csv-files" / "task_execution_details.csv"
    if csv_path.exists():
        import csv
        with open(csv_path) as handle:
            rows = [r for r in csv.DictReader(handle) if r["record_type"] != "worker"]
        dispatched = [r for r in rows if r.get("when_running")]
        # Library instances are not task rows in this projection.
        tx_tasks = sum(len(v) for tid, v in tx["running"].items() if tid not in libraries)
        check("csv_task_rows", len(dispatched) == tx_tasks, f"csv dispatched rows={len(dispatched)} transactions={tx_tasks}")
    return checks


def cleanup_run(run):
    """Keep logs, expectations, and parser outputs; delete worker sandboxes, caches, and staging data."""
    scratch = run["root"] / f"{run['run_dir'].name}.scratch"
    if scratch.exists():
        shutil.rmtree(str(scratch.resolve()), ignore_errors=True)
    if run["run_dir"].exists():
        for child in run["run_dir"].iterdir():
            if child.name not in {"vine-logs", "csv-files", "json-files", "pkl-files"}:
                if child.is_dir():
                    shutil.rmtree(str(child.resolve()), ignore_errors=True)
                else:
                    child.unlink()


def one(args, producer, workload, port):
    if producer not in workload_producers(workload):
        return {"producer": producer, "workload": workload.stem, "result": "SKIP", "checks": [], "elapsed": 0}
    run = run_workload(args, producer, workload, port)
    checks = []
    if run["code"] not in (0, -9) and not (run["code"] == -signal.SIGKILL and run["expected"].get("manager_killed")):
        checks.append(("workload_exit", False, f"exit {run['code']}, see {run['root'] / (workload.stem + '.workload.log')}"))
    if not (run["run_dir"] / "vine-logs" / "debug").exists():
        checks.append(("logs", False, "no debug log"))
    else:
        parse_code = parse_run(args, run)
        checks.append(("parser_exit", parse_code in (0, 2), f"vine_parse exit {parse_code}"))
        try:
            checks += check_run(run)
        except Exception as exc:
            import traceback
            checks.append(("checker", False, f"{type(exc).__name__}: {exc} at {traceback.format_exc().splitlines()[-3].strip()}"))
    if not args.keep_data:
        cleanup_run(run)
    if not args.keep_pkl and (run["run_dir"] / "pkl-files").exists():
        shutil.rmtree(str((run["run_dir"] / "pkl-files").resolve()), ignore_errors=True)
    result = "PASS" if all(ok for _, ok, _ in checks) else "FAIL"
    return {"producer": producer, "workload": workload.stem, "result": result, "checks": checks,
            "elapsed": run["elapsed"], "status": run["expected"].get("expect_status", "PASS")}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--producer", choices=["upstream", "vine-graph", "both"], default="both")
    parser.add_argument("--out", default="/tmp/taskvine-report-examples", help="output root (default: %(default)s)")
    parser.add_argument("--only", nargs="*", help="workload names to run (default: all)")
    parser.add_argument("--upstream-prefix", default=os.environ.get("VINE_EX_UPSTREAM_PREFIX"),
                        help="install prefix of upstream cctools (or VINE_EX_UPSTREAM_PREFIX)")
    parser.add_argument("--fork-root", default=os.environ.get("VINE_EX_FORK_ROOT"),
                        help="checkout of the vine-graph fork (or VINE_EX_FORK_ROOT)")
    parser.add_argument("--python", default=sys.executable, help="interpreter for workloads (needs cloudpickle)")
    parser.add_argument("--parser-python", default=sys.executable, help="interpreter for vine_parse")
    parser.add_argument("--port-base", type=int, default=9700)
    parser.add_argument("--jobs", type=int, default=1, help="workloads run at once (each starts up to 3 workers)")
    parser.add_argument("--timeout", type=int, default=120, help="seconds before a workload is killed")
    parser.add_argument("--keep-data", action="store_true", help="keep worker sandboxes and caches")
    parser.add_argument("--keep-pkl", action="store_true", help="keep the parser's pkl checkpoints")
    args = parser.parse_args(argv)
    if args.producer in ("upstream", "both") and not args.upstream_prefix:
        parser.error("--upstream-prefix (or VINE_EX_UPSTREAM_PREFIX) is required for the upstream producer")
    if args.producer in ("vine-graph", "both") and not args.fork_root:
        parser.error("--fork-root (or VINE_EX_FORK_ROOT) is required for the vine-graph producer")

    producers = ["upstream", "vine-graph"] if args.producer == "both" else [args.producer]
    workloads = sorted(p for p in WORKLOAD_DIR.glob("*.py") if not p.name.startswith("_"))
    if args.only:
        workloads = [p for p in workloads if p.stem in set(args.only)]
    jobs = [(producer, workload) for producer in producers for workload in workloads]
    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        futures = [pool.submit(one, args, producer, workload, args.port_base + (i % 100))
                   for i, (producer, workload) in enumerate(jobs)]
        for future in futures:
            result = future.result()
            results.append(result)
            failed = [c for c in result["checks"] if not c[1]]
            print(f"{result['result']:4} {result['producer']:10} {result['workload']:24} {result['elapsed']:5.1f}s"
                  + ("" if not failed else "  " + "; ".join(f"{n}: {d}" for n, _, d in failed)), flush=True)
    if not args.keep_data:
        # Managers keep generated library code under run_info_path/vine-cache, shared by the workloads of a producer.
        for producer in producers:
            shared = Path(args.out) / producer / "vine-cache"
            if shared.exists():
                shutil.rmtree(str(shared.resolve()), ignore_errors=True)
    summary = Path(args.out) / "summary.json"
    summary.parent.mkdir(parents=True, exist_ok=True)
    summary.write_text(json.dumps(results, indent=2, default=str))
    counts = Counter(r["result"] for r in results)
    print(f"\n{dict(counts)}; summary in {summary}")
    return 0 if counts.get("FAIL", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
