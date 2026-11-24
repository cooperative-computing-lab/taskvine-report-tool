"""Shared helpers for the example workloads run by scripts/run_examples.py.

Each workload is a plain script. The runner gives it, through the environment:

- VINE_EX_PORT: the Manager port, unique per concurrently running workload.
- VINE_EX_WORKER: the vine_worker binary of the producer being tested.
- VINE_EX_RUN_ROOT: run_info_path for the Manager.
- VINE_EX_TEMPLATE: run_info_template, so logs land in RUN_ROOT/TEMPLATE/vine-logs.
- VINE_EX_PRODUCER: "upstream" or "vine-graph".

A workload records what it knows about its own run with expect(); the runner
compares that ground truth with the parser's output.  Workers are started in
their own session and killed by PID with SIGKILL, never by pattern.
"""

import atexit
import json
import os
import signal
import subprocess
import time

PORT = int(os.environ.get("VINE_EX_PORT", "9700"))
WORKER = os.environ.get("VINE_EX_WORKER", "vine_worker")
RUN_ROOT = os.environ.get("VINE_EX_RUN_ROOT", "/tmp/taskvine-report-examples/manual")
TEMPLATE = os.environ.get("VINE_EX_TEMPLATE", "run")
PRODUCER = os.environ.get("VINE_EX_PRODUCER", "unknown")
RUN_DIR = os.path.join(RUN_ROOT, TEMPLATE)
SCRATCH = os.path.join(RUN_ROOT, TEMPLATE + ".scratch")
EXPECTED_PATH = os.path.join(RUN_ROOT, TEMPLATE + ".expected.json")
PID_PATH = os.path.join(RUN_ROOT, TEMPLATE + ".pids")

os.makedirs(SCRATCH, exist_ok=True)
_workers = []
_expected = {}


def scratch(*names):
    """Return a path in the workload's scratch directory, which the runner deletes."""
    path = os.path.join(SCRATCH, *names)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def start_worker(cores=2, memory=2000, disk=8000, gpus=0, extra=()):
    index = len(_workers)
    workdir = scratch(f"worker-{index}", "")
    command = [
        WORKER, "--cores", str(cores), "--memory", str(memory), "--disk", str(disk),
        "--gpus", str(gpus), "--workdir", workdir, "--timeout", "60", *extra,
        "localhost", str(PORT),
    ]
    # Upstream's Python Manager leaves TCP_LOW_PORT/TCP_HIGH_PORT set to the Manager port, which would make a
    # worker's transfer server fail to bind, so workers do not inherit them.
    env = {k: v for k, v in os.environ.items() if k not in ("TCP_LOW_PORT", "TCP_HIGH_PORT")}
    process = subprocess.Popen(command, start_new_session=True, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _workers.append(process)
    with open(PID_PATH, "a") as handle:
        handle.write(f"{process.pid}\n")
    return process


def kill_worker(process):
    try:
        os.kill(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def kill_all_workers():
    for process in _workers:
        kill_worker(process)


atexit.register(kill_all_workers)


def manager(**kwargs):
    from ndcctools.taskvine import Manager
    return Manager(port=PORT, run_info_path=RUN_ROOT, run_info_template=TEMPLATE, staging_path=scratch("staging"), **kwargs)


def executor(**kwargs):
    """A vine_graph Executor (fork only) that logs into this workload's run directory."""
    from ndcctools.taskvine import vine_graph as vg
    return vg.Executor(port=PORT, progress_bar=False, run_info_path=RUN_ROOT, run_info_template=TEMPLATE,
                       staging_path=scratch("staging"), **kwargs)


def expect(**truth):
    """Record ground truth the workload knows (merged across calls)."""
    _expected.update(truth)
    with open(EXPECTED_PATH, "w") as handle:
        json.dump(_expected, handle, indent=2, sort_keys=True)


_returned = set()


def pump(m, seconds, until=None):
    """Run the Manager for about ``seconds``, remembering every task it returns; stop early when until() holds."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        t = m.wait(1)
        if t:
            _returned.add(t.id)
        if until is not None and until():
            return True
    return False


def wait_all(m, tasks, timeout=50, step=None):
    """Wait until every task in ``tasks`` returned, including returns seen by pump(); return {task id: task}."""
    pending = {t.id for t in tasks} - _returned
    done = {t.id: t for t in tasks if t.id in _returned}
    deadline = time.time() + timeout
    while pending and time.time() < deadline:
        t = m.wait(1)
        if t:
            _returned.add(t.id)
            pending.discard(t.id)
            done[t.id] = t
        if step:
            step()
    if pending:
        raise RuntimeError(f"{len(pending)} tasks did not return in {timeout} s: {sorted(pending)[:10]}")
    return done


def outcome_counts(tasks):
    """Count final outcomes as the transactions log names them (SUCCESS, MAX_RETRIES, ...)."""
    counts = {}
    for t in tasks:
        name = str(t.result).upper().replace(" ", "_")
        counts[name] = counts.get(name, 0) + 1
    return counts


def exit_codes(tasks):
    """Map task id to exit code for tasks that ran to completion."""
    return {str(t.id): t.exit_code for t in tasks if str(t.result).upper() == "SUCCESS"}
