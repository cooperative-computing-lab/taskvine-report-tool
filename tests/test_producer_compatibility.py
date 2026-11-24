"""Logs of both producers: upstream cctools master and the vine-graph fork.

Fixtures in tests/fixtures/producer-logs are real logs from scripts/run_examples.py, with host names and paths
replaced.  Synthetic records below cover shapes that a short run rarely produces.
"""

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.vine_parse import data_parser as data_parser_module
from src.vine_parse.data_parser import DataParser
from src.vine_parse.diagnostics import LogCompatibilityError
from src.vine_parse.metric_facts import _signed_deltas, _step_function, _window_bound
from src.vine_parse.sources import SourceRecord, expand_task_descriptions

FIXTURES = Path(__file__).parent / "fixtures" / "producer-logs"
HEAD = "2026/11/23 17:32:03.99 vine_manager[1]vine: "
TZ = HEAD + "manager timezone at startup: local_time=2026-11-23 17:32:03 tz_abbr=EST tz_offset=-0500\n"


def records(text):
    return [SourceRecord(i, raw, raw.decode().strip()) for i, raw in enumerate(text.encode().splitlines(True), 1)]


def texts(text):
    return [r.text for r in expand_task_descriptions(records(text))]


class ExpandTaskDescriptionsTest(unittest.TestCase):
    def test_joined_description_becomes_one_record_per_field(self):
        tx = HEAD + "tx to h (127.0.0.1:5): "
        text = (tx + "task 1\ncmd 8\nrun_nodeneeds_library lib\ncategory default\ncores 1.000\n"
                "infile buffer-a node.pkl 0\nend\n" + HEAD + "Task 1 state change: READY (1) to RUNNING (2)\n")
        self.assertEqual(texts(text), [
            tx + "task 1", tx + "cmd 8", tx + "needs_library lib", tx + "category default", tx + "cores 1.000",
            tx + "infile buffer-a node.pkl 0", tx + "end", HEAD + "Task 1 state change: READY (1) to RUNNING (2)",
        ])

    def test_bodies_are_skipped_by_length_even_when_they_look_like_records(self):
        tx = HEAD + "tx to h (127.0.0.1:5): "
        command = "echo 1\n" + HEAD + "complete 0 0\nend\nx"
        env = "A=end"
        text = (tx + f"task 2\ncmd {len(command.encode())}\n{command}category default\n"
                f"env {len(env)}\n{env}\noutfile temp-b out 0\nend\n")
        out = [t for t in texts(text) if t]
        self.assertEqual(out, [tx + "task 2", tx + f"cmd {len(command.encode())}", tx + "category default",
                               tx + "env 5", tx + "outfile temp-b out 0", tx + "end"])
        # One record per physical line: line accounting is unchanged.
        self.assertEqual(len(list(expand_task_descriptions(records(text)))), len(text.splitlines()))

    def test_upstream_command_record_is_hidden_by_length(self):
        tx = HEAD + "tx to h (127.0.0.1:5): "
        command = "echo complete\nstate change: x"
        text = (tx + "task 3\n" + tx + f"cmd {len(command)}\n" + HEAD + command + "\n" + tx + "cores 2.000\n"
                + tx + "end\n")
        self.assertEqual(texts(text), [tx + "task 3", tx + f"cmd {len(command)}", "", "", tx + "cores 2.000", tx + "end"])

    def test_other_records_pass_through(self):
        text = HEAD + "rx from h (127.0.0.1:5): info a\nnot a record\n"
        self.assertEqual(texts(text), [HEAD + "rx from h (127.0.0.1:5): info a", "not a record"])


class ProducerFixtureTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tempdir.cleanup()

    def parse(self, name):
        run = Path(self.tempdir.name) / name
        shutil.copytree(FIXTURES / name, run)
        parser = DataParser(str(run))
        parser.parse_logs()
        return parser

    def assert_reconciled(self, parser):
        data = parser.diagnostics.data
        self.assertEqual(data["status"], "PASS", data["partial_reasons"])
        self.assertTrue(data["reconciliation"]["available"])
        for name, check in data["reconciliation"]["checks"].items():
            self.assertTrue(check["ok"], (name, check))

    def test_every_fixture_passes_and_reconciles(self):
        for path in sorted(FIXTURES.iterdir()):
            with self.subTest(fixture=path.name):
                self.assert_reconciled(self.parse(path.name))

    def test_function_calls_are_attributed_on_both_producers(self):
        for name in ("upstream-library", "fork-library"):
            with self.subTest(fixture=name):
                parser = self.parse(name)
                calls = [t for t in parser.tasks.values() if t.needs_library]
                libraries = [t for t in parser.tasks.values() if t.is_library_task and t.worker_entry]
                self.assertEqual(len(calls), 3)
                self.assertTrue(all(t.worker_entry and t.cores_requested == 1 for t in calls))
                self.assertEqual(len(libraries), 1)
                self.assertEqual(libraries[0].cores_requested, 0)
                standard = [t for t in parser.tasks.values() if not t.needs_library and not t.is_library_task]
                self.assertEqual([t.task_status_name for t in standard], ["successful"])

    def test_lost_task_descriptions_cannot_pass(self):
        # Without the expander the fork's joined descriptions are lost, as before this adapter existed.
        with mock.patch.object(data_parser_module, "expand_task_descriptions", side_effect=lambda r: r):
            parser = self.parse("fork-library")
        reasons = parser.diagnostics.data["partial_reasons"]
        self.assertEqual(parser.diagnostics.data["status"], "PARTIAL")
        self.assertIn("reconcile_dispatch_cores_mismatch", reasons)

    def test_final_results_come_from_transactions(self):
        for name in ("upstream-cancelled", "fork-cancelled"):
            with self.subTest(fixture=name):
                names = {t.task_status_name for t in self.parse(name).tasks.values() if t.when_retrieved}
                self.assertIn("unsuccessful-cancelled", names)
        statuses = [t.task_status_name for t in self.parse("fork-max-retries").tasks.values() if t.task_id == 2]
        self.assertEqual(statuses[-1], "unsuccessful-max-retries")
        self.assertIn("unsuccessful-worker-disconnected", statuses)

    def test_nonzero_exit_codes_are_successful_results(self):
        parser = self.parse("upstream-nonzero-exit")
        codes = sorted(t.exit_status for t in parser.tasks.values() if t.task_status == 0)
        # "kill -9 $$" ends the shell by a signal, which TaskVine reports as a successful result with exit code 9.
        self.assertEqual(codes, [0, 1, 3, 9, 42, 255])

    def test_vault_reads_and_checkpoints_are_separate_classes(self):
        parser = self.parse("fork-vault-reads")
        summary = parser.diagnostics.data["transfer_summary"]
        self.assertGreater(summary["manager_vault"]["count"], 0)
        self.assertGreater(summary["checkpoint"]["count"], 0)
        self.assertGreater(summary["checkpoint_inline"]["count"], 0)
        checkpoints = parser.manager.checkpoints.values()
        self.assertTrue(all(c.outcome == "complete" and c.source_worker_entry for c in checkpoints))

    def test_simulated_loss_ends_residencies_and_charges_tasks(self):
        parser = self.parse("fork-lose-worker")
        (loss,) = parser.manager.simulated_losses
        self.assertIsNotNone(loss["time_end"])
        self.assertEqual(loss["sequence"], 1)
        reasons = {x.end_reason for f in parser.files.values() for x in f.get_flattened_transfers()}
        self.assertIn("simulated_loss", reasons)

    def test_removed_worker_ends_its_residencies(self):
        parser = self.parse("fork-vault-reads")
        reasons = {x.end_reason for f in parser.files.values() for x in f.get_flattened_transfers()}
        self.assertIn("worker_removed", reasons)


class FailClosedRecordTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.run = Path(self.tempdir.name) / "run"
        (self.run / "vine-logs").mkdir(parents=True)

    def tearDown(self):
        self.tempdir.cleanup()

    def parse(self, *lines):
        connect = HEAD + "tcp: listening on port 9123\n" + HEAD + "worker 127.0.0.1:5 connected\n"
        (self.run / "vine-logs" / "debug").write_text(TZ + connect + "".join(HEAD + line + "\n" for line in lines))
        parser = DataParser(str(self.run))
        parser.parse_logs()
        return parser

    def test_manager_vault_source_is_a_transfer(self):
        parser = self.parse("tx to h (127.0.0.1:5): puturl manager://temp-a temp-a 1 1048576 0755 t1", "manager end")
        transfer = parser.files["temp-a"].indexed_transfers["t1"]
        self.assertEqual((transfer.origin, transfer.source), ("manager_vault", "manager://temp-a"))

    def test_unknown_puturl_source_fails_closed(self):
        with self.assertRaises(LogCompatibilityError):
            self.parse("tx to h (127.0.0.1:5): puturl gopher://x temp-a 1 10 0755 t1")

    def test_checkpoint_lifecycle_and_contract(self):
        parser = self.parse(
            "checkpoint started: temp-a from 127.0.0.1:9 size=5 required=0 urgent=1 connect_back=0 inline=1 "
            "vault_used=0 vault_reserved=5 vault_limit=10",
            "checkpoint queued: temp-b node 3 runtime_us=1 priority=1.0",
            "checkpoint complete: temp-a vault_used=5 vault_reserved=0 memory=1 memory_used=5",
            "round summary: seconds=60.0 rounds=10 round_max_us=5 step=x step_us=1",
            "manager end",
        )
        (checkpoint,) = parser.manager.checkpoints.values()
        self.assertEqual((checkpoint.outcome, checkpoint.inline, checkpoint.urgent, checkpoint.size_bytes),
                         ("complete", True, True, 5))
        self.assertEqual(parser.diagnostics.data["handler_counts"]["fork_info"], 1)
        with self.assertRaises(LogCompatibilityError):
            self.parse("checkpoint complete: temp-z vault_used=5 vault_reserved=0 memory=0 memory_used=0")
        with self.assertRaises(LogCompatibilityError):
            self.parse("checkpoint exploded: temp-z")

    def test_put_notices_are_not_transfers_but_unknown_puts_fail(self):
        parser = self.parse("Failed to put file - /tmp/x (Connection reset by peer)", "manager end")
        self.assertEqual(parser.files, {})
        with self.assertRaises(LogCompatibilityError):
            self.parse("tx to h (127.0.0.1:5): put only-a-name")

    def test_description_fields_are_not_stolen_by_other_families(self):
        parser = self.parse(
            "Task 4 state change: INITIAL (0) to READY (1)",
            "tx to h (127.0.0.1:5): task 4",
            "tx to h (127.0.0.1:5): infile file-a stdout 0",
            "tx to h (127.0.0.1:5): outfile file-b put 0",
            "tx to h (127.0.0.1:5): cores 3.000",
            "tx to h (127.0.0.1:5): end",
            "manager end",
        )
        (task,) = parser.tasks.values()
        self.assertEqual((task.input_files, task.output_files, task.cores_requested), ({"file-a"}, {"file-b"}, 3))

    def test_rejected_connections_are_not_worker_sessions(self):
        parser = self.parse(
            "worker 127.0.0.1:6 connected",
            "worker 127.0.0.1:6 authenticating",
            "worker 127.0.0.1:6 presented the wrong password",
            "worker 127.0.0.1:7 connected",
            "worker h (127.0.0.1:5) opened its probe link from 127.0.0.1:7",
            "worker 127.0.0.1:8 connected",
            "manager end",
        )
        self.assertEqual(sorted((ip, port) for ip, port, _ in parser.workers), [("127.0.0.1", 5), ("127.0.0.1", 8)])
        self.assertEqual(sorted(w.id for w in parser.workers.values()), [1, 2])
        self.assertEqual(parser.manager.retracted_connections, 2)
        self.assertEqual(parser.diagnostics.data["handler_counts"]["worker_rejected"], 2)
        # A rejection of a connection that is not open, or of a session with state, fails the parse.
        with self.assertRaises(LogCompatibilityError):
            self.parse("worker 127.0.0.1:9 presented the wrong password")
        with self.assertRaises(LogCompatibilityError):
            self.parse("rx from h (127.0.0.1:5): info worker-id worker-abc",
                       "worker x (127.0.0.1:1) opened its probe link from 127.0.0.1:5")

    def test_task_output_put_to_another_worker_is_one_residency(self):
        tx, rx = "tx to h (127.0.0.1:%d): ", "rx from h (127.0.0.1:%d): "
        parser = self.parse(
            "worker 127.0.0.1:6 connected",
            "Task 1 state change: INITIAL (0) to READY (1)",
            tx % 5 + "task 1", tx % 5 + "outfile file-a o 0", tx % 5 + "end",
            rx % 5 + "cache-update file-a 1 1 3000000 1795488789 9 1795488789833402 X",
            tx % 6 + "put file-a 1 3000000",
            rx % 6 + "cache-update file-a 1 1 3000000 1795488789 9 1795488789833402 X",
            "manager end",
        )
        transfers = parser.files["file-a"].get_flattened_transfers()
        self.assertEqual(sorted((t.origin, t.dest_worker_entry[1]) for t in transfers),
                         [("manager_put", 6), ("task_output", 5)])
        self.assertTrue(all(t.time_stage_in is not None for t in transfers))

    def test_mini_task_is_a_transfer_ended_by_its_cache_update(self):
        command = "seq 1 9 > made"
        parser = self.parse(
            "tx to h (127.0.0.1:5): mini_task made task-rnd-a 1 0 0755",
            "tx to h (127.0.0.1:5): cmd %d" % len(command),
            command,
            "tx to h (127.0.0.1:5): category default",
            "tx to h (127.0.0.1:5): infile file-meta-x input 0",
            "tx to h (127.0.0.1:5): end",
            "Task 1 state change: INITIAL (0) to READY (1)",
            "tx to h (127.0.0.1:5): task 1",
            "tx to h (127.0.0.1:5): infile task-rnd-a m 0",
            "tx to h (127.0.0.1:5): end",
            "rx from h (127.0.0.1:5): cache-update task-rnd-a 5 1 18 1795488789 9094 1795488789833402 X",
            "manager end",
        )
        (transfer,) = parser.files["task-rnd-a"].get_flattened_transfers()
        self.assertEqual(transfer.origin, "mini_task")
        self.assertIsNotNone(transfer.time_stage_in)
        # The mini task's own input is not an input of task 1.
        self.assertEqual(parser.tasks[(1, 1)].input_files, {"task-rnd-a"})
        with self.assertRaises(LogCompatibilityError):
            self.parse("tx to h (127.0.0.1:5): mini_task made")


class CacheEnvelopeTest(unittest.TestCase):
    def test_same_bucket_events_bound_both_ways(self):
        deltas = {0.0: 80.0, 0.05: 0.0}
        signed = {0.0: [0.0, 80.0], 0.05: [-20.0, 20.0]}
        step, bounds = _step_function(deltas), _signed_deltas(signed)
        self.assertEqual(_window_bound(step, bounds, 0.045, 0.055, "low"), 60.0)
        self.assertEqual(_window_bound(step, bounds, 0.045, 0.055, "high"), 100.0)
        self.assertEqual(_window_bound(step, bounds, 0.06, 0.07, "low"), 80.0)


if __name__ == "__main__":
    unittest.main()
