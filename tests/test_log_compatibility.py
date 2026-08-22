import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from src.vine_parse.data_parser import DataParser
from src.vine_parse.diagnostics import (
    LogCompatibilityError,
    ShapeNormalizer,
    inspect_run,
    installed_tool_version,
    normalized_shape,
    write_parse_manifest,
)
from src import __version__
from src.vine_parse.parse import main
from src.vine_parse.sources import LegacyDebugSource, LegacyTransactionSource


TIMEZONE = (
    "2026/08/19 12:00:00.00 vine_manager[100]vine: manager timezone at startup: "
    "local_time=2026-08-19 12:00:00 tz_abbr=EDT tz_offset=-0400\n"
)
LISTENING = "2026/08/19 12:00:01.00 vine_manager[100]vine: tcp: listening on port 9123\n"
MANAGER_END = "2026/08/19 12:00:02.00 vine_manager[100]vine: manager end\n"


class LogCompatibilityTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)

    def tearDown(self):
        self.tempdir.cleanup()

    def make_run(self, name, debug_text):
        run = self.root / name
        logs = run / "vine-logs"
        logs.mkdir(parents=True)
        (logs / "debug").write_text(debug_text, encoding="utf-8")
        return run

    def test_runtime_source_version_is_manifest_authority(self):
        self.assertEqual(installed_tool_version(), __version__)

    def test_shape_memoizer_is_exact_for_overlapping_patterns(self):
        lines = [
            TIMEZONE.rstrip(),
            LISTENING.rstrip(),
            "2026/08/19 12:00:02.00 vine_manager[100]vine: worker 10.2.3.4:9123 connected",
            "2026/08/19 12:00:03.00 vine_manager[999]vine: worker 10.9.8.7:4567 connected",
            "2026/08/19 12:00:02.00 vine_manager[100]vine: puturl workerip://10.2.3.4:9123/a/123 'x 42'",
            "2026/08/19 12:00:02.00 vine_manager[100]vine: host cmswn2215 id abcdef123456 {\"n\": 7}",
            "2026/08/19 12:00:03.00 vine_manager[999]vine: host cmswn2215 id abcdef123456 {\"n\": 8}",
        ]
        memoizer = ShapeNormalizer(maxsize=8)
        self.assertEqual(
            [memoizer.normalize(line) for line in lines],
            [normalized_shape(line) for line in lines],
        )
        self.assertGreater(memoizer.cache_info().hits, 0)

    def test_debug_source_preserves_line_numbers_and_decode_failures(self):
        run = self.make_run("source", TIMEZONE)
        with (run / "vine-logs" / "debug").open("ab") as handle:
            handle.write(b"\xff\xfe\n")
        records = list(LegacyDebugSource(str(run / "vine-logs" / "debug")).records())
        self.assertEqual(records[0].line_number, 1)
        self.assertIsNone(records[0].decode_error)
        self.assertEqual(records[1].line_number, 2)
        self.assertIsNotNone(records[1].decode_error)

    def test_transaction_source_classifies_unversioned_evidence(self):
        run = self.make_run("transactions", TIMEZONE)
        path = run / "vine-logs" / "transactions"
        path.write_text(
            "# time manager_pid TASK ...\n"
            "1770000000000000 42 TASK 7 READY default FIRST_RESOURCES 1 {}\n"
            "broken record\n",
            encoding="utf-8",
        )
        records = list(LegacyTransactionSource(str(path)).records())
        self.assertEqual(records[0].entity, "TASK")
        self.assertIsNone(records[0].error)
        self.assertEqual(records[1].error, "too_few_fields")

    def test_doctor_is_pass_with_required_preflight_evidence(self):
        run = self.make_run("healthy", TIMEZONE + LISTENING + MANAGER_END)
        result = inspect_run(str(run), "test", persist=True)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["source"]["timezone_offsets"], ["-0400"])
        persisted = json.loads((run / "vine-logs" / "parse-diagnostics.json").read_text())
        self.assertEqual(persisted["phase"], "preflight")

    def test_doctor_marks_old_log_without_timezone_partial(self):
        run = self.make_run("old", LISTENING + MANAGER_END)
        result = inspect_run(str(run), "test", persist=False)
        self.assertEqual(result["status"], "PARTIAL")
        self.assertIn("missing_timezone_contract", result["partial_reasons"])

    def test_timezone_is_safely_inferred_from_transaction_start(self):
        run = self.make_run("inferred-timezone", LISTENING + MANAGER_END)
        local_naive = datetime(2026, 8, 19, 12, 0, 1)
        # The debug wall clock is UTC-04:00, so its epoch is four hours later
        # than interpreting the same digits as UTC.
        start_epoch = local_naive.replace(tzinfo=timezone.utc).timestamp() + 4 * 3600
        (run / "vine-logs" / "transactions").write_text(
            "{} 100 MANAGER 100 START 0\n".format(int(start_epoch * 1e6)),
            encoding="utf-8",
        )
        preflight = inspect_run(str(run), "test", persist=False)
        self.assertEqual(preflight["status"], "PASS")
        self.assertEqual(
            preflight["compatibility"]["timezone"]["method"],
            "inferred_from_transactions",
        )
        parser = DataParser(str(run))
        parser.parse_logs()
        self.assertEqual(parser.manager.time_zone_offset_hours, -4)
        self.assertEqual(parser.diagnostics.data["status"], "PASS")

    def test_explicit_timezone_recovers_log_without_companion_evidence(self):
        run = self.make_run("explicit-timezone", LISTENING + MANAGER_END)
        parser = DataParser(str(run), timezone_offset=-5 * 3600)
        parser.parse_logs()
        self.assertEqual(parser.manager.time_zone_offset_hours, -5)
        self.assertEqual(
            parser.diagnostics.data["compatibility"]["timezone"]["method"],
            "explicit_override",
        )

    def test_successful_parse_writes_pass_diagnostics_and_fingerprint(self):
        run = self.make_run("parse-pass", TIMEZONE + LISTENING + MANAGER_END)
        parser = DataParser(str(run))
        parser.parse_logs()
        diagnostics = json.loads((run / "vine-logs" / "parse-diagnostics.json").read_text())
        self.assertEqual(diagnostics["status"], "PASS")
        self.assertEqual(diagnostics["handler_counts"]["listening"], 1)
        self.assertEqual(diagnostics["handler_counts"]["manager_end"], 1)
        self.assertEqual(len(diagnostics["source"]["sha256"]), 64)
        self.assertEqual(len(diagnostics["source"]["format_fingerprint_sha256"]), 64)

    def test_checkpoint_round_trip_uses_the_shared_field_contract(self):
        run = self.make_run("checkpoint", TIMEZONE + LISTENING + MANAGER_END)
        parser = DataParser(str(run))
        parser.tasks = {"task": 1}
        parser.workers = {"worker": 2}
        parser.files = {"file": 3}
        parser.subgraphs = {"subgraph": 4}
        parser.manager.port = 9123
        parser.checkpoint_pkl_files()

        restored = DataParser(str(run))
        restored.load_pkl_files()
        self.assertEqual(restored.tasks, parser.tasks)
        self.assertEqual(restored.workers, parser.workers)
        self.assertEqual(restored.files, parser.files)
        self.assertEqual(restored.subgraphs, parser.subgraphs)
        self.assertEqual(restored.manager.port, parser.manager.port)

    def test_protocol_drift_raises_typed_error_and_persists_failure(self):
        malformed = (
            "2026/08/19 12:00:02.00 vine_manager[100]vine: "
            "Task 9 state change: TOTALLY NEW FORMAT\n"
        )
        run = self.make_run("parse-fail", TIMEZONE + LISTENING + malformed)
        parser = DataParser(str(run))
        with self.assertRaises(LogCompatibilityError) as caught:
            parser.parse_logs()
        self.assertEqual(caught.exception.line_number, 3)
        diagnostics = json.loads((run / "vine-logs" / "parse-diagnostics.json").read_text())
        self.assertEqual(diagnostics["status"], "FAIL")
        self.assertEqual(diagnostics["failed_stage"], "parse")
        self.assertEqual(diagnostics["errors"][-1]["line"], 3)
        self.assertIn("task_state_change", diagnostics["errors"][-1]["handler"])

    def test_cli_returns_nonzero_on_parse_failure(self):
        malformed = (
            "2026/08/19 12:00:02.00 vine_manager[100]vine: "
            "Task 9 state change: BROKEN\n"
        )
        self.make_run("cli-fail", TIMEZONE + LISTENING + malformed)
        with mock.patch("src.vine_parse.parse.check_pip_updates"):
            exit_code = main(
                ["--logs-dir", str(self.root), "--templates", "cli-fail", "--downsampling", "0"]
            )
        self.assertEqual(exit_code, 1)

    def test_manifest_binds_outputs_to_source_hash(self):
        run = self.make_run("manifest", TIMEZONE + LISTENING + MANAGER_END)
        parser = DataParser(str(run))
        parser.parse_logs()
        csv_dir = run / "csv-files"
        csv_dir.mkdir()
        (csv_dir / "sample.csv").write_text("x\n1\n", encoding="utf-8")
        path = write_parse_manifest(str(run), parser.diagnostics.data)
        manifest = json.loads(path.read_text())
        self.assertEqual(manifest["status"], "PASS")
        self.assertEqual(manifest["source"]["sha256"], parser.diagnostics.data["source"]["sha256"])
        self.assertEqual(manifest["outputs"][0]["path"], "sample.csv")
        self.assertEqual(len(manifest["outputs"][0]["sha256"]), 64)

    def test_doctor_cli_uses_distinct_partial_exit_code(self):
        run = self.make_run("doctor-partial", LISTENING)
        self.assertEqual(main(["--doctor", str(run)]), 2)


if __name__ == "__main__":
    unittest.main()
