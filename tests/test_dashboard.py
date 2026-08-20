import csv
import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from flask import jsonify

from src.vine_report.report import create_app
from src.vine_report.request_context import get_run_snapshot
from src.vine_report.run_store import CatalogLease, RunStoreError


class DashboardAPITest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self._make_run("run-a", successful=8, unsuccessful=2, duration=20, scale=1)
        self._make_run("run-b", successful=9, unsuccessful=1, duration=10, scale=2)
        self.app = create_app(str(self.root))
        self.app.testing = True
        self.client = self.app.test_client()

    def tearDown(self):
        self.tempdir.cleanup()

    @staticmethod
    def _write(path, header, rows):
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)

    def _make_run(self, name, successful, unsuccessful, duration, scale):
        csv_dir = self.root / name / "csv-files"
        csv_dir.mkdir(parents=True)
        metadata = [
            ("total_workers", 4 * scale), ("total_tasks", successful + unsuccessful),
            ("total_all_tasks", successful + unsuccessful + 1), ("total_library_tasks", 1),
            ("successful_tasks", successful), ("unsuccessful_tasks", unsuccessful),
            ("recovery_tasks", 1), ("recovery_successful", 1),
            ("recovery_unsuccessful", 0), ("total_files", 6 * scale),
            ("manager_start_time", 1000), ("manager_end_time", 1000 + duration),
            ("manager_duration", duration),
        ]
        self._write(csv_dir / "metadata.csv", ["key", "value"], metadata)
        self._write(csv_dir / "task_execution_time.csv", ["Execution Time"], [[1], [2], [20 * scale]])
        self._write(csv_dir / "task_response_time.csv", ["Response Time"], [[2], [4], [6]])
        self._write(csv_dir / "worker_concurrency.csv", ["time", "Active Workers (count)"], [[0, 1], [1, 4 * scale]])
        self._write(csv_dir / "worker_lifetime.csv", ["LifeTime (s)"], [[10], [20]])
        self._write(csv_dir / "file_transferred_size.csv", ["time", "cumulative_size_mb"], [[0, 0], [1, 100 * scale]])
        self._write(csv_dir / "file_created_size.csv", ["time", "cumulative_size_mb"], [[0, 0], [1, 50 * scale]])
        self._write(csv_dir / "worker_storage_consumption.csv", ["time", "workflow_completion_percentage", "w1", "w2"], [[0, 0, 1, 2], [1, 100, 10 * scale, 20 * scale]])
        self._write(csv_dir / "time_domain.csv", ["MIN_TIME", "MAX_TIME"], [[0, duration]])
        task_header = [
            "record_type", "task_id", "task_try_id", "worker_entry", "cores_requested",
            "is_recovery_task", "task_status", "category", "execution_time", "when_ready",
            "when_running", "when_done", "input_files", "output_files",
        ]
        task_rows = [
            ["successful_tasks", 1, 1, "worker-a", 2, "False", 0, "alpha", 8, 0, 1, 9, "in-a", "out-a"],
            ["unsuccessful_tasks", 2, 1, "worker-b", 1, "True", 40, "beta", 3, 0, 1, 4, "in-b", "out-b"],
            ["worker", "", "", "worker-a", "", "", "", "", "", "", "", "", "", ""],
        ]
        self._write(csv_dir / "task_execution_details.csv", task_header, task_rows)

    def test_runs_and_index_product_shell(self):
        response = self.client.get("/api/dashboard/runs")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["runs"], ["run-a", "run-b"])
        self.assertEqual(response.headers["X-TaskVine-API-Version"], "1")
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        page = self.client.get("/").get_data(as_text=True)
        self.assertIn("TaskVine Report", page)
        self.assertIn("Task Explorer", page)
        self.assertIn("Compare runs", page)
        self.assertIn("css/app.css') }}?v=5.1", Path("src/vine_report/templates/index.html").read_text())

    def test_v1_contract_matches_registered_routes(self):
        contract = json.loads(Path("docs/api-v1-contract.json").read_text(encoding="utf-8"))
        documented = {
            (item["method"], item["path"].split("?", 1)[0])
            for item in contract["endpoints"]
        }
        registered = {
            (method, rule.rule)
            for rule in self.app.url_map.iter_rules()
            if rule.rule.startswith("/api/v1/")
            for method in rule.methods - {"HEAD", "OPTIONS"}
        }
        self.assertEqual(contract["schema_version"], "report-api/v1")
        self.assertEqual(documented, registered)

    def test_health_and_diagnostics_are_stable_and_bounded(self):
        health = self.client.get("/api/v1/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json["schema_version"], "report-api/v1")
        self.assertEqual(health.json["data"]["status"], "ok")
        self.assertEqual(health.json["data"]["run_count"], 2)
        self.assertIn("source-backed-metrics-v1", health.json["data"]["capabilities"])
        self.assertEqual(health.json["data"]["runtime"]["request_threads"], 8)

        self.client.get("/api/v1/runs/run-a")
        diagnostics = self.client.get("/api/v1/diagnostics").json["data"]
        self.assertEqual(diagnostics["service"]["api_schema"], "report-api/v1")
        by_run = {item["run_id"]: item for item in diagnostics["runs"]}
        self.assertEqual(set(by_run), {"run-a", "run-b"})
        self.assertEqual(len(by_run["run-a"]["current_revision"]), 64)
        self.assertGreaterEqual(by_run["run-a"]["snapshot_count"], 1)
        self.assertNotIn("path", by_run["run-a"])

    def test_visual_tokens_keep_text_legible_and_hover_orange(self):
        css_response = self.client.get("/static/css/app.css")
        base_response = self.client.get("/static/js/modules/base.js")
        css = css_response.get_data(as_text=True)
        base_js = base_response.get_data(as_text=True)
        css_response.close()
        base_response.close()
        self.assertIn("--text-sm: 14px", css)
        self.assertIn("--text-md: 15px", css)
        self.assertIn("--accent-orange: #d96f24", css)
        self.assertIn("background: var(--accent-orange-soft)", css)
        self.assertIn("this.highlightColor = '#d96f24'", base_js)
        self.assertIn("this.tickFontSize = '14px'", base_js)

    def test_frontend_discards_stale_run_responses(self):
        main_js = Path("src/vine_report/static/js/main.js").read_text(encoding="utf-8")
        base_js = Path("src/vine_report/static/js/modules/base.js").read_text(encoding="utf-8")
        self.assertIn("new AbortController()", main_js)
        self.assertIn("Stale run response discarded", main_js)
        self.assertIn("payload.run_id !== scope.run", main_js)
        self.assertIn("requestGeneration !== this._runGeneration", base_js)
        self.assertIn("/api/v1/runs/", base_js)

    def test_summary_metrics_and_findings(self):
        response = self.client.get("/api/dashboard/summary", query_string={"folder": "run-a"})
        self.assertEqual(response.status_code, 200)
        data = response.json
        self.assertEqual(data["total_tasks"], 10)
        self.assertEqual(data["success_rate"], 0.8)
        self.assertEqual(data["peak_active_workers"], 4)
        self.assertEqual(data["transferred_mb"], 100)
        self.assertEqual(data["peak_storage_mb"], 30)
        self.assertTrue(data["findings"])
        self.assertIn("evidence", data["findings"][0])
        self.assertIn("observed", data["findings"][0]["evidence"])
        self.assertEqual(data["data_quality"]["status"], "UNKNOWN")

    def test_summary_cache_invalidates_when_source_csv_changes(self):
        first = self.client.get("/api/dashboard/summary", query_string={"folder": "run-a"}).json
        self.assertEqual(first["total_tasks"], 10)
        metadata = self.root / "run-a" / "csv-files" / "metadata.csv"
        time.sleep(0.01)
        rows = list(csv.reader(metadata.open(encoding="utf-8")))
        rows = [[key, "11"] if key == "total_tasks" else [key, value] for key, value in rows]
        self._write(metadata, rows[0], rows[1:])
        second = self.client.get("/api/dashboard/summary", query_string={"folder": "run-a"}).json
        self.assertEqual(second["total_tasks"], 11)

    def test_summary_exposes_parse_quality_without_raw_errors(self):
        logs_dir = self.root / "run-a" / "vine-logs"
        logs_dir.mkdir()
        (logs_dir / "parse-diagnostics.json").write_text(json.dumps({
            "status": "PARTIAL",
            "parser_contract_version": 1,
            "partial_reasons": ["manager_end_missing_or_log_truncated"],
            "source": {
                "format": "taskvine.debug.unversioned",
                "adapter": "taskvine-debug-legacy-v1",
                "sha256": "abc",
                "handled_lines": 10,
                "unhandled_lines": 2,
                "protocol_candidate_unhandled_lines": 0,
            },
            "compatibility": {"timezone": {"method": "debug_metadata", "offset_seconds": 0}},
            "errors": [{"message": "must not be exposed by summary"}],
        }), encoding="utf-8")
        data = self.client.get("/api/dashboard/summary?folder=run-a").json
        self.assertEqual(data["data_quality"]["status"], "PARTIAL")
        self.assertEqual(
            data["data_quality"]["reasons"],
            ["manager_end_missing_or_log_truncated"],
        )
        self.assertNotIn("errors", data["data_quality"])

    def test_comparison_uses_first_run_as_baseline(self):
        response = self.client.get("/api/dashboard/compare", query_string={"folders": "run-a,run-b"})
        self.assertEqual(response.status_code, 200)
        data = response.json
        self.assertEqual(data["baseline"], "run-a")
        self.assertEqual(len(data["runs"]), 2)
        self.assertAlmostEqual(data["runs"][1]["delta_from_baseline"]["duration_seconds"], -0.5)

    def test_task_search_and_status_filter(self):
        by_category = self.client.get("/api/dashboard/tasks", query_string={"folder": "run-a", "query": "alpha"})
        self.assertEqual(by_category.status_code, 200)
        self.assertEqual(by_category.json["matched"], 1)
        self.assertEqual(by_category.json["tasks"][0]["task_id"], 1)
        failures = self.client.get("/api/dashboard/tasks", query_string={"folder": "run-a", "status": "unsuccessful"})
        self.assertEqual(failures.json["matched"], 1)
        self.assertEqual(failures.json["tasks"][0]["status"], "unsuccessful")
        page = self.client.get(
            "/api/dashboard/tasks",
            query_string={"folder": "run-a", "limit": 1, "offset": 1},
        )
        self.assertEqual(page.json["offset"], 1)
        self.assertFalse(page.json["has_more"])

    def test_invalid_and_incomplete_requests_fail_closed(self):
        self.assertEqual(self.client.get("/api/dashboard/summary?folder=../run-a").status_code, 400)
        missing = self.client.get("/api/dashboard/summary?folder=missing")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json["error"]["code"], "run_not_found")
        self.assertEqual(self.client.get("/api/dashboard/compare?folders=run-a").status_code, 400)
        self.assertEqual(
            self.client.get("/api/dashboard/tasks?folder=run-a&status=maybe").status_code,
            400,
        )
        self.assertEqual(
            self.client.get("/api/dashboard/tasks?folder=run-a&limit=nope").status_code,
            400,
        )

    def test_removed_global_runtime_endpoints_are_unavailable(self):
        self.assertEqual(self.client.get("/api/change-runtime-template").status_code, 404)
        self.assertEqual(self.client.post("/api/lock").status_code, 404)

    def test_concurrent_requests_use_independent_snapshots(self):
        @self.app.get("/api/test-runtime-consistency")
        def runtime_consistency():
            snapshot = get_run_snapshot()
            time.sleep(0.02)
            return jsonify({"run": snapshot.run_id, "revision": snapshot.revision})

        def request_run(folder):
            with self.app.test_client() as client:
                return client.get(
                    "/api/test-runtime-consistency", query_string={"folder": folder}
                ).json

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(request_run, ["run-a", "run-b"]))
        self.assertEqual([item["run"] for item in results], ["run-a", "run-b"])
        self.assertTrue(all(len(item["revision"]) == 64 for item in results))

    def test_v1_pins_identity_and_keeps_old_revision_readable(self):
        descriptor = self.client.get("/api/v1/runs/run-a")
        self.assertEqual(descriptor.status_code, 200)
        revision = descriptor.json["data"]["current_revision"]
        summary_url = f"/api/v1/runs/run-a/revisions/{revision}/summary"
        first = self.client.get(summary_url)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json["run_id"], "run-a")
        self.assertEqual(first.json["revision"], revision)
        self.assertEqual(first.json["data"]["total_tasks"], 10)

        metadata = self.root / "run-a" / "csv-files" / "metadata.csv"
        text = metadata.read_text(encoding="utf-8").replace("total_tasks,10", "total_tasks,11")
        metadata.write_text(text, encoding="utf-8")
        refreshed = self.client.post("/api/v1/runs/run-a/refresh")
        new_revision = refreshed.json["data"]["current_revision"]
        self.assertNotEqual(new_revision, revision)
        old = self.client.get(summary_url)
        self.assertEqual(old.json["data"]["total_tasks"], 10)

    def test_v1_export_is_revision_and_run_scoped(self):
        revisions = {}
        for run in ("run-a", "run-b"):
            descriptor = self.client.get(f"/api/v1/runs/{run}").json
            revisions[run] = descriptor["data"]["current_revision"]
        a = self.client.get(
            f"/api/v1/runs/run-a/revisions/{revisions['run-a']}/exports/task-execution-time.csv"
        )
        b = self.client.get(
            f"/api/v1/runs/run-b/revisions/{revisions['run-b']}/exports/task-execution-time.csv"
        )
        self.assertEqual(a.status_code, 200)
        self.assertEqual(b.status_code, 200)
        self.assertNotEqual(a.data, b.data)
        self.assertEqual(a.headers["X-TaskVine-Run"], "run-a")
        self.assertEqual(b.headers["X-TaskVine-Run"], "run-b")
        a.close()
        b.close()

    def test_mutable_svg_cache_is_outside_immutable_snapshot(self):
        descriptor = self.client.get("/api/v1/runs/run-a").json
        revision = descriptor["data"]["current_revision"]
        snapshot = self.app.config["SNAPSHOT_STORE"].open("run-a", revision)
        snapshot_root = self.root / "run-a" / ".vine-report" / "snapshots" / revision
        cache_root = self.root / "run-a" / ".vine-report" / "cache" / revision
        self.assertTrue(Path(snapshot.svg_files_dir).is_relative_to(cache_root))
        self.assertFalse(Path(snapshot.svg_files_dir).is_relative_to(snapshot_root))

    def test_v1_rejects_cross_run_revision(self):
        revision = self.client.get("/api/v1/runs/run-a").json["data"]["current_revision"]
        response = self.client.get(f"/api/v1/runs/run-b/revisions/{revision}/summary")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json["error"]["code"], "revision_not_found")

    def test_v1_chart_dispatch_keeps_response_identity(self):
        @self.app.get("/api/test-chart-source")
        def test_chart_source():
            snapshot = get_run_snapshot()
            return jsonify({"observed_run": snapshot.run_id})

        self.app.config["CHART_ENDPOINTS"]["test-chart"] = "test_chart_source"
        descriptor = self.client.get("/api/v1/runs/run-a").json
        revision = descriptor["data"]["current_revision"]
        response = self.client.get(
            f"/api/v1/runs/run-a/revisions/{revision}/charts/test-chart"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["run_id"], "run-a")
        self.assertEqual(response.json["revision"], revision)
        self.assertEqual(response.json["data"]["observed_run"], "run-a")

    def test_source_metric_endpoint_is_pinned_and_fail_closed(self):
        csv_dir = self.root / "run-a" / "csv-files"
        self._write(
            csv_dir / "metric_contracts.csv",
            ["metric_id", "semantics_version", "status", "source", "coverage", "unit", "inclusion", "exclusions", "caveat"],
            [["task-throughput", 1, "PASS", "transactions", 1, "attempts/s", "application", "library,recovery", ""]],
        )
        self._write(
            csv_dir / "metric_task_throughput.csv",
            ["time", "bucket_seconds", "successful", "failed", "success_rate_per_second", "failure_rate_per_second"],
            [[0, 1, 2, 0, 2, 0], [1, 1, 1, 1, 1, 1]],
        )
        revision = self.client.post("/api/v1/runs/run-a/refresh").json["revision"]
        response = self.client.get(
            f"/api/v1/runs/run-a/revisions/{revision}/metrics/task-throughput"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["run_id"], "run-a")
        self.assertTrue(response.json["data"]["available"])
        self.assertEqual(response.json["data"]["series"][0]["points"], [[0.0, 2.0], [1.0, 1.0]])

        unavailable = self.client.get(
            f"/api/v1/runs/run-a/revisions/{revision}/metrics/task-latency"
        )
        self.assertEqual(unavailable.status_code, 200)
        self.assertFalse(unavailable.json["data"]["available"])
        self.assertEqual(unavailable.json["data"]["series"], [])

        summary = self.client.get(
            f"/api/v1/runs/run-a/revisions/{revision}/summary"
        ).json["data"]
        self.assertIsNone(summary["total_tasks"])
        self.assertIsNone(summary["transferred_mb"])

    def test_integer_csv_chart_scalars_are_json_serializable(self):
        revision = self.client.get("/api/v1/runs/run-a").json["revision"]
        for chart in ("file-transferred-size", "file-created-size"):
            response = self.client.get(
                f"/api/v1/runs/run-a/revisions/{revision}/charts/{chart}"
            )
            self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
            self.assertEqual(response.json["run_id"], "run-a")
            self.assertTrue(response.json["data"]["points"])

    def test_parallel_v1_requests_never_cross_runs(self):
        revisions = {
            run: self.client.get(f"/api/v1/runs/{run}").json["data"]["current_revision"]
            for run in ("run-a", "run-b")
        }

        def request_summary(index):
            run = "run-a" if index % 2 == 0 else "run-b"
            url = f"/api/v1/runs/{run}/revisions/{revisions[run]}/summary"
            with self.app.test_client() as client:
                payload = client.get(url).json
            return run, payload["run_id"], payload["revision"], payload["data"]["run"]

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(request_summary, range(128)))
        for expected, response_run, revision, summary_run in results:
            self.assertEqual(response_run, expected)
            self.assertEqual(summary_run, expected)
            self.assertEqual(revision, revisions[expected])

    def test_refresh_rejects_uncommitted_manifest_outputs(self):
        csv_dir = self.root / "run-a" / "csv-files"
        (csv_dir / "parse-manifest.json").write_text(json.dumps({
            "outputs": [{
                "path": "metadata.csv",
                "size_bytes": (csv_dir / "metadata.csv").stat().st_size,
                "sha256": "0" * 64,
            }],
            "source": {},
        }), encoding="utf-8")
        response = self.client.post("/api/v1/runs/run-a/refresh")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json["error"]["code"], "parse_in_progress")

    def test_snapshot_gc_dry_run_apply_and_active_server_lock(self):
        store = self.app.config["SNAPSHOT_STORE"]
        revisions = []
        for marker in ("one", "two", "three"):
            metadata = self.root / "run-a" / "csv-files" / "metadata.csv"
            with metadata.open("a", encoding="utf-8") as handle:
                handle.write(f"gc_marker_{marker},1\n")
            revisions.append(store.refresh("run-a").revision)
        self.assertEqual(len(set(revisions)), 3)

        plan = store.collect(keep=1, min_age_seconds=0, dry_run=True, run_ids=["run-a"])
        self.assertTrue(plan["dry_run"])
        self.assertGreaterEqual(plan["candidate_revisions"], 1)
        self.assertEqual(plan["removed_revisions"], 0)
        for revision in plan["runs"][0]["candidates"]:
            self.assertTrue((self.root / "run-a" / ".vine-report" / "snapshots" / revision).is_dir())

        with CatalogLease(self.root, exclusive=False):
            with self.assertRaises(RunStoreError) as blocked:
                store.collect(keep=1, min_age_seconds=0, dry_run=False, run_ids=["run-a"])
        self.assertEqual(blocked.exception.code, "gc_server_active")

        applied = store.collect(keep=1, min_age_seconds=0, dry_run=False, run_ids=["run-a"])
        self.assertGreaterEqual(applied["removed_revisions"], 1)
        current = store.current_revision("run-a", refresh=False)
        self.assertEqual(current, revisions[-1])
        self.assertEqual(store.open("run-a", current).revision, current)


if __name__ == "__main__":
    unittest.main()
