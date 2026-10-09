"""Regression tests for experiment validity and paired analysis."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("run_ab", Path(__file__).resolve().parents[1] / "run_ab.py")
AB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AB)

def config(goal="nonregression"):
    return {"mode": "pilot", "experiment": "fixture", "environment": {"host": "fixture"},
            "arms": {"A": {"cwd": "."}, "B": {"cwd": "."}},
            "pairs": 10, "sessions": 2, "seed": 123,
            "workloads": [{"id": "fixture", "argv": [sys.executable, "-c", "pass"],
                           "timeout_seconds": 5, "hypothesis": "fixture",
                           "correctness_oracle": "completed count",
                           "metrics": {"cost": {"unit": "ns/op", "direction": "lower",
                                                "goal": goal, "min_improvement": 0.05}}}]}

class ABTests(unittest.TestCase):
    def test_minimum_samples_and_path_safety(self):
        for field, value in (("pairs", 9), ("sessions", 1)):
            c = config()
            c[field] = value
            with self.assertRaises(ValueError):
                AB.validate(c)
        c = config()
        c["workloads"][0]["id"] = "../escape"
        with self.assertRaises(ValueError):
            AB.validate(c)

    def test_analysis_preserves_sessions_and_rejects_regression(self):
        rows = []
        for session in range(2):
            for pair in range(10):
                rows.append({"workload": "fixture", "session": session, "pair": pair,
                             "A": {"metrics": {"cost": 100}}, "B": {"metrics": {"cost": 110 if session else 90}}})
        c = config("improve")
        c["mode"] = "acceptance"
        result = AB.summarize(rows, c)
        self.assertTrue(result["results"][0]["gate_passed"])
        self.assertFalse(result["results"][1]["gate_passed"])
        self.assertEqual(result["disposition"], "rejected-or-inconclusive")

    def test_zero_baseline_never_claims_improvement(self):
        rows = [{"workload": "fixture", "session": s, "pair": i,
                 "A": {"metrics": {"cost": 0}}, "B": {"metrics": {"cost": 0}}}
                for s in range(2) for i in range(10)]
        result = AB.summarize(rows, config("improve"))
        self.assertFalse(result["results"][0]["gate_passed"])
        self.assertIsNone(result["results"][0]["paired_95pct_ci"])

    def test_missing_pairs_fail(self):
        with self.assertRaises(ValueError):
            AB.summarize([], config())

    def test_sample_rejects_failures_missing_metrics_and_zero_tests(self):
        cases = [
            ("raise SystemExit(7)", ValueError),
            ("print('test result: ok. 0 passed')", ValueError),
            ("print('AB_METRICS {}')", ValueError),
            ("print('AB_METRICS ' + " + repr(json.dumps(
                {"workload": "fixture", "completed": 0, "metrics": {"cost": 1}, "errors": 0, "drops": 0})) + ")", ValueError),
            ("print('AB_METRICS ' + " + repr(json.dumps(
                {"workload": "fixture", "completed": 1, "metrics": {"cost": 1}, "errors": 1, "drops": 0})) + ")", ValueError),
            ("print('AB_METRICS {}');print('AB_METRICS {}')", ValueError),
            ("import time;time.sleep(10)", ValueError),
        ]
        for index, (code, exception) in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as temp:
                work = config()["workloads"][0]
                work["argv"] = [sys.executable, "-c", code]
                work["timeout_seconds"] = 0.1 if index == len(cases)-1 else 5
                with self.assertRaises(exception):
                    AB.sample(work, {"cwd": temp}, Path(temp) / "sample")
                self.assertTrue((Path(temp) / "sample/status.json").exists())

    def test_lock_failure_is_retained_without_launching_samples(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "config.json"
            path.write_text(json.dumps(config()))
            with patch.object(AB, "snapshot", return_value={"lock_sha256": None}), patch.object(AB, "sample") as sample:
                with self.assertRaises(ValueError):
                    AB.execute(path, Path(temp) / "output")
                sample.assert_not_called()
            self.assertTrue((Path(temp) / "output/blocked.json").exists())

    def test_source_change_blocks_and_orders_are_balanced(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "config.json"
            path.write_text(json.dumps(config()))
            record = {"workload": "fixture", "completed": 10, "metrics": {"cost": 1}, "errors": 0, "drops": 0}
            stable = {"lock_sha256": "same"}
            with patch.object(AB, "snapshot", side_effect=[stable, stable, stable, {"lock_sha256": "changed"}]), \
                 patch.object(AB, "sample", return_value=record):
                with self.assertRaises(ValueError):
                    AB.execute(path, Path(temp) / "output")
            rows = [json.loads(line) for line in (Path(temp) / "output/samples.jsonl").read_text().splitlines()]
            for session in range(2):
                self.assertEqual(sum(r["order"] == "AB" for r in rows if r["session"] == session), 5)
            self.assertTrue((Path(temp) / "output/blocked.json").exists())

    def test_fixed_duration_samples_have_explicit_duration_gate(self):
        c = config()
        work = c["workloads"][0]
        work["completion_policy"] = "fixed_duration"
        with self.assertRaises(ValueError):
            AB.validate(c)
        work["duration_metric"] = "cost"
        work["minimum_duration"] = 10
        AB.validate(c)
        with tempfile.TemporaryDirectory() as temp:
            record = {"workload": "fixture", "completed": 10, "metrics": {"cost": 1}, "errors": 0, "drops": 0}
            work["argv"] = [sys.executable, "-c", "print('AB_METRICS ' + " + repr(json.dumps(record)) + ")"]
            with self.assertRaises(ValueError):
                AB.sample(work, {"cwd": temp}, Path(temp) / "short")

    def test_valid_sample_retains_logs(self):
        with tempfile.TemporaryDirectory() as temp:
            record = {"workload": "fixture", "completed": 10,
                      "metrics": {"cost": 1}, "errors": 0, "drops": 0}
            work = config()["workloads"][0]
            work["argv"] = [sys.executable, "-c", "print('AB_METRICS ' + " + repr(json.dumps(record)) + ")"]
            self.assertEqual(AB.sample(work, {"cwd": temp}, Path(temp) / "sample"), record)
            self.assertIn("AB_METRICS", (Path(temp) / "sample/stdout.log").read_text())

if __name__ == "__main__":
    unittest.main()
