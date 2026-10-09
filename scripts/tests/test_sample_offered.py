import importlib.util
import json
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "sample_offered", Path(__file__).resolve().parents[1] / "sample_offered.py"
)
SAMPLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SAMPLE)


def output(record):
    return (
        "test fixture ... AB_METRICS " + json.dumps(record) + "\n"
        "test result: ok. 1 passed; 0 failed; 0 ignored;\n"
    )


class OfferedSampleTests(unittest.TestCase):
    def record(self):
        return {
            "workload": "ask_offered1000",
            "completed": 10_000,
            "offered": 10_000,
            "errors": 0,
            "drops": 0,
            "metrics": {
                "p50_ns": 10,
                "p95_ns": 20,
                "p99_ns": 30,
                "duration_ns": 10_100_000_000,
                "offer_window_ns": 10_000_000_000,
                "max_backlog": 2,
            },
        }

    def test_exact_window_and_percentiles(self):
        record = self.record()
        self.assertEqual(SAMPLE.parse(output(record), 1000, 10, 64), record)
        record["metrics"]["duration_ns"] = 9_999_000_000
        self.assertEqual(SAMPLE.parse(output(record), 1000, 10, 64), record)

    def test_short_window_drops_bad_quantiles_and_backlog_block(self):
        for field, value in [
            ("offer_window_ns", 9_000_000_000),
            ("duration_ns", 9_998_999_999),
            ("p99_ns", 5),
            ("max_backlog", 65),
            ("max_backlog", 0),
        ]:
            record = self.record()
            record["metrics"][field] = value
            with self.assertRaises(ValueError):
                SAMPLE.parse(output(record), 1000, 10, 64)
        record = self.record()
        record["drops"] = 1
        with self.assertRaises(ValueError):
            SAMPLE.parse(output(record), 1000, 10, 64)
        record = self.record()
        record["completed"] = 9_999
        with self.assertRaises(ValueError):
            SAMPLE.parse(output(record), 1000, 10, 64)
        with self.assertRaises(ValueError):
            SAMPLE.parse(output(self.record()), 500, 10, 64)


if __name__ == "__main__":
    unittest.main()
