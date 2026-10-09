import importlib.util
import json
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("sample_concurrent", Path(__file__).resolve().parents[1] / "sample_concurrent.py")
SAMPLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SAMPLE)

def output(record):
    return ("test fixture ... AB_METRICS " + json.dumps(record) + "\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored;\n")

class ConcurrentSampleTests(unittest.TestCase):
    def record(self):
        return {"workload": "ask_inflight8", "completed": 100,
                "errors": 0, "drops": 0, "metrics": {
                    "throughput": 10, "duration_ns": 10_000_000_000,
                    "p50_ns": 10, "p95_ns": 20, "p99_ns": 30}}

    def test_fixed_duration_completion_and_percentiles(self):
        record = self.record()
        self.assertEqual(SAMPLE.parse(output(record), 8, 10), record)

    def test_mismatch_short_window_bad_quantiles_and_errors_block(self):
        for field, value in [("duration_ns", 1), ("p99_ns", 5), ("throughput", 20)]:
            record = self.record()
            record["metrics"][field] = value
            with self.assertRaises(ValueError):
                SAMPLE.parse(output(record), 8, 10)
        with self.assertRaises(ValueError):
            SAMPLE.parse(output(self.record()), 64, 10)
        record = self.record()
        record["errors"] = 1
        with self.assertRaises(ValueError):
            SAMPLE.parse(output(record), 8, 10)

if __name__ == "__main__":
    unittest.main()
