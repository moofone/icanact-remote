import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("sample_transport", Path(__file__).resolve().parents[1] / "sample_transport.py")
SAMPLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SAMPLE)

GOOD = ("[throughput_benchmarks::tell_delivered] messages=100 payload=256B "
        "elapsed=10.000000s throughput=10.00 msg/s\n"
        "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n")

class TransportTests(unittest.TestCase):
    def test_completion_and_rate(self):
        result = SAMPLE.parse(GOOD, "tell_delivered", 10)
        self.assertEqual(result["completed"], 100)
        self.assertEqual(result["metrics"]["throughput"], 10)

    def test_libtest_same_line_prefix_is_supported(self):
        result = SAMPLE.parse("test integration::fixture ... " + GOOD, "tell_delivered", 10)
        self.assertEqual(result["completed"], 100)

    def test_zero_ambiguous_short_and_inconsistent_results_block(self):
        for output, minimum in [(GOOD.replace("1 passed", "0 passed"), 0),
                                (GOOD + GOOD, 0), (GOOD, 11),
                                (GOOD.replace("throughput=10.00", "throughput=20.00"), 0)]:
            with self.assertRaises(ValueError):
                SAMPLE.parse(output, "tell_delivered", minimum)

if __name__ == "__main__":
    unittest.main()
