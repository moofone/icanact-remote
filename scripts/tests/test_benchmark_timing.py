"""Static regression for explicit Criterion timing boundaries, not runtime proof."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]


def measured_region(source):
    start = "let started = Instant::now();"
    end = "measured += started.elapsed();"
    if source.count(start) != 1 or source.count(end) != 1:
        raise ValueError("exactly one explicit timing region required")
    before, rest = source.split(start)
    target, after = rest.split(end)
    return before, target, after


class BenchmarkTimingTests(unittest.TestCase):
    def test_setup_and_shutdown_are_excluded_from_target_timer(self):
        cases = (
            ("connect_paths.rs", "create_registry(", "peer.connect(", "client.shutdown().await.unwrap();"),
            ("gossip_round.rs", "make_registry(actors, peers).await", "prepare_gossip_round()", "black_box(tasks);"),
        )
        for name, setup, operation, teardown in cases:
            with self.subTest(benchmark=name):
                source = (ROOT / "benches" / name).read_text()
                self.assertIn(".iter_custom(|iterations| async move", source)
                self.assertNotIn("iter_batched", source)
                before, target, after = measured_region(source)
                self.assertIn("for _ in 0..iterations", before)
                self.assertIn(setup, before)
                self.assertIn(operation, target)
                self.assertNotIn(setup, target)
                self.assertNotIn("shutdown", target)
                self.assertNotIn("black_box", target)
                self.assertIn(teardown, after)
                self.assertIn("measured", after)

    def test_timing_region_parser_has_positive_negative_and_nonvacuity_controls(self):
        self.assertEqual(
            measured_region("setup;let started = Instant::now();target;measured += started.elapsed();drop;"),
            ("setup;", "target;", "drop;"),
        )
        for source in ("", "let started = Instant::now();", "measured += started.elapsed();"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                measured_region(source)


if __name__ == "__main__":
    unittest.main()
