"""Deterministic coverage-gate fixtures, without Cargo or coverage tooling."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "check_critical_coverage.sh"


class CriticalCoverageTests(unittest.TestCase):
    def run_fixture(self, marker=True, hits=1):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for directory in ("scripts", "src", "reports", "bin"):
                (root / directory).mkdir()
            shutil.copy2(SCRIPT, root / "scripts/check_critical_coverage.sh")
            source = root / "src/lib.rs"
            source.write_text("// CRITICAL_PATH fixture\nfn fixture() {}\n")
            (root / "reports/coverage.lcov").write_text(
                f"SF:{source}\nDA:2,{hits}\nend_of_record\n"
            )
            rg = root / "bin/rg"
            rg.write_text("#!/bin/sh\n" + (
                "printf '%s:1:// CRITICAL_PATH fixture\\n' \"$FIXTURE_SOURCE\"\n"
                if marker else "exit 1\n"
            ))
            rg.chmod(0o755)
            env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"],
                       SKIP_COVERAGE_REBUILD="1", FIXTURE_SOURCE=str(source))
            return subprocess.run(["bash", str(root / "scripts/check_critical_coverage.sh")],
                                  env=env, capture_output=True, text=True)

    def test_missing_markers_cannot_pass_vacuously(self):
        result = self.run_fixture(marker=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no CRITICAL_PATH markers", result.stderr)
        self.assertNotIn("gate passed", result.stdout)

    def test_real_marker_requires_execution(self):
        passed = self.run_fixture(hits=1)
        self.assertEqual(passed.returncode, 0, passed.stderr)
        self.assertIn("1 annotated locations checked", passed.stdout)
        failed = self.run_fixture(hits=0)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("executed 0 times", failed.stdout)


if __name__ == "__main__":
    unittest.main()
