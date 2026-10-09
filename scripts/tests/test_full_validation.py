"""Offline deterministic harness tests; no Rust/project commands are executed."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "full_validation.sh"

FAKE_CARGO = """#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open('commands.jsonl', 'a') as f:
    f.write(json.dumps({'args': args, 'offline': os.environ.get('CARGO_NET_OFFLINE')}) + '\\n')
mode = os.environ.get('FAKE_MODE', '')
if mode == 'missing_tool':
    print('cargo unavailable', file=sys.stderr)
    sys.exit(127)
if (mode == 'compile' and 'build' in args) or (mode == 'list_compile' and '--list' in args):
    print('compile error', file=sys.stderr)
    sys.exit(101)
if 'test' not in args:
    sys.exit(0)
if '--list' in args:
    if mode != 'zero_selected':
        print('fixture::real_test: test')
    sys.exit(0)
p = pathlib.Path('attempts')
n = int(p.read_text()) + 1 if p.exists() else 1
p.write_text(str(n))
if mode in ('persistent', 'environment') or (mode == 'first_fail' and n == 1):
    print('environment EPERM' if mode == 'environment' else 'assertion failed', file=sys.stderr)
    sys.exit(7)
print('test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;' %
      (0 if mode == 'zero_executed' else 1))
"""

class ValidationTests(unittest.TestCase):
    def run_fixture(self, mode="", coverage=False, focus=False):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "scripts").mkdir()
            (root / "bin").mkdir()
            shutil.copy2(SCRIPT, root / "scripts/full_validation.sh")
            (root / "Cargo.toml").write_text("[package]\nname='fixture'\n")
            (root / "plan.md").write_text("fixture")
            for name in ("check_no_rkyv_from_bytes", "check_forbidden_copy_patterns",
                         "check_critical_coverage", "analyze_coverage_gaps"):
                path = root / "scripts" / (name + ".sh")
                path.write_text("#!/bin/sh\necho guard-ran\n")
                path.chmod(0o755)
            cargo = root / "bin/cargo"
            cargo.write_text(FAKE_CARGO)
            cargo.chmod(0o755)
            if mode == "capture":
                tee = root / "bin/tee"
                tee.write_text("#!/bin/sh\ncase \"$*\" in *step_*) exit 9;; esac\nexec /usr/bin/tee \"$@\"\n")
                tee.chmod(0o755)
            env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"],
                       FAKE_MODE=mode)
            command = ["bash", str(root / "scripts/full_validation.sh")]
            if coverage:
                command += ["--plan", "plan.md"]
            if focus:
                command += ["--focus", "fixture"]
            result = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True)
            commands = (root / "commands.jsonl").read_text() if (root / "commands.jsonl").exists() else ""
            attempts = (root / "attempts").read_text() if (root / "attempts").exists() else "0"
            logs = "\n".join(p.read_text() for p in root.glob("logs/**/*.log"))
            return result, commands, int(attempts), logs

    def test_success_has_all_lanes_and_retained_output(self):
        result, commands, attempts, logs = self.run_fixture(coverage=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(attempts, 6)
        for flag in ("test-helpers", "--all-features", "--release", "--list", "--locked", "--offline"):
            self.assertIn(flag, commands)
        self.assertIn('"offline": "true"', commands)
        self.assertIn("TEST_COUNTS selected=1 executed=1", logs)
        self.assertIn("test result: ok.", logs)
        self.assertIn("guard-ran", logs)

    def test_failures_are_never_retried_or_called_complete(self):
        for mode in ("first_fail", "persistent", "environment"):
            with self.subTest(mode=mode):
                result, _, attempts, logs = self.run_fixture(mode)
                self.assertEqual(result.returncode, 7)
                self.assertEqual(attempts, 1)
                self.assertNotIn("VALIDATION COMPLETE", logs)
                self.assertIn("command_status=7", logs)

    def test_zero_selection_and_execution_block(self):
        for mode in ("zero_selected", "zero_executed"):
            with self.subTest(mode=mode):
                result, _, _, logs = self.run_fixture(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("ERROR: zero", logs)
                self.assertNotIn("VALIDATION COMPLETE", logs)

    def test_missing_tool_and_compile_failure_block(self):
        for mode, status in (("missing_tool", 127), ("compile", 101)):
            result, _, attempts, logs = self.run_fixture(mode)
            self.assertEqual(result.returncode, status)
            self.assertEqual(attempts, 0)
            self.assertNotIn("VALIDATION COMPLETE", logs)

    def test_focus_preserves_counts_and_never_claims_full_validation(self):
        result, commands, attempts, logs = self.run_fixture(focus=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(attempts, 1)
        self.assertEqual(len(commands.splitlines()), 2)
        self.assertIn("--all-features", commands)
        self.assertIn("TEST_COUNTS selected=1 executed=1", logs)
        self.assertIn("FOCUSED PASS (not full validation)", logs)
        self.assertNotIn("VALIDATION COMPLETE", logs)
        for mode in ("zero_selected", "zero_executed", "first_fail", "list_compile", "missing_tool"):
            with self.subTest(mode=mode):
                result, _, attempts, logs = self.run_fixture(mode, focus=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertLessEqual(attempts, 1)
                self.assertNotIn("FOCUSED PASS", logs)
                self.assertNotIn("VALIDATION COMPLETE", logs)

    def test_capture_failure_blocks(self):
        result, _, _, logs = self.run_fixture("capture")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("VALIDATION COMPLETE", logs)

if __name__ == "__main__":
    unittest.main()
