#!/usr/bin/env python3
"""Paired A/B process runner and analysis, using only Python's standard library.

A workload command emits exactly one line: AB_METRICS {"workload": "id",
"completed": 123, "metrics": {"ns_per_op": 42}, "errors": 0, "drops": 0}
All configured metric units and hypotheses are declared in the frozen config.
No builds, retries, dependency installs or network setup are implicit.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import signal
import statistics
import subprocess
import sys


def digest(data):
    return hashlib.sha256(data).hexdigest()


def snapshot(root):
    root = Path(root).resolve()
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args])
    names = git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0")
    files = {}
    for raw in names:
        if not raw:
            continue
        name = os.fsdecode(raw)
        path = root / name
        if path.is_symlink():
            raise ValueError(f"snapshot contains a symlink: {name}")
        files[name] = digest(path.read_bytes()) if path.is_file() else "deleted"
    lock = root / "Cargo.lock"
    return {"root": str(root), "head": git("rev-parse", "HEAD").decode().strip(),
            "diff_sha256": digest(git("diff", "--binary", "HEAD")), "files": files,
            "lock_sha256": digest(lock.read_bytes()) if lock.exists() else None}


def validate(config):
    if config.get("mode") not in ("pilot", "acceptance"):
        raise ValueError("mode must be pilot or acceptance")
    if (type(config.get("pairs")) is not int or type(config.get("sessions")) is not int or
            config["pairs"] < 10 or config["sessions"] < 2):
        raise ValueError("at least 10 integer pairs and 2 integer sessions are required")
    if type(config.get("seed")) is not int:
        raise ValueError("integer random seed required")
    if set(config.get("arms", {})) != {"A", "B"}:
        raise ValueError("exactly arms A and B required")
    if not config.get("experiment") or not config.get("environment"):
        raise ValueError("experiment and environment description required")
    workloads = config.get("workloads", [])
    if not workloads or len({w["id"] for w in workloads}) != len(workloads):
        raise ValueError("workloads must have unique IDs")
    for work in workloads:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", work["id"]):
            raise ValueError("workload ID must be a safe path component")
        command = work.get("argv")
        if not isinstance(command, list) or not command or not all(isinstance(v, str) for v in command):
            raise ValueError("argv must be a nonempty string array (no shell expansion)")
        policy = work.get("completion_policy", "fixed_count")
        if policy not in ("fixed_count", "fixed_duration"):
            raise ValueError("unknown completion policy")
        if policy == "fixed_duration" and (
            work.get("duration_metric") not in work.get("metrics", {}) or
            work.get("minimum_duration", 0) <= 0
        ):
            raise ValueError("fixed-duration workloads require a declared duration metric and minimum")
        if not work.get("hypothesis") or not work.get("correctness_oracle"):
            raise ValueError("hypothesis and correctness oracle required")
        if not work.get("metrics") or work.get("timeout_seconds", 0) <= 0:
            raise ValueError("metrics and positive timeout required")
        for spec in work["metrics"].values():
            if spec.get("direction") not in ("lower", "higher") or not spec.get("unit"):
                raise ValueError("metric direction and unit required")
            if spec.get("goal") not in ("nonregression", "improve", "reduce"):
                raise ValueError("unknown metric goal")
            limit = spec.get("max_regression", 0.03)
            cap = 0.05 if spec.get("category") == "memory" else 0.03
            if not isinstance(limit, (int, float)) or not 0 <= limit <= cap:
                raise ValueError("regression cap is 3% (5% for explicit memory metrics)")
            if spec["goal"] == "improve" and spec.get("min_improvement", 0) < 0.05:
                raise ValueError("timing improvement threshold must be at least 5%")
            if spec["goal"] == "reduce" and spec["direction"] != "lower":
                raise ValueError("reduce is only valid for lower-is-better counters")


def sample(work, arm, destination):
    destination.mkdir(parents=True)
    environment = dict(os.environ, CARGO_NET_OFFLINE="true")
    environment.update(arm.get("env", {}))
    environment["CARGO_NET_OFFLINE"] = "true"
    with (destination / "stdout.log").open("wb") as out, (destination / "stderr.log").open("wb") as err:
        started = datetime.now(timezone.utc).isoformat()
        process = subprocess.Popen(work["argv"], cwd=arm["cwd"], env=environment,
                                   stdout=out, stderr=err, start_new_session=True)
        try:
            status = process.wait(timeout=work["timeout_seconds"])
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            (destination / "status.json").write_text(json.dumps({"timeout": True}))
            raise ValueError(f"timeout: {destination}")
    (destination / "status.json").write_text(json.dumps({"exit_code": status, "timeout": False,
        "started": started, "finished": datetime.now(timezone.utc).isoformat()}))
    if status:
        raise ValueError(f"command failed ({status}): {destination}")
    lines = (destination / "stdout.log").read_text().splitlines()
    records = [json.loads(line.removeprefix("AB_METRICS ")) for line in lines if line.startswith("AB_METRICS ")]
    if len(records) != 1:
        raise ValueError(f"exactly one metric record required: {destination}")
    record = records[0]
    completed = record.get("completed")
    if (record.get("workload") != work["id"] or type(completed) is not int or completed <= 0):
        raise ValueError("workload identity mismatch or invalid/zero completion")
    if record.get("errors") != 0 or record.get("drops") != 0:
        raise ValueError("errors/drops must be present and zero")
    for key in work["metrics"]:
        value = record.get("metrics", {}).get(key)
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid/missing metric {key}")
    if work.get("completion_policy") == "fixed_duration":
        if record["metrics"][work["duration_metric"]] < work["minimum_duration"]:
            raise ValueError("sample duration below preregistered minimum")
    return record


def interval(values, seed):
    """Bootstrap independent paired process observations, never message samples."""
    rng = random.Random(seed)
    estimates = sorted(statistics.median(rng.choices(values, k=len(values))) for _ in range(5000))
    return [estimates[124], estimates[4874]]


def summarize(rows, config):
    results = []
    for work in config["workloads"]:
        for key, spec in work["metrics"].items():
            for session in [*range(config["sessions"]), "pooled"]:
                selected = [r for r in rows if r["workload"] == work["id"] and
                            (session == "pooled" or r["session"] == session)]
                expected = config["pairs"] * (config["sessions"] if session == "pooled" else 1)
                if len(selected) != expected:
                    raise ValueError("missing paired observations")
                a = [r["A"]["metrics"][key] for r in selected]
                b = [r["B"]["metrics"][key] for r in selected]
                # With a zero baseline, no finite ratio exists. Equality is a
                # valid non-regression result, but is never a benefit claim.
                if any(value == 0 for value in a):
                    passed = spec["goal"] == "nonregression" and all(x == y for x, y in zip(a, b))
                    change, ci = None, None
                else:
                    changes = [(y / x - 1) if spec["direction"] == "lower" else (1 - y / x)
                               for x, y in zip(a, b)]
                    change = statistics.median(changes)
                    ci = interval(changes, config["seed"] + (session if session != "pooled" else 1000))
                    passed = ci[1] <= spec.get("max_regression", 0.03)
                    if spec["goal"] == "improve":
                        passed = passed and change <= -spec["min_improvement"] and ci[1] < 0
                    elif spec["goal"] == "reduce":
                        # Counters require every pair to improve, not an
                        # aggregate decrease hiding a workload-specific loss.
                        passed = passed and all(y < x for x, y in zip(a, b))
                results.append({"workload": work["id"], "metric": key, "session": session,
                                "unit": spec["unit"], "A_median": statistics.median(a),
                                "B_median": statistics.median(b), "median_cost_change": change,
                                "paired_95pct_ci": ci, "gate_passed": passed})
    all_passed = all(r["gate_passed"] for r in results)
    return {"mode": config["mode"], "results": results,
            "disposition": "pilot-only" if config["mode"] == "pilot" else
                           ("metric-gates-passed-review-required" if all_passed else "rejected-or-inconclusive")}


def execute(config_path, output):
    config_path = Path(config_path).resolve()
    encoded = config_path.read_bytes()
    config = json.loads(encoded)
    validate(config)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)  # never overwrite paid-for/previous evidence
    (output / "experiment.json").write_bytes(encoded)
    rows = []
    rng = random.Random(config["seed"])
    try:
        snapshots = {key: snapshot(arm["cwd"]) for key, arm in config["arms"].items()}
        locks = [v["lock_sha256"] for v in snapshots.values()]
        if config.get("require_same_lock", True) and (None in locks or locks[0] != locks[1]):
            raise ValueError("runtime comparisons require identical existing Cargo.lock files")
        provenance = {"config_sha256": digest(encoded), "runner_sha256": digest(Path(__file__).read_bytes()),
                      "snapshots": snapshots, "environment": config["environment"]}
        (output / "provenance.json").write_text(json.dumps(provenance, indent=2))
        for session in range(config["sessions"]):
            for work in config["workloads"]:
                orders = ["AB" if i % 2 == 0 else "BA" for i in range(config["pairs"])]
                rng.shuffle(orders)
                for pair, order in enumerate(orders):
                    row = {"workload": work["id"], "session": session, "pair": pair, "order": order}
                    for key in order:
                        dest = output / work["id"] / f"session-{session}" / f"pair-{pair}" / key
                        row[key] = sample(work, config["arms"][key], dest)
                    if (work.get("completion_policy", "fixed_count") == "fixed_count" and
                            row["A"]["completed"] != row["B"]["completed"]):
                        raise ValueError("paired workloads completed different operation counts")
                    rows.append(row)
                    with (output / "samples.jsonl").open("a") as stream:
                        stream.write(json.dumps(row) + "\n")
        if config_path.read_bytes() != encoded:
            raise ValueError("experiment configuration changed during run")
        if {key: snapshot(arm["cwd"]) for key, arm in config["arms"].items()} != snapshots:
            raise ValueError("source/lock changed during run")
        report = summarize(rows, config)
        (output / "analysis.json").write_text(json.dumps(report, indent=2))
        return report
    except BaseException as error:
        (output / "blocked.json").write_text(json.dumps({"status": "blocked", "reason": str(error)}))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        report = execute(args.config, args.output)
    except (ValueError, OSError, subprocess.SubprocessError, KeyError) as error:
        print(f"BLOCKED: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return int(report["disposition"] == "rejected-or-inconclusive")


if __name__ == "__main__":
    sys.exit(main())
