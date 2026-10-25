"""Run the suite under every footgun and check that exactly the expected tests fail.

Usage: python footguns/run.py            (from the repository root)

Exit 0 and the line `all footguns caught` only when the baseline passes and every footgun
makes exactly its `expected_failures` fail or error, with every other test still passing.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "footguns" / "manifest.json"


def run_suite(footgun: str | None) -> tuple[int, list[str], int]:
    """Exit code, sorted ids of tests that failed or errored, and the number that passed."""
    env = dict(os.environ)
    env.pop("ORCH_FOOTGUN", None)
    if footgun:
        env["ORCH_FOOTGUN"] = footgun
    proc = subprocess.run(
        # -o addopts= drops the pyproject options so this runner owns the verbosity.
        [sys.executable, "-m", "pytest", "-o", "addopts=", "-q", "-rfE", "--tb=no", "-p", "no:cacheprovider"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    broken = sorted(
        line.split()[1] for line in proc.stdout.splitlines() if line.startswith(("FAILED ", "ERROR "))
    )
    passed_counts = re.findall(r"(\d+) passed", proc.stdout)
    passed = int(passed_counts[-1]) if passed_counts else 0  # the summary line comes last
    return proc.returncode, broken, passed


def main() -> int:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    problems: list[str] = []

    code, broken, total = run_suite(None)
    print(f"baseline: exit {code}, {total} passed, {len(broken)} failed")
    if code != 0 or broken or total == 0:
        problems.append(f"baseline must pass; failed: {broken}")

    for entry in manifest["footguns"]:
        name = entry["name"]
        expected = sorted(entry["expected_failures"])
        code, broken, passed = run_suite(name)
        caught = code != 0 and broken == expected and passed == total - len(expected)
        print(
            f"{name}: exit {code}, {passed} passed, {len(broken)} failed (expected {len(expected)})"
            f" -> {'CAUGHT' if caught else 'NOT CAUGHT'}"
        )
        if not caught:
            missing = sorted(set(expected) - set(broken))
            extra = sorted(set(broken) - set(expected))
            problems.append(f"{name}: missing {missing}, unexpected {extra}, passed {passed} of {total}")

    if problems:
        print("\n".join(problems))
        return 1
    print("all footguns caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
