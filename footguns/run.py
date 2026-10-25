"""Run the suite under every footgun and check that exactly the expected tests fail.

Usage: python footguns/run.py            (from the repository root)

Exit 0 and the line `all footguns caught` only when the baseline passes and every footgun
makes exactly its `expected_failures` fail, nothing more and nothing less.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "footguns" / "manifest.json"


def run_suite(footgun: str | None) -> tuple[int, list[str]]:
    env = dict(os.environ)
    env.pop("ORCH_FOOTGUN", None)
    if footgun:
        env["ORCH_FOOTGUN"] = footgun
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-rf", "--tb=no", "-p", "no:cacheprovider"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    failed = sorted(line.split()[1] for line in proc.stdout.splitlines() if line.startswith("FAILED "))
    return proc.returncode, failed


def main() -> int:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    problems: list[str] = []

    code, failed = run_suite(None)
    print(f"baseline: exit {code}, failed {len(failed)}")
    if code != 0 or failed:
        problems.append(f"baseline must pass; failed: {failed}")

    for entry in manifest["footguns"]:
        name = entry["name"]
        expected = sorted(entry["expected_failures"])
        code, failed = run_suite(name)
        verdict = "CAUGHT" if code != 0 and failed == expected else "NOT CAUGHT"
        print(f"{name}: exit {code}, failed {len(failed)} (expected {len(expected)}) -> {verdict}")
        if verdict == "NOT CAUGHT":
            missing = sorted(set(expected) - set(failed))
            extra = sorted(set(failed) - set(expected))
            problems.append(f"{name}: missing {missing}, unexpected {extra}")

    if problems:
        print("\n".join(problems))
        return 1
    print("all footguns caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
