"""Child process for the hard-crash test: runs a task and dies inside the writer.

Usage: python tests/_crash_child.py <checkpoint.sqlite> <ledger.sqlite> <thread_id>
Honours ORCH_FOOTGUN so the matrix runs the child under the same footgun as the suite.
"""

import os
import sys

from orchestration import OrchestratorConfig, RecordingSink, build_orchestrator, default_researcher, initial_state, run

# Not 1: an uncaught exception or an import error also exits 1, and the parent must be able
# to tell the planned death from an accident.
DEATH_CODE = 86


def dying_writer(handoff):
    os._exit(DEATH_CODE)


def main() -> None:
    checkpoint_path, ledger_path, thread_id = sys.argv[1:4]
    config = OrchestratorConfig(
        checkpoint_path=checkpoint_path,
        ledger_path=ledger_path,
        sink=RecordingSink(),
        researcher=default_researcher,
        writer=dying_writer,
        footgun=os.environ.get("ORCH_FOOTGUN") or None,
    )
    run(build_orchestrator(config), initial_state("t1", "hard crash"), thread_id)


if __name__ == "__main__":
    main()
