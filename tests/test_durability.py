"""Durable checkpoints (S6): a restart resumes, it does not redo finished work."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from orchestration import approve, default_researcher, default_writer, initial_state, pending_interrupt, run

ROOT = Path(__file__).resolve().parent.parent


def test_restart_after_crash_resumes_without_rerunning_researcher(make_orchestrator, make_config, sink, call_log):
    crashes_left = {"n": 1}

    def crashing_writer(handoff):
        if crashes_left["n"]:
            crashes_left["n"] -= 1
            raise RuntimeError("worker process died")
        return default_writer(handoff)

    config = make_config(
        researcher=call_log.wrap("researcher", default_researcher),
        writer=call_log.wrap("writer", crashing_writer),
    )

    # A crash that surfaces as an exception: LangGraph keeps the checkpoints written so far.
    first_process = make_orchestrator(config)
    with pytest.raises(RuntimeError):
        run(first_process, initial_state("t1", "restart"), "thread-1")
    assert call_log.count("researcher") == 1
    first_process.close()

    # A new orchestrator over the same checkpoint file picks the thread up where it stopped.
    second_process = make_orchestrator(config)
    state = run(second_process, None, "thread-1")
    ask = pending_interrupt(second_process, "thread-1")
    assert ask is not None
    assert state["notes"]  # the researcher's handoff survived the crash

    state = run(second_process, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert call_log.count("researcher") == 1
    assert call_log.count("writer") == 2
    assert len(sink.records) == 1


def test_hard_process_death_resumes_from_the_last_checkpoint(make_orchestrator, make_config, sink, call_log, tmp_path):
    # A real process dies inside the writer with os._exit(1): no exception, no cleanup, no
    # chance for anything to be flushed afterwards. Only checkpoints already on disk survive.
    child = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("_crash_child.py")),
            str(tmp_path / "checkpoints.sqlite"),
            str(tmp_path / "ledger.sqlite"),
            "thread-1",
        ],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
        capture_output=True,
        text=True,
    )
    assert child.returncode == 1, child.stderr

    config = make_config(
        researcher=call_log.wrap("researcher", default_researcher),
        writer=call_log.wrap("writer", default_writer),
    )
    orch = make_orchestrator(config)
    state = run(orch, None, "thread-1")
    ask = pending_interrupt(orch, "thread-1")
    assert ask is not None
    assert state["notes"]  # written by the dead process, read back from its checkpoint

    state = run(orch, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert call_log.count("researcher") == 0
    assert call_log.count("writer") == 1
    assert len(sink.records) == 1
