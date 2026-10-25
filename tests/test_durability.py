"""Durable checkpoints (S6): a process restart resumes, it does not redo finished work."""

import pytest

from orchestration import approve, build_graph, default_researcher, default_writer, initial_state, pending_interrupt, run


def test_restart_after_crash_resumes_without_rerunning_researcher(make_config, sink, call_log):
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

    first_process = build_graph(config)
    with pytest.raises(RuntimeError):
        run(first_process, initial_state("t1", "restart"), "thread-1")
    assert call_log.count("researcher") == 1

    # A new process over the same checkpoint file picks the thread up where it stopped.
    second_process = build_graph(config)
    state = run(second_process, None, "thread-1")
    ask = pending_interrupt(second_process, "thread-1")
    assert ask is not None
    assert state["notes"]  # the researcher's handoff survived the crash

    state = run(second_process, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert call_log.count("researcher") == 1
    assert call_log.count("writer") == 2
    assert len(sink.records) == 1
