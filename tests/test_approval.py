"""Human approval bound to the exact payload (S4)."""

from langgraph.types import Command

from orchestration import approve, build_orchestrator, initial_state, pending_interrupt, reject, run, thread_config


def test_reject_dispatches_nothing(make_config, sink):
    orch = build_orchestrator(make_config())
    run(orch, initial_state("t1", "rejection"), "thread-1")
    assert pending_interrupt(orch, "thread-1") is not None

    state = run(orch, reject(), "thread-1")
    assert state["status"] == "rejected"
    assert sink.records == []
    assert pending_interrupt(orch, "thread-1") is None


def test_stale_approval_does_not_dispatch_an_edited_draft(make_config, sink):
    orch = build_orchestrator(make_config())
    run(orch, initial_state("t1", "binding"), "thread-1")
    first = pending_interrupt(orch, "thread-1")

    # The draft is edited after the reviewer saw version 1 (the official way: update_state)
    # and the thread is resumed, so the gate asks again with version 2 and its hash.
    orch.graph.update_state(thread_config("thread-1"), {"draft": "TAMPERED: " + first["draft"]}, as_node="writer")
    run(orch, None, "thread-1")
    second = pending_interrupt(orch, "thread-1")
    assert second is not None
    assert second["draft"].startswith("TAMPERED")
    assert second["payload_sha"] != first["payload_sha"]

    # The reviewer's approval of version 1 arrives late, as the answer to that question.
    # It must not release version 2.
    state = run(orch, approve(first["payload_sha"]), "thread-1")
    assert sink.records == []
    assert state["status"] == "approval_mismatch"
    again = pending_interrupt(orch, "thread-1")
    assert again is not None
    assert again["payload_sha"] == second["payload_sha"]

    # Approving what is actually on the table releases it, once.
    state = run(orch, approve(second["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert [r["draft"] for r in sink.records] == [second["draft"]]


def test_approval_without_hash_fails_closed(make_config, sink):
    orch = build_orchestrator(make_config())
    run(orch, initial_state("t1", "fail closed"), "thread-1")
    assert pending_interrupt(orch, "thread-1") is not None

    state = run(orch, Command(resume={"decision": "approve"}), "thread-1")
    assert sink.records == []
    assert state["status"] == "approval_mismatch"
    assert pending_interrupt(orch, "thread-1") is not None


def test_unrecognised_decision_re_asks(make_config, sink):
    orch = build_orchestrator(make_config())
    run(orch, initial_state("t1", "typo"), "thread-1")
    assert pending_interrupt(orch, "thread-1") is not None

    state = run(orch, Command(resume={"decision": "yes"}), "thread-1")
    assert sink.records == []
    assert state["status"] == "approval_invalid"
    assert pending_interrupt(orch, "thread-1") is not None
