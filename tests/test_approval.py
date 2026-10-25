"""Human approval bound to the exact payload (S4)."""

from langgraph.types import Command

from orchestration import approve, build_graph, initial_state, pending_interrupt, reject, run


def test_reject_dispatches_nothing(make_config, sink):
    graph = build_graph(make_config())
    run(graph, initial_state("t1", "rejection"), "thread-1")
    assert pending_interrupt(graph, "thread-1") is not None

    state = run(graph, reject(), "thread-1")
    assert state["status"] == "rejected"
    assert sink.records == []
    assert pending_interrupt(graph, "thread-1") is None


def test_stale_approval_does_not_dispatch_an_edited_draft(make_config, sink):
    graph = build_graph(make_config())
    run(graph, initial_state("t1", "binding"), "thread-1")
    first = pending_interrupt(graph, "thread-1")

    # The draft is edited after the reviewer saw version 1 (the official way: update_state),
    # and the run is re-submitted, so the gate asks again with a new hash.
    graph.update_state(
        {"configurable": {"thread_id": "thread-1"}},
        {"draft": "TAMPERED: " + first["draft"]},
        as_node="writer",
    )
    run(graph, None, "thread-1")
    second = pending_interrupt(graph, "thread-1")
    assert second is not None
    assert second["draft"].startswith("TAMPERED")
    assert second["payload_sha"] != first["payload_sha"]

    # The reviewer's approval of version 1 arrives late. It must not release version 2.
    state = run(graph, approve(first["payload_sha"]), "thread-1")
    assert sink.records == []
    assert state["status"] == "approval_mismatch"
    again = pending_interrupt(graph, "thread-1")
    assert again is not None
    assert again["payload_sha"] == second["payload_sha"]

    # Approving what is actually on the table releases it, once.
    state = run(graph, approve(second["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert [r["draft"] for r in sink.records] == [second["draft"]]


def test_approval_without_hash_fails_closed(make_config, sink):
    graph = build_graph(make_config())
    run(graph, initial_state("t1", "fail closed"), "thread-1")
    assert pending_interrupt(graph, "thread-1") is not None

    state = run(graph, Command(resume={"decision": "approve"}), "thread-1")
    assert sink.records == []
    assert state["status"] == "approval_mismatch"
    assert pending_interrupt(graph, "thread-1") is not None
