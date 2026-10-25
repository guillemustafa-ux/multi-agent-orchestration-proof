"""Typed handoffs (S3) and the loop budget (S7): nothing is lost, nothing runs forever."""

from orchestration import approve, build_graph, initial_state, pending_interrupt, run


def test_happy_path_dispatches_once_after_approval(make_config, sink):
    graph = build_graph(make_config())
    state = run(graph, initial_state("t1", "durable handoffs"), "thread-1")

    # The run pauses at the approval gate with the draft and its hash.
    assert state["status"] == "running"
    ask = pending_interrupt(graph, "thread-1")
    assert ask is not None
    assert ask["task_id"] == "t1"
    assert ask["draft"]
    assert len(ask["payload_sha"]) == 64

    state = run(graph, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert state["pending"] == []
    assert [r["task_id"] for r in sink.records] == ["t1"]
    assert sink.records[0]["draft"] == ask["draft"]


def test_malformed_handoff_is_rejected_not_dropped(make_config, sink):
    def sloppy_writer(handoff):
        # No task_id, and a destination that does not exist in the graph.
        return {"from_agent": "writer", "to_agent": "editor", "payload": {"draft": "whatever"}}

    graph = build_graph(make_config(writer=sloppy_writer))
    state = run(graph, initial_state("t1", "contracts"), "thread-1")

    assert state["status"] == "contract_error"
    assert "task_id" in state["error"] or "to_agent" in state["error"]
    # The task is still pending in the last checkpoint: refused, not silently finished.
    assert state["pending"] == ["t1"]
    assert sink.records == []
    assert pending_interrupt(graph, "thread-1") is None


def test_loop_budget_stops_ping_pong_before_recursion_limit(make_config, sink, call_log):
    def insatiable_writer(handoff):
        # Never drafts: always sends the supervisor back to the researcher.
        return {
            "task_id": handoff.task_id,
            "from_agent": "writer",
            "to_agent": "supervisor",
            "attempt": handoff.attempt,
            "payload": {"needs_more_research": True},
        }

    from orchestration import default_researcher

    config = make_config(
        researcher=call_log.wrap("researcher", default_researcher),
        writer=call_log.wrap("writer", insatiable_writer),
        max_handoffs=8,
    )
    graph = build_graph(config)
    # recursion_limit far above the budget: if the run stops, it was the budget, not LangGraph.
    state = run(graph, initial_state("t1", "ping pong"), "thread-1", recursion_limit=200)

    assert state["status"] == "budget_exceeded"
    assert state["pending"] == ["t1"]
    assert len(call_log.calls) <= 8
    assert state["handoffs_used"] == 8
    assert sink.records == []
