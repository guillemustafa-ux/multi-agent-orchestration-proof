"""Typed handoffs (S3) and the loop budget (S7): nothing is lost, nothing runs forever."""

import pytest

from orchestration import approve, default_researcher, default_writer, initial_state, pending_interrupt, run


def test_happy_path_dispatches_once_after_approval(make_orchestrator, make_config, sink):
    orch = make_orchestrator(make_config())
    state = run(orch, initial_state("t1", "durable handoffs"), "thread-1")

    # The run pauses at the approval gate with the draft and its hash.
    assert state["status"] == "running"
    ask = pending_interrupt(orch, "thread-1")
    assert ask is not None
    assert ask["task_id"] == "t1"
    assert ask["draft"]
    assert len(ask["payload_sha"]) == 64

    state = run(orch, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert state["pending"] == []
    assert [r["task_id"] for r in sink.records] == ["t1"]
    assert sink.records[0]["draft"] == ask["draft"]


def test_malformed_handoff_is_rejected_not_dropped(make_orchestrator, make_config, sink):
    def sloppy_writer(handoff):
        # No task_id, no attempt, and a destination that does not exist in the graph.
        return {"from_agent": "writer", "to_agent": "editor", "payload": {"draft": "whatever"}}

    orch = make_orchestrator(make_config(writer=sloppy_writer))
    state = run(orch, initial_state("t1", "contracts"), "thread-1")

    assert state["status"] == "contract_error"
    assert "task_id" in state["error"] and "Field required" in state["error"]
    # The task is still pending in the last checkpoint: refused, not silently finished.
    assert state["pending"] == ["t1"]
    assert sink.records == []
    assert pending_interrupt(orch, "thread-1") is None


def _set(**fields):
    def mutate(reply):
        reply.update(fields)
        return reply

    return mutate


def _payload(**fields):
    def mutate(reply):
        reply["payload"] = fields
        return reply

    return mutate


@pytest.mark.parametrize(
    ("mutate", "expected_error"),
    [
        (_set(to_agent="researcher"), "expected 'supervisor'"),
        (_set(task_id="other"), "expected 't1'"),
        (_set(from_agent="researcher"), "expected from 'writer'"),
        (_set(attempt=999), "attempt 999, expected 1"),
        (_set(signature="trust me"), "signature"),
        (_payload(draft=12345), "Input should be a valid string"),
        (_payload(drfat="typo"), "drfat"),
        (_payload(), "exactly one of a draft or a request for more research"),
        (_payload(draft="done", needs_more_research=True), "exactly one of a draft or a request for more research"),
        (_payload(draft=""), "String should have at least 1 character"),
        (_payload(draft="   "), "String should match pattern"),
        (lambda reply: None, "no handoff for writer attempt 1: the inbox is empty"),
    ],
    ids=[
        "addressed-to-researcher",
        "other-task",
        "forged-sender",
        "forged-attempt",
        "extra-envelope-key",
        "draft-not-a-string",
        "misspelled-payload-key",
        "empty-payload",
        "draft-and-request-at-once",
        "empty-draft",
        "blank-draft",
        "no-reply",
    ],
)
def test_contract_violations_end_the_run_as_contract_error(make_orchestrator, make_config, sink, mutate, expected_error):
    def writer(handoff):
        return mutate(default_writer(handoff))

    orch = make_orchestrator(make_config(writer=writer))
    state = run(orch, initial_state("t1", "contracts"), "thread-1")

    assert state["status"] == "contract_error"
    assert expected_error in state["error"]
    assert state["pending"] == ["t1"]
    assert sink.records == []


def test_loop_budget_stops_ping_pong_before_recursion_limit(make_orchestrator, make_config, sink, call_log):
    def insatiable_writer(handoff):
        # Never drafts: always sends the supervisor back to the researcher.
        return {
            "task_id": handoff.task_id,
            "from_agent": "writer",
            "to_agent": "supervisor",
            "attempt": handoff.attempt,
            "payload": {"needs_more_research": True},
        }

    config = make_config(
        researcher=call_log.wrap("researcher", default_researcher),
        writer=call_log.wrap("writer", insatiable_writer),
        max_handoffs=8,
    )
    orch = make_orchestrator(config)
    # recursion_limit far above the budget: if the run stops, it was the budget, not LangGraph.
    state = run(orch, initial_state("t1", "ping pong"), "thread-1", recursion_limit=200)

    assert state["status"] == "budget_exceeded"
    assert state["pending"] == ["t1"]
    assert len(call_log.calls) <= 8
    assert state["handoffs_used"] == 8
    assert sink.records == []


def test_resubmission_after_contract_error_runs_the_task_again(make_orchestrator, make_config, sink, call_log):
    broken = {"yes": True}

    def writer(handoff):
        reply = default_writer(handoff)
        if broken["yes"]:
            reply["payload"] = {"draft": 12345}
        return reply

    orch = make_orchestrator(make_config(researcher=call_log.wrap("researcher", default_researcher), writer=writer))
    state = run(orch, initial_state("t1", "resubmit"), "thread-1")
    assert state["status"] == "contract_error"

    # The worker is fixed and the same task is submitted again on the same thread.
    broken["yes"] = False
    state = run(orch, initial_state("t1", "resubmit"), "thread-1")
    assert state["status"] == "running"
    assert call_log.count("researcher") == 2
    ask = pending_interrupt(orch, "thread-1")
    assert ask is not None

    state = run(orch, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert len(sink.records) == 1


def test_blank_researcher_notes_are_a_contract_error(make_orchestrator, make_config, sink):
    def blank_researcher(handoff):
        reply = default_researcher(handoff)
        reply["payload"] = {"notes": "   "}
        return reply

    orch = make_orchestrator(make_config(researcher=blank_researcher))
    state = run(orch, initial_state("t1", "blank notes"), "thread-1")

    assert state["status"] == "contract_error"
    assert "String should match pattern" in state["error"]
    assert state["pending"] == ["t1"]
    assert sink.records == []
