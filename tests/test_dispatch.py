"""At-most-once dispatch through the claim-then-send ledger (S5)."""

import pytest

from orchestration import (
    DispatchLedger,
    RecordingSink,
    approve,
    initial_state,
    ledger_key,
    pending_interrupt,
    run,
    thread_config,
)


class FlakySink(RecordingSink):
    """The server processes the request, then the connection drops before the client hears back."""

    def __init__(self):
        super().__init__()
        self.failures_left = 1

    def publish(self, task_id, draft):
        super().publish(task_id, draft)
        if self.failures_left:
            self.failures_left -= 1
            raise ConnectionError("connection reset after the server accepted the publish")


def test_approval_resume_publishes_exactly_once(make_orchestrator, make_config, sink):
    orch = make_orchestrator(make_config())
    run(orch, initial_state("t1", "once"), "thread-1")
    ask = pending_interrupt(orch, "thread-1")

    state = run(orch, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert len(sink.records) == 1


def test_ledger_row_is_sent_after_dispatch(make_orchestrator, make_config, tmp_path):
    orch = make_orchestrator(make_config())
    run(orch, initial_state("t1", "ledger"), "thread-1")
    ask = pending_interrupt(orch, "thread-1")
    run(orch, approve(ask["payload_sha"]), "thread-1")

    assert DispatchLedger(tmp_path / "ledger.sqlite").state(ledger_key("thread-1", "t1")) == "sent"


def test_crash_after_publish_is_not_retried_blindly(make_orchestrator, make_config, tmp_path):
    flaky = FlakySink()
    orch = make_orchestrator(make_config(sink=flaky))
    run(orch, initial_state("t1", "crash"), "thread-1")
    ask = pending_interrupt(orch, "thread-1")

    with pytest.raises(ConnectionError):
        run(orch, approve(ask["payload_sha"]), "thread-1")
    assert len(flaky.records) == 1

    # The operator retries the thread. The ledger remembers the claim and refuses a second send.
    state = run(orch, None, "thread-1")
    assert len(flaky.records) == 1
    assert state["status"] == "dispatch_unconfirmed"
    assert DispatchLedger(tmp_path / "ledger.sqlite").state(ledger_key("thread-1", "t1")) == "claimed"


def test_second_version_of_a_dispatched_task_is_not_sent_again(make_orchestrator, make_config, sink):
    orch = make_orchestrator(make_config())
    run(orch, initial_state("t1", "versions"), "thread-1")
    ask = pending_interrupt(orch, "thread-1")
    run(orch, approve(ask["payload_sha"]), "thread-1")
    assert len(sink.records) == 1

    # The draft is corrected after it went out and the thread is resumed; the gate asks again.
    orch.graph.update_state(thread_config("thread-1"), {"draft": "v3 correction"}, as_node="writer")
    run(orch, None, "thread-1")
    again = pending_interrupt(orch, "thread-1")
    assert again["draft"] == "v3 correction"

    # Approving the correction does not publish the same task a second time.
    state = run(orch, approve(again["payload_sha"]), "thread-1")
    assert state["status"] == "already_dispatched"
    assert [r["draft"] for r in sink.records] == [ask["draft"]]


def test_ledger_key_separates_thread_and_task_ids(make_orchestrator, make_config, sink):
    orch = make_orchestrator(make_config())
    for thread_id, task_id in (("acme:1", "7"), ("acme", "1:7")):
        run(orch, initial_state(task_id, "collision"), thread_id)
        ask = pending_interrupt(orch, thread_id)
        state = run(orch, approve(ask["payload_sha"]), thread_id)
        assert state["status"] == "dispatched"
    assert [r["task_id"] for r in sink.records] == ["7", "1:7"]
