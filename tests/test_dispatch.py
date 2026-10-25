"""Exactly-once dispatch through the claim-then-send ledger (S5)."""

import pytest

from orchestration import DispatchLedger, RecordingSink, approve, build_graph, initial_state, pending_interrupt, run


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


def test_approval_resume_publishes_exactly_once(make_config, sink, tmp_path):
    graph = build_graph(make_config())
    run(graph, initial_state("t1", "once"), "thread-1")
    ask = pending_interrupt(graph, "thread-1")

    state = run(graph, approve(ask["payload_sha"]), "thread-1")
    assert state["status"] == "dispatched"
    assert len(sink.records) == 1
    assert DispatchLedger(tmp_path / "ledger.sqlite").state("thread-1:t1") == "sent"


def test_crash_after_publish_is_not_retried_blindly(make_config, tmp_path):
    flaky = FlakySink()
    graph = build_graph(make_config(sink=flaky))
    run(graph, initial_state("t1", "crash"), "thread-1")
    ask = pending_interrupt(graph, "thread-1")

    with pytest.raises(ConnectionError):
        run(graph, approve(ask["payload_sha"]), "thread-1")
    assert len(flaky.records) == 1

    # The operator retries the thread. The ledger remembers the claim and refuses a second send.
    state = run(graph, None, "thread-1")
    assert len(flaky.records) == 1
    assert state["status"] == "dispatch_unconfirmed"
    assert DispatchLedger(tmp_path / "ledger.sqlite").state("thread-1:t1") == "claimed"
