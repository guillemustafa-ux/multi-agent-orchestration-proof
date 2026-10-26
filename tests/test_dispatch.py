"""At-most-once dispatch through the claim-then-send ledger (S5)."""

import pytest

from orchestration import (
    DispatchLedger,
    RecordingSink,
    approve,
    initial_state,
    ledger_key,
    pending_interrupt,
    reject,
    resume_dispatch,
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


class RefusingSink(RecordingSink):
    """The server refuses the first request before accepting anything."""

    def __init__(self):
        super().__init__()
        self.failures_left = 1

    def publish(self, task_id, draft):
        if self.failures_left:
            self.failures_left -= 1
            raise ConnectionError("connection refused before the server accepted the publish")
        super().publish(task_id, draft)


def _leave_an_unconfirmed_dispatch(orch):
    """Crash inside the publish, retry once, and stop at dispatch_unconfirmed with a claimed row."""
    run(orch, initial_state("t1", "operator"), "thread-1")
    ask = pending_interrupt(orch, "thread-1")
    with pytest.raises(ConnectionError):
        run(orch, approve(ask["payload_sha"]), "thread-1")
    state = run(orch, None, "thread-1")
    assert state["status"] == "dispatch_unconfirmed"
    return ask


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


def test_operator_confirm_closes_an_unconfirmed_dispatch_without_sending_again(make_orchestrator, make_config, tmp_path):
    # The operator checked the sink and found the publish there: confirm, then resume.
    # The dispatch node re-runs, finds the row sent with the same hash, and sends nothing.
    flaky = FlakySink()
    orch = make_orchestrator(make_config(sink=flaky))
    _leave_an_unconfirmed_dispatch(orch)
    ledger = DispatchLedger(tmp_path / "ledger.sqlite")
    key = ledger_key("thread-1", "t1")

    ledger.confirm(key)
    state = resume_dispatch(orch, "thread-1")
    assert state["status"] == "dispatched"
    assert len(flaky.records) == 1
    assert ledger.state(key) == "sent"


def test_operator_release_lets_a_refused_publish_go_out_once(make_orchestrator, make_config, tmp_path):
    # The operator checked the sink and the publish is not there: release, then resume.
    refusing = RefusingSink()
    orch = make_orchestrator(make_config(sink=refusing))
    ask = _leave_an_unconfirmed_dispatch(orch)
    assert refusing.records == []
    ledger = DispatchLedger(tmp_path / "ledger.sqlite")
    key = ledger_key("thread-1", "t1")

    ledger.release(key)
    state = resume_dispatch(orch, "thread-1")
    assert state["status"] == "dispatched"
    assert [r["draft"] for r in refusing.records] == [ask["draft"]]
    assert ledger.state(key) == "sent"


def test_confirm_and_release_only_touch_a_claimed_row(tmp_path):
    ledger = DispatchLedger(tmp_path / "ledger.sqlite")
    with pytest.raises(LookupError):
        ledger.confirm(ledger_key("thread-1", "missing"))
    with pytest.raises(LookupError):
        ledger.release(ledger_key("thread-1", "missing"))

    key = ledger_key("thread-1", "t1")
    ledger.claim(key, "sha-1")
    ledger.mark_sent(key)
    with pytest.raises(LookupError):
        ledger.release(key)  # a sent row is never deleted: that would allow a second send
    with pytest.raises(LookupError):
        ledger.confirm(key)
    assert ledger.state(key) == "sent"


def test_resume_dispatch_still_checks_the_recorded_approval(make_orchestrator, make_config, tmp_path):
    # A draft edited after the crash is not the one the human approved: the resumed dispatch
    # asks again instead of sending it, even with the claim released.
    refusing = RefusingSink()
    orch = make_orchestrator(make_config(sink=refusing))
    _leave_an_unconfirmed_dispatch(orch)
    DispatchLedger(tmp_path / "ledger.sqlite").release(ledger_key("thread-1", "t1"))
    orch.graph.update_state(thread_config("thread-1"), {"draft": "edited after the crash"})

    state = resume_dispatch(orch, "thread-1")
    assert state["status"] == "approval_mismatch"
    assert refusing.records == []
    assert pending_interrupt(orch, "thread-1")["draft"] == "edited after the crash"


def test_resume_dispatch_refuses_a_thread_that_is_not_waiting_on_a_dispatch(make_orchestrator, make_config, sink):
    orch = make_orchestrator(make_config())
    run(orch, initial_state("t1", "not yet"), "thread-1")  # waiting at the approval gate
    with pytest.raises(ValueError):
        resume_dispatch(orch, "thread-1")

    run(orch, reject(), "thread-1")
    with pytest.raises(ValueError):
        resume_dispatch(orch, "thread-1")
    assert sink.records == []
    assert pending_interrupt(orch, "thread-1") is None
