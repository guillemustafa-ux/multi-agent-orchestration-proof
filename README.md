# multi-agent-orchestration-proof

A LangGraph supervisor with two worker agents that refuses a malformed handoff instead of
dropping it, never sends the same task twice on a thread, and resumes after a crash, and the
seven footguns that prove the test suite would notice if it did not.

No model and no API key are needed. The workers are deterministic Python functions behind
one seam, because the thing that breaks in production is the orchestration around the
model: a handoff dropped or applied on a malformed reply, a side effect that runs twice on
resume, an approval that releases a draft nobody approved, two agents looping until the
bill arrives, a restart that cannot pick the thread back up. Each of those has a test here,
and each has a footgun that reintroduces it so you can watch the suite catch it.

## Run it

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e .[test]
pytest                      # 26 passed
python footguns/run.py      # baseline + 7 footguns, ends with: all footguns caught
```

The footgun run executes the suite eight times (under a minute locally). Verified with
Python 3.13, LangGraph 1.1.6 and langchain-core 1.7.0; CI runs Python 3.11 and 3.13.

## What it proves

| Guarantee | Where | Tests | Footgun that removes it |
|---|---|---|---|
| Every worker reply is validated at the supervisor boundary: envelope, sender, attempt, task, and a per-role payload model with no coercion; a worker that returns nothing is a contract error too. Anything else ends the run as `contract_error` with the task still pending, never routed on a guess, never applied, never silently finished | `orchestration/contracts.py`, `supervisor` in `graph.py` | `test_malformed_handoff_is_rejected_not_dropped`, `test_contract_violations_end_the_run_as_contract_error` (12 cases) | 03 |
| A human approves exactly the payload they were shown: the approval carries the SHA-256 of the draft; the gate re-asks on an answer it does not recognise, and dispatch refuses any other draft or an approval with no hash | `approval`, `dispatch` | `test_stale_approval_does_not_dispatch_an_edited_draft`, `test_approval_without_hash_fails_closed`, `test_unrecognised_decision_re_asks`, `test_reject_dispatches_nothing` | 05, 01 |
| The external call happens at most once per `(thread_id, task_id)`: across the interrupt resume (the send lives in its own node after the gate), across a crash after the server accepted the call (the ledger), and across a corrected version approved later (the ledger keeps the hash of what went out) | `orchestration/ledger.py`, `dispatch` | `test_approval_resume_publishes_exactly_once`, `test_ledger_row_is_sent_after_dispatch`, `test_crash_after_publish_is_not_retried_blindly`, `test_second_version_of_a_dispatched_task_is_not_sent_again`, `test_ledger_key_separates_thread_and_task_ids` | 01, 02 |
| A restart resumes the thread from its last checkpoint instead of redoing finished work: after a worker exception, and after a real process killed mid-run with `os._exit(1)` | `SqliteSaver`, `durability="sync"` | `test_restart_after_crash_resumes_without_rerunning_researcher`, `test_hard_process_death_resumes_from_the_last_checkpoint` | 06, 07 |
| Two workers that keep asking each other for more are stopped by a handoff budget, long before LangGraph's `recursion_limit` | `max_handoffs`, `supervisor` | `test_loop_budget_stops_ping_pong_before_recursion_limit` | 04 |

## The graph

```text
START -> supervisor -> researcher -> supervisor -> writer -> supervisor -> approval -> dispatch -> END
                                                                            ^            |
                                                                            +- mismatch -+
```

Workers hand back to the supervisor through static edges. The supervisor is the only node
that routes between agents (`Command(goto=...)`), the only node that reads a worker's reply,
and the only node that counts. The approval node only asks. The dispatch node only sends.
The two conditional edges after them carry the approve/reject/re-ask outcomes.

### The handoff contract

```python
STRICT = ConfigDict(extra="forbid", strict=True)   # no unknown keys, no coercion

class Handoff(BaseModel):
    model_config = STRICT
    task_id: str = Field(min_length=1)
    from_agent: Literal["supervisor", "researcher", "writer"]
    to_agent: Literal["supervisor", "researcher", "writer"]
    attempt: int = Field(ge=1)
    payload: dict[str, Any] = Field(default_factory=dict)

class ResearcherReply(BaseModel):
    model_config = STRICT
    notes: str = Field(min_length=1, pattern=r"\S")

class WriterReply(BaseModel):
    model_config = STRICT
    draft: str | None = Field(default=None, min_length=1, pattern=r"\S")
    needs_more_research: bool = False
    # plus: exactly one of a draft or a request for more research
```

A worker returns a plain `dict`, or nothing at all when it forgets to. In production that
dict comes out of a model, so it is the untrusted side of the boundary. When a reply is due,
the supervisor validates it as a `Handoff`, then checks that it is addressed to the
supervisor, belongs to the current task, comes from the agent the supervisor handed off to,
and carries the same `attempt` number the supervisor issued; then it validates the payload
with the sender's reply model. Any failure, including no reply, ends the run with
`status = "contract_error"`, the message in `error`, and the task still in `pending`. The
parametrized test covers a reply addressed elsewhere, for another task, with a forged sender,
a forged attempt, an extra envelope key, a draft that is a number, a misspelled payload key,
an empty payload, a draft and a request for more research at once, an empty draft, a blank
draft, and no reply.

### The approval gate

The approval node pauses with `interrupt()` and shows the draft together with
`payload_sha = sha256({"task_id", "draft"})`. The reviewer resumes with:

```python
Command(resume={"decision": "approve", "payload_sha": "<the hash they were shown>"})
Command(resume={"decision": "reject"})
```

The dispatch node recomputes the hash of the draft it holds and compares it with the
approved one. A mismatch, or a missing hash, means `status = "approval_mismatch"` and the
gate asks again with the current draft. An answer that is neither `approve` nor `reject`
means `status = "approval_invalid"` and the gate asks again.

The binding test edits the draft after the reviewer saw version 1 (through `update_state`,
the official way to edit a thread) and resumes, so the gate asks again with version 2 and
its hash; then it delivers the stale approval of version 1 as the answer to that question.
Nothing is sent, the gate re-asks with version 2's hash, and approving version 2 sends it
once.

### The dispatch ledger

The only side effect in the graph lives in its own node, after the approval node, and goes
through a claim-then-send ledger in SQLite keyed by `json.dumps([thread_id, task_id])`, so
`("acme:1", "7")` and `("acme", "1:7")` never share a row:

1. `claim(key, payload_sha)` inserts the row as `claimed` inside `BEGIN IMMEDIATE`, or reports
   the row that is already there and the hash it was claimed with;
2. `new`: send, then mark the row `sent`;
3. `sent` with the same hash: `dispatched`, nothing sent (a replay of the same send);
4. `sent` with a different hash: `status = "already_dispatched"`, nothing sent (a corrected
   version of a task that already went out);
5. `claimed`: `status = "dispatch_unconfirmed"`, nothing sent.

Case 5 is the honest one. If the process dies between the send and `mark_sent`, the row
stays `claimed` and the retry does not fire again: that is **at-most-once**, chosen over
at-least-once because a duplicate publish is worse than a publish an operator has to
confirm. The row is the evidence that a confirmation is missing.

Keeping the send out of the approval node is what makes the interrupt resume safe: the
approval node re-runs from the top on resume, the dispatch node runs once after it. The
ledger covers the other two paths, the crash and the later version.

### Durability

Checkpoints go to SQLite with `durability="sync"`: each one is written before the next step
starts. Two tests resume a thread in a new orchestrator over the same file. In the first the
writer raises; LangGraph writes the checkpoints it has on the way out, so this one also
passes with `"async"` and `"exit"`. In the second a child process runs the task and dies
inside the writer with `os._exit(1)`, with no exception and no cleanup; only what was already
on disk survives, and the parent resumes at the writer without running the researcher again.
That is the test footgun 07 fails.

### The budget

`max_handoffs` (default 8) counts handoffs from the supervisor to a worker for one task
submission (this proof runs one task per thread; a re-submission on the same thread starts a
new count). The ninth is refused with `status = "budget_exceeded"` and the task still
pending. The ping-pong test sets `recursion_limit` to 200 so the only thing that can stop
the run early is the budget. `recursion_limit` stays as the backstop it is meant to be: the
code sets its default to 10,000 super-steps since 1.0.6 (10,007 in 1.1.6; 25 before 1.0.6;
the graph-api docs say 1,000). In this graph every other super-step is the supervisor, so
that backstop is roughly 5,000 worker calls.

## Footgun matrix

Each footgun is one flag of `OrchestratorConfig` that reintroduces one real failure. The
test suite runs unchanged under `ORCH_FOOTGUN=<name>`; `footguns/manifest.json` lists the
exact test ids that must fail under each, and `footguns/run.py` checks that exactly those
fail or error, that every other test still passes, and that the baseline is green.

| # | Footgun | What it reintroduces | Reference | Tests that must fail |
|---|---|---|---|---|
| 01 | `01-side-effect-before-interrupt` | The publish call moves into the approval node, before `interrupt()`. The node re-runs from the top on resume, so it sends again; and it already sent before anyone answered, so a rejection has published too | [LangGraph: side effects called before `interrupt` must be idempotent](https://docs.langchain.com/oss/python/langgraph/interrupts) | 12 of 26: every test that reaches the gate |
| 02 | `02-no-dispatch-ledger` | Dispatch sends without the ledger. The retry after a crash sends again, and a corrected version of a task that already went out goes out too | [LangGraph: re-execution and idempotency](https://docs.langchain.com/oss/python/langgraph/graph-api) | `test_crash_after_publish_is_not_retried_blindly`, `test_second_version_of_a_dispatched_task_is_not_sent_again`, `test_ledger_row_is_sent_after_dispatch` |
| 03 | `03-untyped-handoff` | The supervisor trusts the dict and routes on `to_agent` as a string; a destination it does not know means "finished", and no reply at all is just re-dispatched. The malformed handoff ends the run as `done` with nothing dispatched; forged senders, forged attempts, a numeric draft and a blank draft are applied as if valid; a worker that returns nothing burns the budget | The default branch of a routing function | `test_malformed_handoff_is_rejected_not_dropped` (`assert 'done' == 'contract_error'`) and the 12 contract cases |
| 04 | `04-no-loop-budget` | No handoff budget. The ping-pong runs until `GraphRecursionError: Recursion limit of 200 reached` (10,007 super-steps and over a minute with the default limit) | [LangGraph: `recursion_limit`](https://docs.langchain.com/oss/python/langgraph/graph-api) | `test_loop_budget_stops_ping_pong_before_recursion_limit` |
| 05 | `05-approval-not-bound` | The approval is a bare decision. A draft edited after review, or an approval with no hash, is dispatched | Same shape as the bound approvals in [hermes-agent-hardened-setup](https://github.com/guillemustafa-ux/hermes-agent-hardened-setup) | `test_stale_approval_does_not_dispatch_an_edited_draft`, `test_approval_without_hash_fails_closed` |
| 06 | `06-in-memory-checkpointer` | Checkpoints live in process memory. A new orchestrator has nothing to resume: `EmptyInputError: Received no input for __start__`; a real system would start the task over and rerun the researcher | [LangGraph: checkpointers and durability](https://docs.langchain.com/oss/python/langgraph/checkpointers) | both durability tests |
| 07 | `07-durability-exit` | `durability="exit"`: checkpoints only when the run pauses or ends. The process killed mid-run leaves nothing on disk. A worker exception still gets its checkpoint, which is why only the hard-death test notices | [LangGraph: checkpointers and durability](https://docs.langchain.com/oss/python/langgraph/checkpointers) | `test_hard_process_death_resumes_from_the_last_checkpoint` |

Expected tail of `python footguns/run.py`:

```text
baseline: exit 0, 26 passed, 0 failed
01-side-effect-before-interrupt: exit 1, 14 passed, 12 failed (expected 12) -> CAUGHT
02-no-dispatch-ledger: exit 1, 23 passed, 3 failed (expected 3) -> CAUGHT
03-untyped-handoff: exit 1, 13 passed, 13 failed (expected 13) -> CAUGHT
04-no-loop-budget: exit 1, 25 passed, 1 failed (expected 1) -> CAUGHT
05-approval-not-bound: exit 1, 24 passed, 2 failed (expected 2) -> CAUGHT
06-in-memory-checkpointer: exit 1, 24 passed, 2 failed (expected 2) -> CAUGHT
07-durability-exit: exit 1, 25 passed, 1 failed (expected 1) -> CAUGHT
all footguns caught
```

## Plugging in a real model

A worker is a callable `Handoff -> dict`. The model call goes inside it; the return shape
does not change, and the supervisor validates it the same way:

```python
def llm_writer(handoff: Handoff) -> dict:
    draft = my_model.invoke(f"Write about {handoff.payload['topic']} using: {handoff.payload['notes']}")
    return {
        "task_id": handoff.task_id,
        "from_agent": "writer",
        "to_agent": "supervisor",
        "attempt": handoff.attempt,
        "payload": {"draft": draft},
    }

config = OrchestratorConfig(checkpoint_path=..., ledger_path=..., sink=my_sink, writer=llm_writer)
with build_orchestrator(config) as orch:      # closes the checkpoint connection on exit
    ...
```

This plug-in is not exercised by the suite.

## What it does not prove

- Anything about model quality. The workers are stubs on purpose.
- Multi-process concurrency. The ledger serializes claims with `BEGIN IMMEDIATE`, but the
  suite runs one orchestrator at a time; two processes racing for the same thread are not
  tested.
- Exactly-once under every crash. A crash between the claim and the send ends as
  `dispatch_unconfirmed` with a `claimed` row, which is at-most-once plus evidence, not a
  guarantee that the send happened.
- Production deployment: one task per thread, SQLite files on local disk, no retries, no
  tracing. Those are deliberate omissions in a proof of the handoff layer, not oversights.

## License

MIT.
