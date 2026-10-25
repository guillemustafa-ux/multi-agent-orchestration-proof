# multi-agent-orchestration-proof

A LangGraph supervisor with two worker agents that never loses and never duplicates a
handoff, and the six footguns that prove the test suite would notice if it did.

No model and no API key are needed. The workers are deterministic Python functions behind
one seam, because the thing that breaks in production is the orchestration around the
model: a handoff dropped on a malformed reply, a side effect that runs twice on resume, an
approval that releases a draft nobody approved, two agents looping until the bill arrives, a
restart that redoes finished work. Each of those has a test here, and each has a footgun that
reintroduces it so you can watch the suite catch it.

## Run it

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e .[test]
pytest                      # 9 passed
python footguns/run.py      # baseline + 6 footguns, ends with: all footguns caught
```

The footgun run executes the suite seven times (about 20 seconds locally). Verified with
Python 3.13 and LangGraph 1.1.6; CI runs 3.11 and 3.13.

## What it proves

| Guarantee | Where | Test | Footgun that removes it |
|---|---|---|---|
| Every handoff is validated at the supervisor boundary; a malformed one is refused with the task still pending, never routed on a guess or silently finished | `orchestration/contracts.py`, `supervisor` in `graph.py` | `test_malformed_handoff_is_rejected_not_dropped` | 03 |
| A human approves exactly the payload they were shown: the approval carries the SHA-256 of the draft, and dispatch refuses any other draft, or an approval with no hash | `approval`, `dispatch` | `test_stale_approval_does_not_dispatch_an_edited_draft`, `test_approval_without_hash_fails_closed`, `test_reject_dispatches_nothing` | 05, 01 |
| The external call happens at most once per `(thread_id, task_id)`: across the interrupt resume, and across a crash after the server accepted the call | `orchestration/ledger.py`, `dispatch` | `test_approval_resume_publishes_exactly_once`, `test_crash_after_publish_is_not_retried_blindly` | 02, 01 |
| A process restart resumes the thread from its last checkpoint; the researcher's finished work is not redone | `SqliteSaver`, `durability="sync"` | `test_restart_after_crash_resumes_without_rerunning_researcher` | 06 |
| Two workers that keep asking each other for more are stopped by a per-thread handoff budget, long before LangGraph's `recursion_limit` | `max_handoffs`, `supervisor` | `test_loop_budget_stops_ping_pong_before_recursion_limit` | 04 |

## The graph

```
START -> supervisor -> researcher -> supervisor -> writer -> supervisor -> approval -> dispatch -> END
                                                                            ^            |
                                                                            +- mismatch -+
```

Workers hand back to the supervisor through static edges. The supervisor is the only node
that routes (`Command(goto=...)`), the only node that reads a worker's reply, and the only
node that counts. The approval node only asks. The dispatch node only sends.

### The handoff contract

```python
class Handoff(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_id: str
    from_agent: Literal["supervisor", "researcher", "writer"]
    to_agent: Literal["supervisor", "researcher", "writer"]
    attempt: int            # 1 + previous handoffs to the same agent on this thread
    payload: dict[str, Any]
```

A worker returns a plain `dict`. In production that dict comes out of a model, so it is the
untrusted side of the boundary. The supervisor runs `Handoff.model_validate` on it and also
checks that it is addressed to the supervisor and belongs to the current task. Anything else
ends the run with `status = "contract_error"`, the pydantic message in `error`, and the task
still in `pending`.

### The approval gate

The approval node pauses with `interrupt()` and shows the draft together with
`payload_sha = sha256({"task_id", "draft"})`. The reviewer resumes with:

```python
Command(resume={"decision": "approve", "payload_sha": "<the hash they were shown>"})
Command(resume={"decision": "reject"})
```

The dispatch node recomputes the hash of the draft it holds and compares it with the
approved one. A mismatch, or a missing hash, means `status = "approval_mismatch"` and the
gate asks again with the current draft. The test edits the draft after the reviewer saw
version 1 (through `update_state`, the official way to edit a thread) and then delivers the
stale approval of version 1: nothing is sent, the gate re-asks with version 2's hash, and
approving version 2 sends it once.

### The dispatch ledger

The only side effect in the graph lives in its own node, after the approval node, and goes
through a claim-then-send ledger in SQLite:

1. `claim(key)` inserts `key = "<thread_id>:<task_id>"` as `claimed` inside `BEGIN IMMEDIATE`,
   or reports the row that is already there;
2. `new` sends and marks the row `sent`;
3. `sent` returns `dispatched` without sending again;
4. `claimed` returns `status = "dispatch_unconfirmed"` and does not send.

Case 4 is the honest one. If the process dies between the send and `mark_sent`, the row
stays `claimed` and the retry does not fire again: that is **at-most-once**, chosen over
at-least-once because a duplicate publish is worse than a publish an operator has to
confirm. The row is the evidence that a confirmation is missing.

### The budget

`max_handoffs` (default 8) counts handoffs from the supervisor to a worker on the thread.
The ninth is refused with `status = "budget_exceeded"` and the task still pending. The
ping-pong test sets `recursion_limit` to 200 so the only thing that can stop the run early
is the budget. `recursion_limit` stays as the backstop it is meant to be: since 1.0.6 it
defaults to 1000 super-steps, which in production is 1000 model calls.

## Footgun matrix

Each footgun is one flag of `OrchestratorConfig` that reintroduces one real failure. The
test suite runs unchanged under `ORCH_FOOTGUN=<name>`; `footguns/manifest.json` lists the
exact test ids that must fail under each, and `footguns/run.py` checks that exactly those
fail, nothing more and nothing less.

| # | Footgun | What it reintroduces | Reference | Tests that must fail |
|---|---|---|---|---|
| 01 | `01-side-effect-before-interrupt` | The publish call moves into the approval node, before `interrupt()`. The node re-runs from the top on resume, so it sends again; and it already sent before anyone answered, so a rejection has published too | [LangGraph: side effects called before `interrupt` must be idempotent](https://docs.langchain.com/oss/python/langgraph/interrupts) | 7 of 9: every test that reaches the gate |
| 02 | `02-no-dispatch-ledger` | Dispatch sends without the ledger. The retry after a crash sends again | [LangGraph: re-execution and idempotency](https://docs.langchain.com/oss/python/langgraph/graph-api) | `test_approval_resume_publishes_exactly_once`, `test_crash_after_publish_is_not_retried_blindly` |
| 03 | `03-untyped-handoff` | The supervisor trusts the dict and routes on `to_agent` as a string; a destination it does not know means "finished". The malformed handoff ends the run as `done` with nothing dispatched | The default branch of a routing function | `test_malformed_handoff_is_rejected_not_dropped` (`assert 'done' == 'contract_error'`) |
| 04 | `04-no-loop-budget` | No per-thread budget. The ping-pong runs until `GraphRecursionError: Recursion limit of 200 reached` | [LangGraph: `recursion_limit`](https://docs.langchain.com/oss/python/langgraph/graph-api) | `test_loop_budget_stops_ping_pong_before_recursion_limit` |
| 05 | `05-approval-not-bound` | The approval is a bare decision. A draft edited after review, or an approval with no hash, is dispatched | Same shape as the bound approvals in [hermes-agent-hardened-setup](https://github.com/guillemustafa-ux/hermes-agent-hardened-setup) | `test_stale_approval_does_not_dispatch_an_edited_draft`, `test_approval_without_hash_fails_closed` |
| 06 | `06-in-memory-checkpointer` | Checkpoints live in process memory. The restart has nothing to resume: `EmptyInputError: Received no input for __start__`; a real system would start the task over and rerun the researcher | [LangGraph: checkpointers and durability](https://docs.langchain.com/oss/python/langgraph/checkpointers) | `test_restart_after_crash_resumes_without_rerunning_researcher` |

Expected tail of `python footguns/run.py`:

```
baseline: exit 0, failed 0
01-side-effect-before-interrupt: exit 1, failed 7 (expected 7) -> CAUGHT
02-no-dispatch-ledger: exit 1, failed 2 (expected 2) -> CAUGHT
03-untyped-handoff: exit 1, failed 1 (expected 1) -> CAUGHT
04-no-loop-budget: exit 1, failed 1 (expected 1) -> CAUGHT
05-approval-not-bound: exit 1, failed 2 (expected 2) -> CAUGHT
06-in-memory-checkpointer: exit 1, failed 1 (expected 1) -> CAUGHT
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
```

This plug-in is not exercised by the suite.

## What it does not prove

- Anything about model quality. The workers are stubs on purpose.
- Multi-process concurrency. The ledger serializes claims with `BEGIN IMMEDIATE`, but the
  suite runs one process; two workers racing for the same thread are not tested.
- Exactly-once under every crash. A crash between the claim and the send ends as
  `dispatch_unconfirmed` with a `claimed` row, which is at-most-once plus evidence, not a
  guarantee that the send happened.
- Production deployment: one task per thread, SQLite files on local disk, no retries, no
  tracing. Those are deliberate omissions in a proof of the handoff layer, not oversights.

## License

MIT.
