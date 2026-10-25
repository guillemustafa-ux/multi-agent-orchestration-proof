"""The graph: a supervisor, two workers, an approval gate and a dispatch node.

    START -> supervisor -> researcher -> supervisor -> writer -> supervisor -> approval -> dispatch -> END
                                                                                 ^            |
                                                                                 +-- mismatch -+

Workers hand back to the supervisor through static edges. The supervisor validates every
incoming handoff against the contract, keeps the handoff budget, and routes with Command.
The approval node only asks; the dispatch node only sends, through the ledger.
"""

from __future__ import annotations

import operator
import sqlite3
from dataclasses import dataclass
from typing import Annotated, Any, Literal, TypedDict

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from pydantic import ValidationError

from .config import OrchestratorConfig
from .contracts import REPLY_MODELS, Handoff, HandoffContractError, payload_sha
from .ledger import DispatchLedger, ledger_key
from .workers import WorkerFn

Worker = Literal["researcher", "writer"]


class OrchestrationState(TypedDict, total=False):
    task: dict[str, Any]
    pending: list[str]
    # Raw dict returned by the last worker; the supervisor validates it and clears it.
    inbox: dict[str, Any] | None
    # Every validated handoff, in order. Workers read the last one addressed to them.
    handoffs: Annotated[list[dict[str, Any]], operator.add]
    handoffs_used: int
    notes: str | None
    draft: str | None
    needs_more_research: bool
    approval: dict[str, Any] | None
    status: str
    error: str | None


def initial_state(task_id: str, topic: str) -> OrchestrationState:
    return {
        "task": {"task_id": task_id, "topic": topic},
        "pending": [task_id],
        "inbox": None,
        "handoffs_used": 0,
        "notes": None,
        "draft": None,
        "needs_more_research": False,
        "approval": None,
        "status": "running",
        "error": None,
    }


def thread_config(thread_id: str, recursion_limit: int | None = None) -> RunnableConfig:
    config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
    if recursion_limit is not None:
        config["recursion_limit"] = recursion_limit
    return config


@dataclass
class Orchestrator:
    """A compiled graph, the configuration that built it, and the checkpoint connection."""

    graph: Any
    config: OrchestratorConfig
    connection: sqlite3.Connection | None = None

    def __enter__(self) -> Orchestrator:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    @property
    def durability(self) -> Literal["sync", "async", "exit"]:
        """Checkpoints are written before the next step starts: a hard crash loses at most the step in flight."""
        if self.config.footgun == "07-durability-exit":
            # Checkpoints only when the run pauses or ends: a process killed mid-run leaves none.
            return "exit"
        return "sync"

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None


def run(orch: Orchestrator, payload, thread_id: str, recursion_limit: int | None = None) -> OrchestrationState:
    """Invoke or resume a thread."""
    return orch.graph.invoke(payload, config=thread_config(thread_id, recursion_limit), durability=orch.durability)


def pending_interrupt(orch: Orchestrator, thread_id: str) -> dict[str, Any] | None:
    """The question the thread is paused on, or None when nothing is pending."""
    snapshot = orch.graph.get_state(thread_config(thread_id))
    for task in snapshot.tasks:
        if task.interrupts:
            return task.interrupts[0].value
    return None


def approve(payload_sha_value: str) -> Command:
    """The human approves exactly the payload whose hash they were shown."""
    return Command(resume={"decision": "approve", "payload_sha": payload_sha_value})


def reject() -> Command:
    return Command(resume={"decision": "reject"})


def build_orchestrator(cfg: OrchestratorConfig) -> Orchestrator:
    ledger = DispatchLedger(cfg.ledger_path)

    def supervisor(state: OrchestrationState) -> Command[Literal["researcher", "writer", "approval", "__end__"]]:
        task = state["task"]
        update: dict[str, Any] = {"inbox": None}
        inbox = state.get("inbox")
        issued = (state.get("handoffs") or [None])[-1]
        # A reply is due when this submission has issued a handoff (handoffs_used resets per
        # submission; the handoffs list does not) and the last one on record is the
        # supervisor's: validated replies are appended behind it, so "last from supervisor"
        # means unanswered. A stale issue left by an earlier contract_error is not due.
        reply_due = (
            issued is not None and issued["from_agent"] == "supervisor" and state.get("handoffs_used", 0) > 0
        )
        if cfg.footgun == "03-untyped-handoff":
            if inbox is not None:
                # The untyped supervisor trusts the dict, routes on a string, and treats a
                # destination it does not know as "the worker is finished".
                update.update(_apply_payload(inbox.get("payload") or {}))
                if inbox.get("to_agent") != "supervisor":
                    return Command(goto=END, update={**update, "status": "done", "pending": []})
        elif reply_due or inbox is not None:
            try:
                if inbox is None:
                    raise HandoffContractError(
                        f"no handoff for {issued['to_agent']} attempt {issued['attempt']}: the inbox is empty"
                    )
                handoff = Handoff.model_validate(inbox)
                if handoff.to_agent != "supervisor":
                    raise HandoffContractError(
                        f"handoff addressed to {handoff.to_agent!r}, expected 'supervisor'"
                    )
                if handoff.task_id != task["task_id"]:
                    raise HandoffContractError(
                        f"handoff for task {handoff.task_id!r}, expected {task['task_id']!r}"
                    )
                if handoff.from_agent != issued["to_agent"]:
                    raise HandoffContractError(
                        f"handoff from {handoff.from_agent!r}, expected from {issued['to_agent']!r}"
                    )
                if handoff.attempt != issued["attempt"]:
                    raise HandoffContractError(
                        f"handoff attempt {handoff.attempt}, expected {issued['attempt']}"
                    )
                reply = REPLY_MODELS[handoff.from_agent].model_validate(handoff.payload)
            except (ValidationError, HandoffContractError) as exc:
                # Refuse the run and keep the task pending; never route on a guess.
                return Command(
                    goto=END,
                    update={**update, "status": "contract_error", "error": f"{type(exc).__name__}: {exc}"},
                )
            update.update(_apply_payload(reply.model_dump(exclude_none=True)))
            update["handoffs"] = [handoff.model_dump()]

        view = {**state, **update}
        target = _next_agent(view)
        if target == "approval":
            return Command(goto="approval", update={**update, "status": "running"})

        used = view.get("handoffs_used", 0)
        if cfg.footgun != "04-no-loop-budget" and used >= cfg.max_handoffs:
            return Command(
                goto=END,
                update={
                    **update,
                    "status": "budget_exceeded",
                    "error": f"{used} handoffs used, budget is {cfg.max_handoffs}; stopped before recursion_limit",
                },
            )

        seen = state.get("handoffs", []) + update.get("handoffs", [])
        outgoing = Handoff(
            task_id=task["task_id"],
            from_agent="supervisor",
            to_agent=target,
            attempt=1 + sum(1 for h in seen if h["to_agent"] == target),
            payload={"topic": task["topic"], "notes": view.get("notes")},
        )
        update["handoffs"] = update.get("handoffs", []) + [outgoing.model_dump()]
        update["handoffs_used"] = used + 1
        update["status"] = "running"
        return Command(goto=target, update=update)

    def worker_node(name: Worker, fn: WorkerFn):
        def node(state: OrchestrationState) -> dict[str, Any]:
            incoming = [h for h in state.get("handoffs", []) if h["to_agent"] == name][-1]
            return {"inbox": fn(Handoff.model_validate(incoming))}

        node.__name__ = name
        return node

    def approval(state: OrchestrationState) -> dict[str, Any]:
        task_id = state["task"]["task_id"]
        draft = state["draft"]
        sha = payload_sha(task_id, draft)
        if cfg.footgun == "01-side-effect-before-interrupt":
            # The mistake the LangGraph docs warn about: a side effect before interrupt() in
            # the same node. The node re-runs from the top on resume, so this sends again,
            # and it already sent before anyone answered.
            cfg.sink.publish(task_id, draft)
        answer = interrupt(
            {
                "task_id": task_id,
                "draft": draft,
                "payload_sha": sha,
                "question": "Publish this draft? Resume with {'decision': 'approve' | 'reject', 'payload_sha': <this hash>}",
            }
        )
        decision = answer.get("decision") if isinstance(answer, dict) else None
        if decision == "reject":
            return {"approval": {"decision": "reject", "payload_sha": None}, "status": "rejected", "pending": []}
        if decision == "approve":
            return {"approval": {"decision": "approve", "payload_sha": answer.get("payload_sha")}, "status": "approved"}
        return {"approval": None, "status": "approval_invalid", "error": f"unrecognised decision {decision!r}"}

    def after_approval(state: OrchestrationState) -> Literal["dispatch", "approval", "__end__"]:
        if state["status"] == "approved":
            return "dispatch"
        if state["status"] == "rejected":
            return END
        return "approval"

    def dispatch(state: OrchestrationState, config: RunnableConfig) -> dict[str, Any]:
        task_id = state["task"]["task_id"]
        draft = state["draft"]
        approval_record = state.get("approval") or {}
        if cfg.footgun != "05-approval-not-bound" and approval_record.get("payload_sha") != payload_sha(task_id, draft):
            return {
                "status": "approval_mismatch",
                "approval": None,
                "error": "the approved hash does not match the draft on the table; asking again",
            }
        if cfg.footgun == "01-side-effect-before-interrupt":
            return {"status": "dispatched", "pending": []}  # already sent inside the approval node
        if cfg.footgun == "02-no-dispatch-ledger":
            cfg.sink.publish(task_id, draft)
            return {"status": "dispatched", "pending": []}

        key = ledger_key(config["configurable"]["thread_id"], task_id)
        sha = payload_sha(task_id, draft)
        claim, sent_sha = ledger.claim(key, sha)
        if claim == "sent":
            if sent_sha == sha:
                return {"status": "dispatched", "pending": []}  # a replay of the same send
            return {
                "status": "already_dispatched",
                "error": f"task {task_id!r} already went out on this thread with a different draft; not sending twice",
            }
        if claim == "claimed":
            return {
                "status": "dispatch_unconfirmed",
                "error": f"ledger key {key} was claimed but never confirmed; check the sink before retrying",
            }
        cfg.sink.publish(task_id, draft)
        ledger.mark_sent(key)
        return {"status": "dispatched", "pending": []}

    def after_dispatch(state: OrchestrationState) -> Literal["approval", "__end__"]:
        return "approval" if state["status"] == "approval_mismatch" else END

    builder = StateGraph(OrchestrationState)
    builder.add_node("supervisor", supervisor)
    builder.add_node("researcher", worker_node("researcher", cfg.researcher))
    builder.add_node("writer", worker_node("writer", cfg.writer))
    builder.add_node("approval", approval)
    builder.add_node("dispatch", dispatch)
    builder.add_edge(START, "supervisor")
    builder.add_edge("researcher", "supervisor")
    builder.add_edge("writer", "supervisor")
    builder.add_conditional_edges("approval", after_approval, ["dispatch", "approval", END])
    builder.add_conditional_edges("dispatch", after_dispatch, ["approval", END])
    checkpointer, connection = _checkpointer(cfg)
    return Orchestrator(graph=builder.compile(checkpointer=checkpointer), config=cfg, connection=connection)


def _apply_payload(payload: dict[str, Any]) -> dict[str, Any]:
    update: dict[str, Any] = {"needs_more_research": bool(payload.get("needs_more_research", False))}
    if "notes" in payload:
        update["notes"] = payload["notes"]
    if "draft" in payload:
        update["draft"] = payload["draft"]
    return update


def _next_agent(view: dict[str, Any]) -> Literal["researcher", "writer", "approval"]:
    if view.get("notes") is None or view.get("needs_more_research"):
        return "researcher"
    if view.get("draft") is None:
        return "writer"
    return "approval"


def _checkpointer(cfg: OrchestratorConfig) -> tuple[Any, sqlite3.Connection | None]:
    if cfg.footgun == "06-in-memory-checkpointer":
        # Checkpoints that die with the process: a restart cannot resume the thread.
        return InMemorySaver(), None
    connection = sqlite3.connect(str(cfg.checkpoint_path), check_same_thread=False)
    return SqliteSaver(connection), connection
