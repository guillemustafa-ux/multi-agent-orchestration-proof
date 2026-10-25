"""The two workers and the external sink.

Workers are the pluggable seam: a callable that receives the validated Handoff addressed to
it and returns a plain dict handoff back to the supervisor. The defaults are deterministic
so the suite runs without a model or an API key; a production worker puts its LLM call
inside the same callable and returns the same shape.
"""

from __future__ import annotations

from typing import Any, Callable, Protocol

from .contracts import Handoff

WorkerFn = Callable[[Handoff], dict[str, Any]]


def default_researcher(handoff: Handoff) -> dict[str, Any]:
    topic = handoff.payload["topic"]
    return {
        "task_id": handoff.task_id,
        "from_agent": "researcher",
        "to_agent": "supervisor",
        "attempt": handoff.attempt,
        "payload": {"notes": f"Three verified facts about {topic}."},
    }


def default_writer(handoff: Handoff) -> dict[str, Any]:
    topic = handoff.payload["topic"]
    notes = handoff.payload.get("notes") or ""
    return {
        "task_id": handoff.task_id,
        "from_agent": "writer",
        "to_agent": "supervisor",
        "attempt": handoff.attempt,
        "payload": {"draft": f"# {topic}\n\n{notes}"},
    }


class Sink(Protocol):
    """Where the approved draft leaves the system: the call that must never happen twice."""

    def publish(self, task_id: str, draft: str) -> None: ...


class RecordingSink:
    """Records every publish so a test can count them."""

    def __init__(self) -> None:
        self.records: list[dict[str, str]] = []

    def publish(self, task_id: str, draft: str) -> None:
        self.records.append({"task_id": task_id, "draft": draft})
