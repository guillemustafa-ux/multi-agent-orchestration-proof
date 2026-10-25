"""The handoff contract: the one shape every transfer between agents must have."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Agent = Literal["supervisor", "researcher", "writer"]
AGENTS: tuple[str, ...] = ("supervisor", "researcher", "writer")

# Every row of footguns/manifest.json. OrchestratorConfig refuses any other name.
FOOTGUNS: tuple[str, ...] = (
    "01-side-effect-before-interrupt",
    "02-no-dispatch-ledger",
    "03-untyped-handoff",
    "04-no-loop-budget",
    "05-approval-not-bound",
    "06-in-memory-checkpointer",
)


class HandoffContractError(ValueError):
    """A handoff crossed the supervisor boundary without honouring the contract."""


class Handoff(BaseModel):
    """What an agent passes to another agent. Validated at the supervisor boundary.

    Workers return plain dicts (the untrusted side of the boundary: in production that dict
    comes out of a model). The supervisor turns them into this model or refuses the run.
    """

    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(min_length=1)
    from_agent: Agent
    to_agent: Agent
    attempt: int = Field(ge=1)
    payload: dict[str, Any] = Field(default_factory=dict)


def payload_sha(task_id: str, draft: str) -> str:
    """Hash of exactly what a human is asked to approve. Any edit to the draft changes it."""
    canonical = json.dumps({"task_id": task_id, "draft": draft}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
