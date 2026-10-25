from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .contracts import FOOTGUNS
from .workers import Sink, WorkerFn, default_researcher, default_writer


@dataclass
class OrchestratorConfig:
    checkpoint_path: Path
    ledger_path: Path
    sink: Sink
    researcher: WorkerFn = default_researcher
    writer: WorkerFn = default_writer
    # Handoffs to workers allowed per thread before the supervisor gives up. recursion_limit
    # stays as LangGraph's backstop; this is the budget that stops a loop while it is cheap.
    max_handoffs: int = 8
    # Exactly one regression from footguns/manifest.json, or None for the hardened baseline.
    footgun: str | None = None

    def __post_init__(self) -> None:
        if self.footgun is not None and self.footgun not in FOOTGUNS:
            raise ValueError(f"unknown footgun {self.footgun!r}; known: {', '.join(FOOTGUNS)}")
        if self.max_handoffs < 1:
            raise ValueError("max_handoffs must be at least 1")
