import os

import pytest

from orchestration import OrchestratorConfig, RecordingSink, default_researcher, default_writer

# One footgun at a time, selected by the environment so the same suite runs under every
# row of footguns/manifest.json. Unset means the hardened baseline.
FOOTGUN = os.environ.get("ORCH_FOOTGUN") or None


@pytest.fixture
def sink():
    return RecordingSink()


@pytest.fixture
def make_config(tmp_path, sink):
    """Build an OrchestratorConfig over fresh SQLite files, with overrides for the test."""

    def _make(**overrides):
        kwargs = dict(
            checkpoint_path=tmp_path / "checkpoints.sqlite",
            ledger_path=tmp_path / "ledger.sqlite",
            sink=sink,
            researcher=default_researcher,
            writer=default_writer,
            footgun=FOOTGUN,
        )
        kwargs.update(overrides)
        return OrchestratorConfig(**kwargs)

    return _make


class CallLog:
    """Counts worker invocations by wrapping the worker callables."""

    def __init__(self):
        self.calls: list[str] = []

    def wrap(self, name, fn):
        def _wrapped(handoff):
            self.calls.append(name)
            return fn(handoff)

        return _wrapped

    def count(self, name):
        return self.calls.count(name)


@pytest.fixture
def call_log():
    return CallLog()
