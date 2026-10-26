"""Claim-then-send ledger: the external call happens at most once per (thread, task).

Two row states: `claimed` (we are about to send, or we sent and never heard back) and
`sent` (the sink confirmed). A retry that finds `claimed` does not send again: that is the
at-most-once choice, and the row is the operator's evidence that a confirmation is missing.
The operator resolves it after checking the sink: `confirm` (the publish is there) or
`release` (it is not, so the next retry may send). Both act only on a `claimed` row, and
only once no process is still running that dispatch: a live send also holds its row as
`claimed`, and releasing it would let a second send through.
The row also keeps the hash of what was sent, so a later version of the same task is told
the task already went out instead of going out twice.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

ClaimResult = Literal["new", "claimed", "sent"]


def ledger_key(thread_id: str, task_id: str) -> str:
    """One key per (thread, task) that cannot collide however the two ids are spelled."""
    return json.dumps([thread_id, task_id])


class DispatchLedger:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with closing(self._connect()) as con:
            con.execute(
                "create table if not exists dispatch_ledger ("
                " key text primary key, state text not null, payload_sha text not null,"
                " claimed_at text not null, sent_at text)"
            )

    def _connect(self) -> sqlite3.Connection:
        # Autocommit mode: transactions are explicit below.
        return sqlite3.connect(self.path, isolation_level=None)

    def claim(self, key: str, payload_sha: str) -> tuple[ClaimResult, str | None]:
        """Insert the claim unless a row exists; report what was there before and its hash."""
        with closing(self._connect()) as con:
            con.execute("begin immediate")
            row = con.execute("select state, payload_sha from dispatch_ledger where key = ?", (key,)).fetchone()
            if row is not None:
                con.execute("commit")
                return row[0], row[1]
            con.execute(
                "insert into dispatch_ledger (key, state, payload_sha, claimed_at) values (?, 'claimed', ?, ?)",
                (key, payload_sha, _now()),
            )
            con.execute("commit")
            return "new", None

    def mark_sent(self, key: str) -> None:
        with closing(self._connect()) as con:
            con.execute("update dispatch_ledger set state = 'sent', sent_at = ? where key = ?", (_now(), key))

    def confirm(self, key: str) -> None:
        """Operator: the sink has the publish. The claimed row becomes sent; nothing is sent."""
        self._resolve_claimed(key, "update dispatch_ledger set state = 'sent', sent_at = ? where key = ? and state = 'claimed'", (_now(), key))

    def release(self, key: str) -> None:
        """Operator: the sink does not have the publish. The claim is dropped so a retry sends once."""
        self._resolve_claimed(key, "delete from dispatch_ledger where key = ? and state = 'claimed'", (key,))

    def _resolve_claimed(self, key: str, sql: str, params: tuple) -> None:
        with closing(self._connect()) as con:
            con.execute("begin immediate")
            changed = con.execute(sql, params).rowcount
            con.execute("commit")
        if changed != 1:
            raise LookupError(f"ledger key {key} has no claimed row to resolve")

    def state(self, key: str) -> str | None:
        with closing(self._connect()) as con:
            row = con.execute("select state from dispatch_ledger where key = ?", (key,)).fetchone()
        return None if row is None else row[0]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
