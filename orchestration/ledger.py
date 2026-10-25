"""Claim-then-send ledger: the external call happens at most once per key.

Two rows states: `claimed` (we are about to send, or we sent and never heard back) and
`sent` (the sink confirmed). A retry that finds `claimed` does not send again: that is the
at-most-once choice, and the row is the operator's evidence that a confirmation is missing.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

ClaimResult = Literal["new", "claimed", "sent"]


class DispatchLedger:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with self._connect() as con:
            con.execute(
                "create table if not exists dispatch_ledger ("
                " key text primary key, state text not null, claimed_at text not null, sent_at text)"
            )

    def _connect(self) -> sqlite3.Connection:
        # Autocommit mode: transactions are explicit below.
        return sqlite3.connect(self.path, isolation_level=None)

    def claim(self, key: str) -> ClaimResult:
        """Insert the claim unless a row exists; report what was there before."""
        with self._connect() as con:
            con.execute("begin immediate")
            row = con.execute("select state from dispatch_ledger where key = ?", (key,)).fetchone()
            if row is not None:
                con.execute("commit")
                return row[0]
            con.execute(
                "insert into dispatch_ledger (key, state, claimed_at) values (?, 'claimed', ?)",
                (key, _now()),
            )
            con.execute("commit")
            return "new"

    def mark_sent(self, key: str) -> None:
        with self._connect() as con:
            con.execute(
                "update dispatch_ledger set state = 'sent', sent_at = ? where key = ?",
                (_now(), key),
            )

    def state(self, key: str) -> str | None:
        with self._connect() as con:
            row = con.execute("select state from dispatch_ledger where key = ?", (key,)).fetchone()
        return None if row is None else row[0]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
