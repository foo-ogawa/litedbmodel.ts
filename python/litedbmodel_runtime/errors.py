"""litedbmodel v2 SCP — Error Mapping (Python port of ``src/scp/errors.ts``, spec §11 item 5).

Maps a SQLite driver error (Python ``sqlite3`` exceptions, or a better-sqlite3-shaped
``SQLITE_*`` code carried in a message tag) to a structured :class:`SqlFailure` with a stable
`kind` + the bc Execution-Plan Policy Kind the runtime honors (fail / retry / continue).

The mapping is closed and explicit (no silent catch-all that hides a driver error): an
unrecognized error maps to ``kind='driver_error'`` / ``policy='fail'`` — loud, and carrying the
original code + message. The `SQLITE_*` code family is mirrored from the TS reference; for the
stdlib ``sqlite3`` driver (whose exceptions do not carry a `SQLITE_*` string code on Python <3.11)
the code is derived from the message text ("UNIQUE constraint failed", "FOREIGN KEY constraint
failed", …) so the same kind/policy is produced as the better-sqlite3 seam.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Optional

# The SCP failure kinds and their honored bc Policy Kind.
_KIND_POLICY = {
    "constraint_violation": "fail",
    "foreign_key_violation": "fail",
    "retryable": "retry",
    "driver_error": "fail",
}


class SqlFailure(Exception):
    """A mapped SCP failure: SCP `kind`, honored bc Policy Kind, the SQLite code, a message."""

    def __init__(self, kind: str, policy: str, sqlite_code: Optional[str], message: str) -> None:
        super().__init__(message)
        self.kind = kind
        self.policy = policy
        self.sqlite_code = sqlite_code


def _code_from_bettersqlite_tag(message: str) -> Optional[str]:
    """Extract a `SQLITE_*` code embedded by the TS seam (`[SQLITE_...] ...`) or a bare mention."""
    m = re.search(r"(SQLITE_[A-Z_]+)", message)
    return m.group(1) if m else None


def _code_from_stdlib(e: BaseException) -> Optional[str]:
    """Derive a `SQLITE_*`-style code from a stdlib sqlite3 exception (type + message text)."""
    # Python 3.11+ exposes sqlite_errorname (e.g. 'SQLITE_CONSTRAINT_UNIQUE'); prefer it.
    name = getattr(e, "sqlite_errorname", None)
    if isinstance(name, str) and name.startswith("SQLITE_"):
        return name
    msg = str(e)
    if isinstance(e, sqlite3.IntegrityError):
        if "FOREIGN KEY" in msg:
            return "SQLITE_CONSTRAINT_FOREIGNKEY"
        return "SQLITE_CONSTRAINT"
    if isinstance(e, sqlite3.OperationalError):
        if "locked" in msg:
            return "SQLITE_LOCKED"
        if "busy" in msg:
            return "SQLITE_BUSY"
    return None


def map_sqlite_error(e: BaseException) -> SqlFailure:
    """Map a caught driver error to a :class:`SqlFailure` (byte-for-byte kind/policy with TS)."""
    if isinstance(e, sqlite3.Error):
        code = _code_from_stdlib(e)
    else:
        code = _code_from_bettersqlite_tag(str(e))

    if code is None:
        message = str(e)
        return SqlFailure("driver_error", "fail", None, f"non-SQLite driver error: {message}")

    tagged = f"[{code}] {e}"
    if code == "SQLITE_CONSTRAINT_FOREIGNKEY":
        return SqlFailure("foreign_key_violation", "fail", code, tagged)
    if code.startswith("SQLITE_CONSTRAINT"):
        return SqlFailure("constraint_violation", "fail", code, tagged)
    if code in ("SQLITE_BUSY", "SQLITE_LOCKED"):
        return SqlFailure("retryable", "retry", code, tagged)
    return SqlFailure("driver_error", "fail", code, tagged)
