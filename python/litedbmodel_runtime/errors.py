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
    """A mapped SCP failure: SCP `kind`, honored bc Policy Kind, the SQLite code, a message.

    ``wrapped`` retains the ORIGINAL concrete driver error (a live psycopg / PyMySQL error) when this
    failure maps a live-DB driver error (Phase B / #84). :func:`map_sqlite_error` flattens the driver
    error's TEXT into the message, but a text string is opaque to a TYPED classifier — so
    :func:`litedbmodel_runtime.tx_options.is_retryable_tx_error`'s SQLSTATE/errno extraction (the
    robust, driver-version-independent classifier) would be DEAD CODE unless the concrete error stays
    reachable. ``wrapped`` (the go ``SqlFailure.Unwrap()`` analogue) re-exposes it so the classifier
    traverses to ``err.sqlstate`` / ``err.args[0]`` even at COMMIT time (where a PG 40001 write-skew /
    MySQL 1213 deadlock surfaces). ``None`` for a synthetically-constructed failure or an in-proc
    SQLite error (which carries its own typed path via ``sqlite_code``). This attribute never touches
    the byte-identical conformance surface (the corpus compares the encoded result, never the error
    object). ``__cause__`` is set to the same driver error (``raise … from``-style) as a second
    traversal path.
    """

    def __init__(
        self,
        kind: str,
        policy: str,
        sqlite_code: Optional[str],
        message: str,
        wrapped: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.policy = policy
        self.sqlite_code = sqlite_code
        self.wrapped = wrapped
        if wrapped is not None:
            # Expose the concrete driver error through the standard exception-chain attribute too, so a
            # traversal that only follows __cause__ still reaches the typed SQLSTATE/errno.
            self.__cause__ = wrapped


class LimitExceededError(Exception):
    """The SHARED cross-language runaway-prevention error (Phase E-2, epic #74; Python port of the TS
    ``LimitExceededError`` reference in ``src/scp/errors.ts``, #99).

    Raised by the native read / relation post-fetch guard when a read (``context='find'``) or a
    ``hasMany`` relation batch (``context='relation'``) returns MORE rows than the cap BAKED onto the
    portable artifact (``ReadGraph.findGuard.hardLimit`` / ``RelationOp.hardLimit``), so an accidental
    missing-WHERE / N+1 pattern fails LOUD instead of loading an unbounded result. NOT a
    :class:`SqlFailure` (a runaway guard is a litedbmodel-level policy error, carrying no ``SQLITE_*``
    code — so :func:`litedbmodel_runtime.static_bundle._re_error_to_sql_failure` propagates it
    unchanged). Byte-for-byte with the TS reference:

      - fields: ``limit`` (the cap), ``count`` (rows fetched), ``context`` (``'find'`` | ``'relation'``),
        ``model`` (the read / relation-TARGET-TABLE), ``relation`` (the relation NAME, relation context);
      - message: ``Query limit exceeded: <where> returned <count-phrase> records, but limit is <limit>.
        This usually indicates a missing WHERE clause or an N+1 query pattern. Set a higher limit or use
        pagination.`` — ``find`` reports ``more than <limit>`` (the N+1 fetch only KNOWS the total
        exceeds the cap); ``relation`` reports the EXACT ``<count>`` (the batch is fetched in full).

    The ``.name`` attribute mirrors the JS ``Error.name`` (``'LimitExceededError'``) so the conformance
    runner asserts the same ``expectedError`` shape across every language port (#100-103).
    """

    def __init__(
        self,
        limit: int,
        count: int,
        context: str,
        model: Optional[str] = None,
        relation: Optional[str] = None,
    ) -> None:
        where = (
            f"find() on {model if model is not None else 'unknown'}"
            if context == "find"
            else f"relation '{relation if relation is not None else 'unknown'}' on "
            f"{model if model is not None else 'unknown'}"
        )
        count_phrase = f"more than {limit}" if context == "find" else f"{count}"
        super().__init__(
            f"Query limit exceeded: {where} returned {count_phrase} records, "
            f"but limit is {limit}. This usually indicates a missing WHERE clause or "
            f"an N+1 query pattern. Set a higher limit or use pagination."
        )
        self.name = "LimitExceededError"
        self.limit = limit
        self.count = count
        self.context = context
        self.model = model
        self.relation = relation

    @classmethod
    def check(
        cls,
        limit: int,
        count: int,
        context: str,
        model: Optional[str] = None,
        relation: Optional[str] = None,
    ) -> None:
        """The SHARED post-fetch runaway check (SSoT) — the ONE ``count > limit ⇒ raise`` primitive
        both the FIND-context guard (:func:`check_find_hard_limit`) and the RELATION-context guard
        (:func:`litedbmodel_runtime.relation.run_relation_op`) call, so no path re-implements the
        comparison or the error assembly (Python port of the rust ``LimitExceededError::check`` /
        go ``CheckLimit`` SSoT). ``None`` (returns) when within the cap; raises otherwise."""
        if count > limit:
            raise cls(limit, count, context, model, relation)


def check_find_hard_limit(limit: int, count: int, model: Optional[str] = None) -> None:
    """The FIND-context runaway guard (Python port of the rust ``check_find_hard_limit`` / go
    ``CheckFindHardLimit``). The compile injects ``LIMIT hardLimit + 1``, so a fetched ``count`` of
    ``hardLimit + 1`` means the TRUE total exceeds the cap ⇒ the read fails LOUD (``context='find'``)
    instead of loading an unbounded set. A thin find-context adapter over :meth:`LimitExceededError.check`
    (the relation guard calls that core directly with its own ``'relation'`` context). ``None`` (returns)
    when within the cap; raises :class:`LimitExceededError` otherwise. The 19 native ops bake explicit
    LIMITs so this is not wired into them (same as rust/go) — it is the available guard primitive."""
    LimitExceededError.check(limit, count, "find", model, None)


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
        # A live-DB (non-SQLite) driver error — e.g. a live psycopg / PyMySQL error. Retain the
        # concrete error (``wrapped``) so the TYPED retryable classifier can reach its SQLSTATE / errno
        # through the mapped failure (Phase B / #84 typed-retryable path). This is the branch a PG
        # 40001 raised at COMMIT lands in.
        message = str(e)
        return SqlFailure("driver_error", "fail", None, f"non-SQLite driver error: {message}", wrapped=e)

    tagged = f"[{code}] {e}"
    if code == "SQLITE_CONSTRAINT_FOREIGNKEY":
        return SqlFailure("foreign_key_violation", "fail", code, tagged, wrapped=e)
    if code.startswith("SQLITE_CONSTRAINT"):
        return SqlFailure("constraint_violation", "fail", code, tagged, wrapped=e)
    if code in ("SQLITE_BUSY", "SQLITE_LOCKED"):
        return SqlFailure("retryable", "retry", code, tagged, wrapped=e)
    return SqlFailure("driver_error", "fail", code, tagged, wrapped=e)
