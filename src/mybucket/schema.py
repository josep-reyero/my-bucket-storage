"""SQLite schema, connection settings and format checks for one bucket database.

One bucket = one SQLite file (``bucket.sqlite``) in WAL mode. docs/FORMAT.md
describes the same schema for people reading the data with the sqlite3 CLI;
keep the two in sync.

Tables (format 2; format 1 is the same without ``dicts`` and ``frames``):

``meta(k, v)``
    format (=2), created_at (unix ms), trash_days, last_version, and
    active_dict (the dictionary new small objects are compressed with; absent
    when there is none).
``blobs(id, hash, size, stored_size, codec, dict_id, data, created_at)``
    One row per distinct content (dedup unit). ``hash`` is the 32-byte SHA-256
    of the uncompressed bytes (the ETag). ``codec`` 0 = raw, 1 = one zstd
    frame, 2 = zstd frames with a seek table (see ``frames``). ``dict_id`` is
    the ``dicts.id`` the zstd data was compressed with, or NULL. ``data``
    holds the stored bytes when the blob is inline (stored size <= 128 KiB);
    NULL means the stored bytes are the file
    ``blobs/<hex[0:2]>/<hex[2:4]>/<hex>``.
``dicts(id, hash, zstd_id, data, created_at, samples, prefix)``
    Trained zstd dictionaries (``data`` is what ``zstd -D`` reads). Rows are
    never changed; gc removes a dictionary once it is neither active nor used.
``frames(blob_id, start, size, stored_start, stored_size, hash)``
    The independently readable pieces of a large blob, for verified range
    reads: content bytes ``[start, start + size)`` are stored bytes
    ``[stored_start, stored_start + stored_size)`` (one zstd frame for codec 2,
    the same bytes for codec 0), and ``hash`` is their SHA-256.
``objects(key, version, blob_id, content_type, meta, modified)``
    The current version of every key.
``trash(key, version, blob_id, content_type, meta, modified, trashed_at, expires_at, reason)``
    Previous versions kept after an overwrite (reason "overwritten") or a
    delete (reason "deleted") until ``expires_at``.

Reference tracking: a blob is referenced by any ``objects`` or ``trash`` row
pointing at its ``id``. There is no counter to drift: references are the rows
themselves, enforced by foreign keys (``PRAGMA foreign_keys=ON``), with
indexes on ``blob_id`` so that "is this blob referenced?" is cheap.
Unreferenced blobs are exactly::

    SELECT id FROM blobs b
    WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.blob_id = b.id)
      AND NOT EXISTS (SELECT 1 FROM trash t WHERE t.blob_id = b.id)

Format 1 buckets (written by mybucket 0.1) are read as they are. Opening one
for writing upgrades it to format 2 in one transaction (``upgrade_db``): the
two tables are added and ``format`` / ``user_version`` become 2. Nothing
else changes, so every format 1 object reads back exactly as before; an
older mybucket then refuses the bucket instead of misreading it.

Internal for siblings:
    FORMAT_VERSION, APPLICATION_ID, DB_NAME
    connect(path, readonly=False, timeout=10.0) -> sqlite3.Connection
        autocommit connection (isolation_level=None) with mybucket pragmas;
        callers issue BEGIN IMMEDIATE / COMMIT themselves.
    create_db(path, trash_days) -> None    new bucket database (file mode 0600)
    check_db(conn) -> dict                  validates format and schema; returns meta table
    check_schema(conn, fmt) -> None         UnsafeStore unless the schema is exactly mybucket's
    upgrade_db(conn) -> bool                format 1 -> 2 (True if it upgraded); takes the write lock
    has_table(conn, name) -> bool
    translate_sqlite_error(exc, what) -> MyBucketError
    UNREFERENCED_BLOBS_SQL                  the query above
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
import urllib.parse
from pathlib import Path

import re

from .errors import Busy, DiskFull, MyBucketError, NotAStore, StoreError, UnsafeStore, UnsupportedFormat

FORMAT_VERSION = 2
#: PRAGMA application_id of every bucket database: ASCII "mybk".
APPLICATION_ID = 0x6D79626B
DB_NAME = "bucket.sqlite"

SCHEMA_SQL = """
CREATE TABLE meta (
    k TEXT PRIMARY KEY NOT NULL,
    v NOT NULL
) WITHOUT ROWID;

CREATE TABLE blobs (
    id          INTEGER PRIMARY KEY,
    hash        BLOB NOT NULL UNIQUE CHECK (length(hash) = 32),
    size        INTEGER NOT NULL CHECK (size >= 0),
    stored_size INTEGER NOT NULL CHECK (stored_size >= 0),
    codec       INTEGER NOT NULL,
    dict_id     INTEGER,
    data        BLOB,
    created_at  INTEGER NOT NULL
);

CREATE TABLE objects (
    key          TEXT PRIMARY KEY NOT NULL,
    version      INTEGER NOT NULL,
    blob_id      INTEGER NOT NULL REFERENCES blobs(id),
    content_type TEXT,
    meta         TEXT,
    modified     INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX objects_blob ON objects(blob_id);

CREATE TABLE trash (
    key          TEXT NOT NULL,
    version      INTEGER NOT NULL,
    blob_id      INTEGER NOT NULL REFERENCES blobs(id),
    content_type TEXT,
    meta         TEXT,
    modified     INTEGER NOT NULL,
    trashed_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    PRIMARY KEY (key, version)
) WITHOUT ROWID;
CREATE INDEX trash_blob ON trash(blob_id);
CREATE INDEX trash_expires ON trash(expires_at);
"""

#: What format 2 adds to format 1 (also the upgrade script).
SCHEMA_V2_SQL = """
CREATE TABLE IF NOT EXISTS dicts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    hash       BLOB NOT NULL CHECK (length(hash) = 32),
    zstd_id    INTEGER NOT NULL,
    data       BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    samples    INTEGER NOT NULL,
    prefix     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS frames (
    blob_id      INTEGER NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
    start        INTEGER NOT NULL CHECK (start >= 0),
    size         INTEGER NOT NULL CHECK (size > 0),
    stored_start INTEGER NOT NULL CHECK (stored_start >= 0),
    stored_size  INTEGER NOT NULL CHECK (stored_size > 0),
    hash         BLOB NOT NULL CHECK (length(hash) = 32),
    PRIMARY KEY (blob_id, start)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS blobs_dict ON blobs(dict_id) WHERE dict_id IS NOT NULL;
"""

UNREFERENCED_BLOBS_SQL = """
SELECT b.id, CAST(b.hash AS BLOB) AS hash, b.data IS NULL AS in_file FROM blobs b
WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.blob_id = b.id)
  AND NOT EXISTS (SELECT 1 FROM trash t WHERE t.blob_id = b.id)
"""


def translate_sqlite_error(exc: sqlite3.Error, what: str) -> MyBucketError:
    """Turn a sqlite3 exception into a plain-language mybucket error."""
    text = str(exc)
    lowered = text.lower()
    if "locked" in lowered or "busy" in lowered:
        return Busy(f"Could not {what}: the bucket is locked by another process.")
    if "readonly" in lowered or "read-only" in lowered:
        return StoreError(
            f"Could not {what}: the database file or its directory is not writable.",
            hint="Check file permissions. A read-only open still needs a writable bucket "
            "directory (SQLite WAL mode keeps a shared-memory file next to the database).",
        )
    if "full" in lowered:
        return DiskFull(f"Could not {what}: the disk is full. Nothing was written.")
    if "disk i/o error" in lowered:
        return StoreError(
            f"Could not {what}: the disk reported an input/output error.",
            hint="Check that the disk is still attached and has free space, then retry; "
            "if it keeps happening, run verify.",
        )
    if "malformed" in lowered or "not a database" in lowered:
        return StoreError(
            f"Could not {what}: the bucket database is damaged ({text}).",
            hint="Restore the bucket from a snapshot or backup.",
        )
    return StoreError(f"Could not {what}: {text}.")


def _apply_pragmas(conn: sqlite3.Connection, readonly: bool, timeout: float) -> None:
    # A database received from elsewhere is untrusted: SQL stored in its schema
    # may not call functions with side effects (check_schema refuses triggers
    # and views altogether).
    conn.execute("PRAGMA trusted_schema = OFF")
    conn.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)}")
    conn.execute("PRAGMA foreign_keys = ON")
    if readonly:
        conn.execute("PRAGMA query_only = ON")
        return
    # FULL: a committed transaction survives power loss, not just a process crash.
    conn.execute("PRAGMA synchronous = FULL")
    if sys.platform == "darwin":
        # On macOS plain fsync does not flush the drive cache; F_FULLFSYNC does.
        conn.execute("PRAGMA fullfsync = ON")
        conn.execute("PRAGMA checkpoint_fullfsync = ON")
    conn.execute("PRAGMA journal_size_limit = 8388608")


def connect(path: str | os.PathLike, readonly: bool = False, timeout: float = 10.0) -> sqlite3.Connection:
    """Open an existing bucket database. Raises a MyBucketError on failure."""
    path = Path(path)
    if not path.is_file():
        raise StoreError(f"Bucket database {path.name} is missing.", hint="The bucket directory is incomplete.")
    try:
        if readonly:
            uri = "file:" + urllib.parse.quote(str(path.resolve())) + "?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=timeout, isolation_level=None, check_same_thread=False)
        else:
            conn = sqlite3.connect(str(path), timeout=timeout, isolation_level=None, check_same_thread=False)
        _apply_pragmas(conn, readonly, timeout)
        if not readonly:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            if mode.lower() != "wal":
                conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, "open the bucket database") from exc
    return conn


def create_db(path: str | os.PathLike, trash_days: int) -> None:
    """Create a new, empty bucket database at ``path`` (which must not exist)."""
    path = Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    conn = sqlite3.connect(str(path), isolation_level=None)
    try:
        # auto_vacuum must be chosen before the first table exists.
        conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
        conn.execute(f"PRAGMA user_version = {FORMAT_VERSION}")
        conn.execute("BEGIN IMMEDIATE")
        for statement in (SCHEMA_SQL + SCHEMA_V2_SQL).split(";"):
            if statement.strip():
                conn.execute(statement)
        now = int(time.time() * 1000)
        conn.executemany(
            "INSERT INTO meta (k, v) VALUES (?, ?)",
            [("format", FORMAT_VERSION), ("created_at", now), ("trash_days", trash_days), ("last_version", 0)],
        )
        conn.execute("COMMIT")
    finally:
        conn.close()


def check_db(conn: sqlite3.Connection) -> dict:
    """Check that ``conn`` is a mybucket bucket database this code can read; return its meta."""
    try:
        app_id = conn.execute("PRAGMA application_id").fetchone()[0]
        if app_id != APPLICATION_ID:
            raise NotAStore(
                "This file is not a mybucket bucket database.",
                hint="Check the store path; do not put other files in the store directory.",
            )
        meta = dict(conn.execute("SELECT k, v FROM meta").fetchall())
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, "read the bucket database") from exc
    try:
        fmt = int(meta.get("format", 0))
    except (TypeError, ValueError, OverflowError):
        fmt = 0
    if fmt > FORMAT_VERSION:
        raise UnsupportedFormat(f"This bucket uses format {fmt}; this mybucket reads up to format {FORMAT_VERSION}.")
    if fmt < 1:
        raise StoreError("The bucket database has no format version.", hint="Restore it from a snapshot or backup.")
    check_schema(conn, fmt)
    return meta


def _normalise_sql(sql: str) -> str:
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql.replace(" IF NOT EXISTS ", " ")


def _expected_schema() -> dict[str, tuple[str, str, bool]]:
    """name -> (type, normalised sql, format 2 only) for everything mybucket creates."""
    out: dict[str, tuple[str, str, bool]] = {}
    for text, v2 in ((SCHEMA_SQL, False), (SCHEMA_V2_SQL, True)):
        for statement in text.split(";"):
            sql = _normalise_sql(statement)
            if not sql:
                continue
            m = re.match(r"CREATE (TABLE|INDEX) (\w+)", sql)
            assert m, sql
            out[m.group(2)] = (m.group(1).lower(), sql, v2)
    return out


_EXPECTED = _expected_schema()
_SQLITE_INTERNAL = {"sqlite_sequence", "sqlite_stat1", "sqlite_stat2", "sqlite_stat3", "sqlite_stat4"}


def check_schema(conn: sqlite3.Connection, fmt: int) -> None:
    """Refuse (UnsafeStore) a bucket database whose schema mybucket did not write.

    Every table and index mybucket defines must have exactly its definition
    (whitespace aside); triggers, views and virtual tables are never
    accepted. Other plain tables are allowed only when their names start
    with ``x_`` (room for extensions that older readers safely ignore).
    """
    try:
        rows = conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_master").fetchall()
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, "read the bucket database") from exc
    seen = set()
    for kind, name, table, sql in rows:
        problem = None
        text = _normalise_sql(sql) if isinstance(sql, str) else None
        if kind in ("trigger", "view"):
            problem = f"it contains a {kind} ({name!r})"
        elif text is not None and text.upper().startswith("CREATE VIRTUAL"):
            problem = f"it contains a virtual table ({name!r})"
        elif name in _EXPECTED:
            want_kind, want_sql, _v2 = _EXPECTED[name]
            if kind != want_kind or text != want_sql:
                problem = f"its {name!r} {want_kind} is not defined the way mybucket defines it"
            seen.add(name)
        elif kind == "index" and text is None and isinstance(name, str) and name.startswith("sqlite_autoindex_"):
            pass  # made by SQLite for a UNIQUE or PRIMARY KEY constraint
        elif kind == "table" and name in _SQLITE_INTERNAL:
            pass
        elif isinstance(table, str) and table.startswith("x_") and kind in ("table", "index"):
            pass
        else:
            problem = f"it contains an unexpected {kind} ({name!r})"
        if problem:
            raise UnsafeStore(
                f"This bucket database was not written by mybucket, so it was not opened: {problem}.",
                hint="If the store came from elsewhere, take the bucket from a trusted copy, or read "
                "its data by hand as docs/FORMAT.md describes.",
            )
    required = [n for n, (k, _sql, v2) in _EXPECTED.items() if k == "table" and (fmt >= 2 or not v2)]
    missing = [n for n in required if n not in seen]
    if missing:
        raise StoreError(
            f"The bucket database is incomplete: the {', '.join(repr(n) for n in missing)} table is missing.",
            hint="Restore the bucket from a snapshot or backup.",
        )


def has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)).fetchone() is not None


def upgrade_db(conn: sqlite3.Connection) -> bool:
    """Upgrade a format 1 bucket database to format 2, in one transaction.

    Idempotent and crash-safe: either everything below is committed or
    nothing is. Returns True when this call did the upgrade.
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            fmt = int(conn.execute("SELECT v FROM meta WHERE k = 'format'").fetchone()[0])
            if fmt >= FORMAT_VERSION:
                conn.execute("ROLLBACK")
                return False
            for statement in SCHEMA_V2_SQL.split(";"):
                if statement.strip():
                    conn.execute(statement)
            conn.execute("UPDATE meta SET v = ? WHERE k = 'format'", (FORMAT_VERSION,))
            conn.execute(f"PRAGMA user_version = {FORMAT_VERSION}")
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, "upgrade the bucket database to format 2") from exc
    return True
