"""One bucket: objects, conditional writes, batches, trash and listing.

Public (``Bucket``):
    put(key, data, content_type=None, metadata=None, if_match=None, if_none_match=None,
        expected_size=None, expected_sha256=None) -> PutInfo
    get(key) -> bytes
    open(key) -> ObjectStream            buffered binary stream with ``.info``, ``.verified``
    head(key) -> ObjectInfo              raises ObjectNotFound
    exists(key) -> bool
    list(prefix="", delimiter=None, limit=None, start_after=None) -> ListResult
    delete(key, if_match=None, missing_ok=False) -> ObjectInfo | None
                                         (the version moved to trash)
    batch() -> Batch                     context manager; all writes commit together
    list_versions(key) -> list[VersionInfo]    newest first
    restore(key, version) -> PutInfo     bring a trashed version back
    gc(...), verify()                    see mybucket.maintenance
    export_dir(dest, ...), import_dir(src, ...)   see mybucket.transfer
    stats() -> BucketStats
    configure(trash_days=None)
    trash_days (property), name, path, readonly, close()

A Bucket handle belongs to one bucket directory. If another process deletes
the bucket (and perhaps creates a new one with the same name), every later
call on the old handle raises BucketNotFound instead of writing into, or
cleaning up, the wrong files: each transaction first checks that the
database file is still the one it opened and carries no deletion mark.

Repair: when a put (or import) brings content the bucket already stores,
the stored copy is re-checked first; if it is damaged, the write replaces it
with the new bytes, which are proven correct by their SHA-256.

Write path and crash safety (see also blobs.py invariants):

1. Outside any lock, the input is hashed and, unless the bucket already has
   this content, compressed. Large input is streamed into an fsynced temp
   file under ``tmp/``; small stored blobs stay in memory.
2. ``BEGIN IMMEDIATE`` takes the bucket write lock. Inside it: read the
   current row, evaluate ``if_match`` / ``if_none_match``, detect a no-op
   ("unchanged"), then reuse the existing blob row for this hash or create
   one (inline bytes in the row, or ``install()`` the temp file and fsync the
   directory), move the previous version to ``trash``, and write the new
   ``objects`` row.
3. ``COMMIT`` (``synchronous=FULL``). If anything fails before the commit,
   files installed by this transaction are removed while the lock is still
   held, then the transaction rolls back. A crash before the commit leaves at
   most an orphan blob file with no row, which gc may delete (holding the
   lock); a committed object is never affected.

Readers use WAL snapshots. If a reader's row points at a blob file that gc
removed after the reader's snapshot, the read is retried with a fresh
snapshot (the object will then be gone or changed).

Internal for siblings (maintenance and CLI code):
    Bucket.path, .db_path, .blobs_dir, .tmp_dir   Paths of this bucket
    Bucket._now() -> int                          unix ms from the store clock
    Bucket._write_txn(what) -> ctx[_Txn]          BEGIN IMMEDIATE .. COMMIT with the
                                                  write lock; ``txn.conn`` is the
                                                  connection, ``txn.installed`` the
                                                  blob hexes installed by it
    Bucket._read_txn() -> ctx[sqlite3.Connection] consistent read snapshot (deferred BEGIN)
    Bucket._locked_conn() -> ctx[sqlite3.Connection]  the raw connection, lock held,
                                                  no transaction (e.g. for the backup API)
    Bucket._current(conn, key) -> _Current | None
    Bucket._trash_entry(conn, key, version) -> _Current | None
    Bucket._set_current(txn, key, blob_id, content_type, meta_json, previous, reason) -> int
                                                  makes blob_id the current content of
                                                  key (new version number), moving
                                                  ``previous`` to trash with ``reason``
    Bucket._blob_rows(conn) -> Iterator[BlobRow]  every blobs row (for verify)
    Bucket._blob_row(conn, blob_id) -> BlobRow | None
    REASON_OVERWRITTEN, REASON_DELETED, DAY_MS
"""

from __future__ import annotations

import io
import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from . import blobs as _blobs
from . import keys as _keys
from .codec import BLOB_COLUMNS, CHUNK, BlobReader, BlobRow, Staged, check_blob, stage
from .errors import (
    BucketNotFound,
    ContentMismatch,
    DamagedError,
    InvalidArgument,
    MyBucketError,
    ObjectNotFound,
    PreconditionFailed,
    ReadOnlyError,
    StoreError,
    from_os_error,
)
from .models import (
    STATUS_CREATED,
    STATUS_UNCHANGED,
    STATUS_UPDATED,
    BucketStats,
    ListResult,
    ObjectInfo,
    PutInfo,
    VersionInfo,
)
from .schema import DB_NAME, check_db, connect, translate_sqlite_error

if TYPE_CHECKING:
    from .store import Store

REASON_OVERWRITTEN = "overwritten"
REASON_DELETED = "deleted"
DAY_MS = 86_400_000
_LIST_BATCH = 1000

#: Test hook: called with a point name ("staged", "after_install",
#: "before_commit", "after_commit"). Crash tests set it to os._exit.
_hook = None


def _crash_point(name: str) -> None:
    if _hook is not None:
        _hook(name)


class _Current(NamedTuple):
    key: str
    version: int
    blob_id: int
    content_type: str | None
    meta: str | None
    modified: int
    hash: bytes
    size: int
    stored_size: int

    def info(self) -> ObjectInfo:
        return ObjectInfo(
            key=self.key,
            etag=self.hash.hex(),
            size=self.size,
            content_type=self.content_type,
            metadata=_keys.parse_metadata(self.meta),
            modified=self.modified,
            version=self.version,
            stored_size=self.stored_size,
        )


_CURRENT_SQL = (
    "SELECT o.key, o.version, o.blob_id, o.content_type, o.meta, o.modified, b.hash, b.size, b.stored_size "
    "FROM objects o CROSS JOIN blobs b ON b.id = o.blob_id"
)


class _Txn:
    __slots__ = ("conn", "installed", "now")

    def __init__(self, conn: sqlite3.Connection, now: int) -> None:
        self.conn = conn
        self.installed: list[str] = []
        self.now = now


class ObjectStream(io.BufferedReader):
    """A readable binary stream of one object. ``.info`` describes the object.

    The content is checked against its size and SHA-256 as it is read, but
    the check can only conclude at the end: reaching the end of damaged
    content raises DamagedError instead of returning EOF. Bytes returned by a
    partial read (for example ``stream.read(1000)`` on a large object) are not
    verified yet. ``verified`` becomes True once the end was reached and every
    byte matched; use ``get()``, or read to the end, when you need checked data.
    """

    def __init__(self, raw: BlobReader, info: ObjectInfo) -> None:
        super().__init__(raw, buffer_size=CHUNK)
        self.info = info

    @property
    def verified(self) -> bool:
        """True once the whole object was read and matched its size and SHA-256."""
        return bool(getattr(self.raw, "verified", False))


def _successor(text: str) -> str:
    """The smallest string greater than every string starting with ``text``."""
    return text[:-1] + chr(ord(text[-1]) + 1)


class Bucket:
    def __init__(self, store: Store, name: str, path: Path, readonly: bool, timeout: float) -> None:
        self.store = store
        self.name = name
        self.path = Path(path)
        self.readonly = readonly
        self.db_path = self.path / DB_NAME
        self.blobs_dir = self.path / _blobs.BLOBS_DIR
        self.tmp_dir = self.path / _blobs.TMP_DIR
        self._lock = threading.RLock()
        self._local = threading.local()  # per-thread state (the open batch)
        self._conn: sqlite3.Connection | None = connect(self.db_path, readonly=readonly, timeout=timeout)
        try:
            check_db(self._conn)
            self._identity = self._file_identity()
            if self._identity is None or self._is_marked_deleted(self._conn):
                raise BucketNotFound(f"Bucket {name!r} does not exist in the store at {store.path}.")
        except BaseException:
            self._conn.close()
            raise

    # -- plumbing ----------------------------------------------------------

    def __repr__(self) -> str:
        mode = "ro" if self.readonly else "rw"
        return f"<Bucket {self.name!r} {mode}>"

    def __enter__(self) -> Bucket:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise StoreError(f"Bucket {self.name!r} is closed.", hint="Open it again with store.bucket(name).")
        return self._conn

    # -- is this handle still about the live bucket? ------------------------

    def _file_identity(self) -> tuple[int, int] | None:
        try:
            st = os.stat(self.db_path)
        except FileNotFoundError:
            return None
        return st.st_dev, st.st_ino

    @staticmethod
    def _is_marked_deleted(conn: sqlite3.Connection) -> bool:
        return conn.execute("SELECT 1 FROM meta WHERE k = 'deleted'").fetchone() is not None

    def _alive(self) -> bool:
        """True while the database file at this bucket's path is the one this handle opened."""
        return self._conn is not None and self._file_identity() == self._identity

    def _gone(self) -> BucketNotFound:
        return BucketNotFound(
            f"Bucket {self.name!r} was deleted or replaced by another process, so this handle "
            "can no longer be used; nothing was changed.",
            hint="Open the bucket again (store.bucket(name)) if it should still exist.",
        )

    def _check_alive(self) -> None:
        if self._file_identity() != self._identity:
            raise self._gone()

    def _now(self) -> int:
        return self.store._now()

    def _require_writable(self) -> None:
        if self.readonly:
            raise ReadOnlyError(f"Cannot change bucket {self.name!r}: the store is open read-only.")

    @contextmanager
    def _locked_conn(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connection()
            self._check_alive()
            yield conn

    @contextmanager
    def _read_txn(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connection()
            self._check_alive()
            try:
                conn.execute("BEGIN")
            except sqlite3.Error as exc:
                raise translate_sqlite_error(exc, f"read bucket {self.name!r}") from exc
            try:
                yield conn
            finally:
                try:
                    conn.execute("COMMIT")
                except sqlite3.Error:
                    pass

    @contextmanager
    def _write_txn(self, what: str) -> Iterator[_Txn]:
        self._require_writable()
        with self._lock:
            conn = self._connection()
            try:
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.Error as exc:
                raise translate_sqlite_error(exc, what) from exc
            txn = _Txn(conn, self._now())
            try:
                # With the write lock held: is this still the live bucket? (A
                # delete_bucket marks the database, then moves it aside.)
                if self._file_identity() != self._identity or self._is_marked_deleted(conn):
                    raise self._gone()
                yield txn
                _crash_point("before_commit")
                conn.execute("COMMIT")
            except BaseException as exc:
                self._abort(txn)
                if isinstance(exc, sqlite3.Error):
                    raise translate_sqlite_error(exc, what) from exc
                if isinstance(exc, OSError):
                    raise from_os_error(exc, what) from exc
                raise
            _crash_point("after_commit")

    def _abort(self, txn: _Txn) -> None:
        conn = txn.conn
        if conn.in_transaction:
            # Still holding the write lock: files this transaction installed have
            # no committed row, so removing them now cannot race with anyone.
            for hex_hash in txn.installed:
                try:
                    _blobs.remove_blob_file(self.path, hex_hash)
                except OSError:
                    pass  # left as an orphan; gc removes it later
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass

    # -- row helpers -------------------------------------------------------

    def _current(self, conn: sqlite3.Connection, key: str) -> _Current | None:
        row = conn.execute(_CURRENT_SQL + " WHERE o.key = ?", (key,)).fetchone()
        return _Current(*row) if row else None

    def _trash_entry(self, conn: sqlite3.Connection, key: str, version: int) -> _Current | None:
        row = conn.execute(
            "SELECT t.key, t.version, t.blob_id, t.content_type, t.meta, t.modified, b.hash, b.size, b.stored_size "
            "FROM trash t CROSS JOIN blobs b ON b.id = t.blob_id WHERE t.key = ? AND t.version = ?",
            (key, version),
        ).fetchone()
        return _Current(*row) if row else None

    def _blob_row(self, conn: sqlite3.Connection, blob_id: int) -> BlobRow | None:
        row = conn.execute(f"SELECT {BLOB_COLUMNS} FROM blobs WHERE id = ?", (blob_id,)).fetchone()
        return BlobRow(*row) if row else None

    def _blob_rows(self, conn: sqlite3.Connection) -> Iterator[BlobRow]:
        for row in conn.execute(f"SELECT {BLOB_COLUMNS} FROM blobs ORDER BY id"):
            yield BlobRow(*row)

    def _meta_value(self, conn: sqlite3.Connection, name: str):
        row = conn.execute("SELECT v FROM meta WHERE k = ?", (name,)).fetchone()
        return None if row is None else row[0]

    def _next_version(self, conn: sqlite3.Connection) -> int:
        version = int(self._meta_value(conn, "last_version")) + 1
        conn.execute("UPDATE meta SET v = ? WHERE k = 'last_version'", (version,))
        return version

    def _move_to_trash(self, txn: _Txn, previous: _Current, reason: str) -> None:
        days = int(self._meta_value(txn.conn, "trash_days"))
        if days <= 0:
            return
        txn.conn.execute(
            "INSERT INTO trash (key, version, blob_id, content_type, meta, modified, trashed_at, expires_at, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                previous.key,
                previous.version,
                previous.blob_id,
                previous.content_type,
                previous.meta,
                previous.modified,
                txn.now,
                txn.now + days * DAY_MS,
                reason,
            ),
        )

    def _set_current(
        self,
        txn: _Txn,
        key: str,
        blob_id: int,
        content_type: str | None,
        meta_json: str | None,
        previous: _Current | None,
        reason: str = REASON_OVERWRITTEN,
    ) -> int:
        version = self._next_version(txn.conn)
        if previous is not None:
            self._move_to_trash(txn, previous, reason)
        txn.conn.execute(
            "INSERT INTO objects (key, version, blob_id, content_type, meta, modified) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (key) DO UPDATE SET version = excluded.version, blob_id = excluded.blob_id, "
            "content_type = excluded.content_type, meta = excluded.meta, modified = excluded.modified",
            (key, version, blob_id, content_type, meta_json, txn.now),
        )
        return version

    def _ensure_blob(self, txn: _Txn, staged: Staged) -> tuple[int, int]:
        """Return (blob_id, stored_size) for staged content, creating the row if needed.

        When ``staged.heal`` is set (the stored copy was found damaged before
        the lock), the existing row's stored bytes are replaced by the staged ones.
        """
        conn = txn.conn
        row = conn.execute(
            "SELECT id, stored_size, data IS NULL FROM blobs WHERE hash = ?", (staged.hash,)
        ).fetchone()
        if row is not None:
            if staged.heal:
                return self._heal_blob(txn, staged, row[0], bool(row[2]))
            return row[0], row[1]
        staged.heal = False  # nothing to repair: the row is gone, a fresh one is made
        staged.prepare()  # normally done before the lock; here only if gc raced us
        if staged.inline is not None:
            data: bytes | None = staged.inline
        else:
            assert staged.tmp is not None
            _blobs.install(staged.tmp, self.path, staged.hex)
            txn.installed.append(staged.hex)
            _crash_point("after_install")
            data = None
        cur = conn.execute(
            "INSERT INTO blobs (hash, size, stored_size, codec, dict_id, data, created_at) "
            "VALUES (?, ?, ?, ?, NULL, ?, ?)",
            (staged.hash, staged.size, staged.stored_size, staged.codec, data, txn.now),
        )
        return cur.lastrowid, staged.stored_size  # type: ignore[return-value]

    def _heal_blob(self, txn: _Txn, staged: Staged, blob_id: int, in_file: bool) -> tuple[int, int]:
        """Replace the damaged stored bytes of ``blob_id`` by the staged (hash-proven) ones."""
        staged.prepare()
        conn = txn.conn
        if in_file or staged.tmp is not None:
            tmp = staged.stored_bytes_file()
            try:
                conn.execute(
                    "UPDATE blobs SET size = ?, stored_size = ?, codec = ?, dict_id = NULL, data = NULL "
                    "WHERE id = ?",
                    (staged.size, staged.stored_size, staged.codec, blob_id),
                )
                # Last step: the file is replaced by bytes with the same content. It is
                # not listed in txn.installed, so an abort never removes it (the row
                # keeps its name either way).
                _blobs.install(tmp, self.path, staged.hex)
            finally:
                tmp.discard()
        else:
            conn.execute(
                "UPDATE blobs SET size = ?, stored_size = ?, codec = ?, dict_id = NULL, data = ? WHERE id = ?",
                (staged.size, staged.stored_size, staged.codec, staged.inline, blob_id),
            )
        staged.repaired = True
        staged.heal = False
        return blob_id, staged.stored_size  # type: ignore[return-value]

    def _existing_blob(self, digest: bytes) -> BlobRow | None:
        with self._locked_conn() as conn:
            try:
                row = conn.execute(f"SELECT {BLOB_COLUMNS} FROM blobs WHERE hash = ?", (digest,)).fetchone()
            except sqlite3.Error as exc:
                raise translate_sqlite_error(exc, f"read bucket {self.name!r}") from exc
        return BlobRow(*row) if row else None

    def _stored_copy_damaged(self, digest: bytes) -> bool:
        """True when this content is stored but its stored bytes do not read back exactly."""
        row = self._existing_blob(digest)
        return row is not None and check_blob(self.path, row) is not None

    def _prepare_outside_lock(self, staged: Staged) -> None:
        """Compress new content before taking the lock; re-check content the bucket already has.

        The re-check is what lets a put repair a damaged stored copy, and it
        means a put never reports success for content that cannot be read back.
        """
        row = self._existing_blob(staged.hash)
        if row is None:
            staged.prepare()
        elif check_blob(self.path, row) is not None:
            staged.heal = True
            staged.prepare()

    # -- preconditions -----------------------------------------------------

    @staticmethod
    def _check_conditions(key: str, current: _Current | None, if_match: str | None, if_none_match: str | None) -> None:
        if if_none_match is not None and current is not None:
            raise PreconditionFailed(
                f"Object {key!r} already exists, so the write with if_none_match='*' was not applied.",
                hint=PreconditionFailed.HINT_EXISTS,
            )
        if if_match is not None:
            if current is None:
                raise PreconditionFailed(
                    f"Object {key!r} does not exist, so the write with if_match was not applied.",
                    hint=PreconditionFailed.HINT_MISSING,
                )
            if if_match != "*" and if_match != current.hash.hex():
                raise PreconditionFailed(
                    f"Object {key!r} has changed (its ETag is {current.hash.hex()}, "
                    f"not {if_match}), so the write was not applied."
                )

    @staticmethod
    def _normalize_conditions(if_match: object, if_none_match: object) -> tuple[str | None, str | None]:
        match = None if if_match is None else _keys.normalize_etag(if_match)
        none_match = None
        if if_none_match is not None:
            if not isinstance(if_none_match, str) or if_none_match.strip().strip('"') != "*":
                raise InvalidArgument(
                    f"if_none_match must be '*', got {if_none_match!r}.",
                    hint="Use if_none_match='*' to write only when the key does not exist yet.",
                )
            none_match = "*"
        return match, none_match

    # -- apply (inside a write transaction) --------------------------------

    def _apply_put(
        self,
        txn: _Txn,
        key: str,
        staged: Staged,
        content_type: str | None,
        meta_json: str | None,
        if_match: str | None,
        if_none_match: str | None,
    ) -> PutInfo:
        current = self._current(txn.conn, key)
        self._check_conditions(key, current, if_match, if_none_match)
        if (
            current is not None
            and current.hash == staged.hash
            and current.content_type == content_type
            and current.meta == meta_json
        ):
            if staged.heal:
                _blob_id, stored_size = self._ensure_blob(txn, staged)
                info = current.info().__dict__ | {"stored_size": stored_size}
                return PutInfo(**info, status=STATUS_UNCHANGED, repaired=True)
            return PutInfo(**current.info().__dict__, status=STATUS_UNCHANGED)
        blob_id, stored_size = self._ensure_blob(txn, staged)
        version = self._set_current(txn, key, blob_id, content_type, meta_json, current, REASON_OVERWRITTEN)
        return PutInfo(
            key=key,
            etag=staged.hex,
            size=staged.size,
            content_type=content_type,
            metadata=_keys.parse_metadata(meta_json),
            modified=txn.now,
            version=version,
            stored_size=stored_size,
            status=STATUS_CREATED if current is None else STATUS_UPDATED,
            repaired=staged.repaired,
        )

    def _apply_delete(self, txn: _Txn, key: str, if_match: str | None, missing_ok: bool = False) -> ObjectInfo | None:
        current = self._current(txn.conn, key)
        if current is None:
            if missing_ok and if_match is None:
                return None
            raise ObjectNotFound(
                f"Object {key!r} does not exist in bucket {self.name!r}.",
                hint="List the bucket to see which keys exist. To ignore missing keys, pass missing_ok=True.",
            )
        self._check_conditions(key, current, if_match, None)
        self._move_to_trash(txn, current, REASON_DELETED)
        txn.conn.execute("DELETE FROM objects WHERE key = ?", (key,))
        return current.info()

    # -- public API: writes ------------------------------------------------

    def put(
        self,
        key: str,
        data,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        expected_size: int | None = None,
        expected_sha256: str | None = None,
    ) -> PutInfo:
        """Store ``data`` (bytes, binary file or iterator of bytes) under ``key``.

        Atomic. Returns a PutInfo whose ``status`` is "created", "updated" or
        "unchanged" (identical content, content type and metadata: nothing
        was written). ``if_match=etag`` writes only if the current ETag
        matches; ``if_none_match="*"`` writes only if the key does not exist.
        Both are checked inside the write transaction.

        ``expected_size`` / ``expected_sha256``: when given, the data read
        must have exactly that size / SHA-256 (hex), otherwise nothing is
        written and ContentMismatch is raised. Use them when the data comes
        from a pipe or a download that could stop early.

        If the bucket already stores this content but its stored copy is
        damaged, the put replaces the stored copy (``info.repaired`` is True).
        """
        self._require_writable()
        key = _keys.validate_key(key)
        content_type = _keys.validate_content_type(content_type)
        meta_json = _keys.validate_metadata(metadata)
        if_match, if_none_match = self._normalize_conditions(if_match, if_none_match)
        expected = _normalize_expected(expected_size, expected_sha256)
        self._connection()
        self._check_alive()
        what = f"write {key!r}"
        try:
            staged = stage(data, self.tmp_dir)
        except OSError as exc:
            raise from_os_error(exc, what) from exc
        try:
            _check_expected(key, staged, *expected)
            _crash_point("staged")
            try:
                self._prepare_outside_lock(staged)
            except OSError as exc:
                raise from_os_error(exc, what) from exc
            with self._write_txn(what) as txn:
                return self._apply_put(txn, key, staged, content_type, meta_json, if_match, if_none_match)
        finally:
            staged.discard()

    def delete(self, key: str, if_match: str | None = None, missing_ok: bool = False) -> ObjectInfo | None:
        """Delete ``key``. The deleted version stays in the trash for ``trash_days``.

        Returns the deleted version's ObjectInfo. Unlike S3, deleting a key
        that does not exist raises ObjectNotFound, unless ``missing_ok=True``
        (then it returns None). PreconditionFailed when ``if_match`` does not
        match the current ETag (a missing key never matches).
        """
        self._require_writable()
        key = _keys.validate_key(key)
        if_match, _ = self._normalize_conditions(if_match, None)
        with self._write_txn(f"delete {key!r}") as txn:
            return self._apply_delete(txn, key, if_match, missing_ok)

    def batch(self) -> Batch:
        """A context manager whose puts and deletes are committed in one transaction.

        Inputs are read and compressed as each call is made; nothing is visible
        to anyone until the ``with`` block ends without an exception, and then
        either every operation applies or none does. Reads made inside the
        block (get, head, exists, list) do not see the batch's pending writes.
        ``tx.put()`` and ``tx.delete()`` return nothing; after the block,
        ``batch.results`` holds one PutInfo or ObjectInfo (None for a
        ``missing_ok`` delete of a missing key) per operation. Batches do not
        nest: opening a second batch on the same bucket inside the first one
        raises StoreError.
        """
        self._require_writable()
        return Batch(self)

    # -- public API: maintenance and transfer (thin wrappers) ----------------

    def restore(self, key: str, version: int) -> PutInfo:
        """Make ``version`` of ``key`` (kept in the trash) current again; see maintenance.restore."""
        from .maintenance import restore

        return restore(self, key, version)

    def gc(self, **kwargs) -> dict:
        """Remove expired trash and unused stored content; see maintenance.gc."""
        from .maintenance import gc

        return gc(self, **kwargs)

    def verify(self) -> dict:
        """Re-read and re-hash everything; see maintenance.verify. Changes nothing."""
        from .maintenance import verify

        return verify(self)

    def export_dir(self, dest, prefix: str = "", overwrite: bool = False, skip_conflicts: bool = False) -> dict:
        """Write every object under ``prefix`` as a plain file at its key path; see transfer.export_dir."""
        from .transfer import export_dir

        return export_dir(self, dest, prefix=prefix, overwrite=overwrite, skip_conflicts=skip_conflicts)

    def import_dir(self, src, prefix: str = "", guess_types: bool = True) -> dict:
        """Store every regular file under ``src``; see transfer.import_dir."""
        from .transfer import import_dir

        return import_dir(self, src, prefix=prefix, guess_types=guess_types)

    def configure(self, trash_days: int | None = None) -> None:
        """Change bucket settings. ``trash_days``: how long old versions are kept (0 = not kept)."""
        if trash_days is None:
            return
        if isinstance(trash_days, bool) or not isinstance(trash_days, int) or not 0 <= trash_days <= 36500:
            raise InvalidArgument(
                f"trash_days must be a whole number of days between 0 and 36500, got {trash_days!r}.",
                hint="For example configure(trash_days=30); 0 turns the trash off.",
            )
        with self._write_txn(f"configure bucket {self.name!r}") as txn:
            txn.conn.execute("UPDATE meta SET v = ? WHERE k = 'trash_days'", (trash_days,))

    @property
    def trash_days(self) -> int:
        with self._locked_conn() as conn:
            return int(self._meta_value(conn, "trash_days"))

    # -- public API: reads -------------------------------------------------

    def _open_reader(self, key: str) -> tuple[ObjectInfo, BlobReader]:
        key = _keys.validate_key(key)
        for _attempt in range(3):
            try:
                with self._read_txn() as conn:  # one snapshot for both rows
                    current = self._current(conn, key)
                    row = None if current is None else self._blob_row(conn, current.blob_id)
            except sqlite3.Error as exc:
                raise translate_sqlite_error(exc, f"read {key!r}") from exc
            if current is None or row is None:
                raise ObjectNotFound(f"Object {key!r} does not exist in bucket {self.name!r}.")
            try:
                return current.info(), BlobReader(self.path, row, label=repr(key))
            except FileNotFoundError:
                continue  # gc may have removed it after our snapshot; look again
        raise DamagedError(f"Object {key!r} is damaged: its blob file is missing.")

    def get(self, key: str) -> bytes:
        """Return the whole content of ``key``. Raises ObjectNotFound or DamagedError."""
        _info, reader = self._open_reader(key)
        with reader:
            return reader.readall()

    def open(self, key: str) -> ObjectStream:
        """Open ``key`` for streaming reads (use as a context manager)."""
        info, reader = self._open_reader(key)
        return ObjectStream(reader, info)

    def head(self, key: str) -> ObjectInfo:
        """Describe ``key`` without reading its content. Raises ObjectNotFound."""
        key = _keys.validate_key(key)
        with self._locked_conn() as conn:
            try:
                current = self._current(conn, key)
            except sqlite3.Error as exc:
                raise translate_sqlite_error(exc, f"read {key!r}") from exc
        if current is None:
            raise ObjectNotFound(f"Object {key!r} does not exist in bucket {self.name!r}.")
        return current.info()

    def exists(self, key: str) -> bool:
        key = _keys.validate_key(key)
        with self._locked_conn() as conn:
            try:
                return conn.execute("SELECT 1 FROM objects WHERE key = ?", (key,)).fetchone() is not None
            except sqlite3.Error as exc:
                raise translate_sqlite_error(exc, f"read {key!r}") from exc

    def list(
        self,
        prefix: str = "",
        delimiter: str | None = None,
        limit: int | None = None,
        start_after: str | None = None,
    ) -> ListResult:
        """List current objects in key order (S3 style).

        With ``delimiter``, keys that contain the delimiter after ``prefix`` are
        rolled up into common prefixes (each counted once toward ``limit``).
        When the result is truncated, pass ``next_start_after`` as
        ``start_after`` to continue. ``start_after`` may also be any key:
        listing starts strictly after it, and a common prefix that still has
        keys after it is listed (as S3 does). A ``start_after`` equal to a
        common prefix skips everything under that prefix.
        """
        prefix = _keys.validate_prefix(prefix)
        if delimiter is not None:
            delimiter = _keys.validate_delimiter(delimiter)
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
            raise InvalidArgument(
                f"limit must be a positive whole number, got {limit!r}.",
                hint="Leave it out (None) to list everything.",
            )
        if start_after is not None:
            start_after = _keys.validate_prefix(start_after)

        upper = _successor(prefix) if prefix else None
        cursor, inclusive = prefix, True
        if start_after is not None and start_after >= prefix:
            cursor, inclusive = start_after, False
            if delimiter is not None and start_after.startswith(prefix):
                i = start_after.find(delimiter, len(prefix))
                if i >= 0 and i + len(delimiter) == len(start_after):
                    # start_after is itself a common prefix (the cursor list()
                    # returns after one): everything under it was already listed.
                    # Any other start_after inside a common prefix only skips the
                    # keys up to it; the prefix is listed again if keys follow.
                    cursor, inclusive = _successor(start_after), True

        objects: list[ObjectInfo] = []
        prefixes: list[str] = []
        count = 0
        truncated = False
        last: str | None = None
        try:
            with self._read_txn() as conn:
                while not truncated:
                    want = _LIST_BATCH if limit is None else min(_LIST_BATCH, limit - count + 1)
                    sql = _CURRENT_SQL + (" WHERE o.key >= ?" if inclusive else " WHERE o.key > ?")
                    params: list = [cursor]
                    if upper is not None:
                        sql += " AND o.key < ?"
                        params.append(upper)
                    sql += " ORDER BY o.key LIMIT ?"
                    params.append(want)
                    rows = conn.execute(sql, params).fetchall()
                    if not rows:
                        break
                    jumped = False
                    for row in rows:
                        if limit is not None and count >= limit:
                            truncated = True
                            break
                        key = row[0]
                        if delimiter is not None:
                            i = key.find(delimiter, len(prefix))
                            if i >= 0:
                                common = key[: i + len(delimiter)]
                                prefixes.append(common)
                                count += 1
                                last = common
                                cursor, inclusive = _successor(common), True
                                jumped = True
                                break
                        objects.append(_Current(*row).info())
                        count += 1
                        last = key
                        cursor, inclusive = key, False
                    if not truncated and not jumped and len(rows) < want:
                        break
        except sqlite3.Error as exc:
            raise translate_sqlite_error(exc, f"list bucket {self.name!r}") from exc
        return ListResult(
            objects=objects,
            prefixes=prefixes,
            truncated=truncated,
            next_start_after=last if truncated else None,
        )

    def list_versions(self, key: str) -> list[VersionInfo]:
        """The current version (if any) and every version kept in the trash, newest first.

        Raises ObjectNotFound when the key has neither.
        """
        key = _keys.validate_key(key)
        out: list[VersionInfo] = []
        try:
            with self._read_txn() as conn:
                current = self._current(conn, key)
                if current is not None:
                    out.append(
                        VersionInfo(
                            key=key,
                            version=current.version,
                            etag=current.hash.hex(),
                            size=current.size,
                            content_type=current.content_type,
                            metadata=_keys.parse_metadata(current.meta),
                            modified=current.modified,
                            current=True,
                        )
                    )
                rows = conn.execute(
                    "SELECT t.version, b.hash, b.size, t.content_type, t.meta, t.modified, t.trashed_at, "
                    "t.expires_at, t.reason FROM trash t CROSS JOIN blobs b ON b.id = t.blob_id "
                    "WHERE t.key = ? ORDER BY t.version DESC",
                    (key,),
                ).fetchall()
        except sqlite3.Error as exc:
            raise translate_sqlite_error(exc, f"read versions of {key!r}") from exc
        for version, digest, size, ctype, meta, modified, trashed_at, expires_at, reason in rows:
            out.append(
                VersionInfo(
                    key=key,
                    version=version,
                    etag=digest.hex(),
                    size=size,
                    content_type=ctype,
                    metadata=_keys.parse_metadata(meta),
                    modified=modified,
                    current=False,
                    trashed_at=trashed_at,
                    expires_at=expires_at,
                    reason=reason,
                )
            )
        if not out:
            raise ObjectNotFound(f"Object {key!r} has no current or trashed versions in bucket {self.name!r}.")
        out.sort(key=lambda v: v.version, reverse=True)
        return out

    def stats(self) -> BucketStats:
        try:
            with self._read_txn() as conn:
                objects, logical = conn.execute(
                    "SELECT count(*), coalesce(sum(b.size), 0) FROM objects o CROSS JOIN blobs b ON b.id = o.blob_id"
                ).fetchone()
                trash_n, trash_logical = conn.execute(
                    "SELECT count(*), coalesce(sum(b.size), 0) FROM trash t CROSS JOIN blobs b ON b.id = t.blob_id"
                ).fetchone()
                n_blobs, blob_bytes, stored, inline_n, file_bytes = conn.execute(
                    "SELECT count(*), coalesce(sum(size), 0), coalesce(sum(stored_size), 0), "
                    "coalesce(sum(data IS NOT NULL), 0), coalesce(sum(CASE WHEN data IS NULL THEN stored_size END), 0) "
                    "FROM blobs"
                ).fetchone()
                trash_days = int(self._meta_value(conn, "trash_days"))
        except sqlite3.Error as exc:
            raise translate_sqlite_error(exc, f"read bucket {self.name!r}") from exc
        db_bytes = 0
        for suffix in ("", "-wal"):
            try:
                db_bytes += os.path.getsize(str(self.db_path) + suffix)
            except OSError:
                pass
        return BucketStats(
            bucket=self.name,
            objects=objects,
            logical_bytes=logical,
            trash_versions=trash_n,
            trash_logical_bytes=trash_logical,
            blobs=n_blobs,
            blob_bytes=blob_bytes,
            stored_bytes=stored,
            inline_blobs=inline_n,
            file_blobs=n_blobs - inline_n,
            database_bytes=db_bytes,
            file_bytes=file_bytes,
            trash_days=trash_days,
        )


class Batch:
    """Collects puts and deletes and commits them atomically on exit. See Bucket.batch()."""

    def __init__(self, bucket: Bucket) -> None:
        self._bucket = bucket
        self._ops: list[tuple] = []
        self._state = "new"
        self.results: list[ObjectInfo] | None = None

    def __enter__(self) -> Batch:
        if self._state != "new":
            raise StoreError("This batch was already used.", hint="Start a new one with bucket.batch().")
        local = self._bucket._local
        if getattr(local, "batch", None) is not None:
            raise StoreError(
                f"A batch is already open on bucket {self._bucket.name!r} in this thread.",
                hint="Put these writes in the outer batch; batches do not nest.",
            )
        local.batch = self
        self._state = "open"
        return self

    def _release(self) -> None:
        local = self._bucket._local
        if getattr(local, "batch", None) is self:
            local.batch = None

    def _require_open(self) -> None:
        if self._state != "open":
            raise StoreError(
                "Batch operations must be made inside its 'with' block.",
                hint="Write: with bucket.batch() as b: b.put(...)",
            )

    def put(
        self,
        key: str,
        data,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        expected_size: int | None = None,
        expected_sha256: str | None = None,
    ) -> None:
        """Queue a put; same arguments as Bucket.put(). The result is in ``results`` after the block."""
        self._require_open()
        key = _keys.validate_key(key)
        content_type = _keys.validate_content_type(content_type)
        meta_json = _keys.validate_metadata(metadata)
        if_match, if_none_match = self._bucket._normalize_conditions(if_match, if_none_match)
        expected = _normalize_expected(expected_size, expected_sha256)
        what = f"write {key!r}"
        try:
            staged = stage(data, self._bucket.tmp_dir)
        except OSError as exc:
            raise from_os_error(exc, what) from exc
        try:
            _check_expected(key, staged, *expected)
            self._bucket._prepare_outside_lock(staged)
        except OSError as exc:
            staged.discard()  # this call failed: it is not part of the batch
            raise from_os_error(exc, what) from exc
        except BaseException:
            staged.discard()
            raise
        self._ops.append(("put", key, staged, content_type, meta_json, if_match, if_none_match))

    def delete(self, key: str, if_match: str | None = None, missing_ok: bool = False) -> None:
        """Queue a delete; same arguments as Bucket.delete()."""
        self._require_open()
        key = _keys.validate_key(key)
        if_match, _ = self._bucket._normalize_conditions(if_match, None)
        self._ops.append(("delete", key, if_match, missing_ok))

    def _discard(self) -> None:
        for op in self._ops:
            if op[0] == "put":
                op[2].discard()

    def __exit__(self, exc_type, exc, tb) -> None:
        self._state = "done"
        self._release()
        try:
            if exc_type is not None or not self._ops:
                self.results = [] if exc_type is None else None
                return
            bucket = self._bucket
            results: list[ObjectInfo] = []
            with bucket._write_txn(f"commit a batch of {len(self._ops)} operations") as txn:
                for op in self._ops:
                    if op[0] == "put":
                        _, key, staged, ctype, meta_json, if_match, if_none_match = op
                        results.append(bucket._apply_put(txn, key, staged, ctype, meta_json, if_match, if_none_match))
                    else:
                        _, key, if_match, missing_ok = op
                        results.append(bucket._apply_delete(txn, key, if_match, missing_ok))
            self.results = results
        except MyBucketError as error:
            if not error.message.endswith("No operation in the batch was applied."):
                error.message = f"{error.message} No operation in the batch was applied."
            raise
        finally:
            self._discard()


def _normalize_expected(size: object, sha256: object) -> tuple[int | None, str | None]:
    if size is not None and (isinstance(size, bool) or not isinstance(size, int) or size < 0):
        raise InvalidArgument(
            f"expected_size must be a whole number of bytes >= 0, got {size!r}.",
            hint="Pass the exact length of the data, or leave it out.",
        )
    digest = None if sha256 is None else _keys.normalize_etag(sha256, allow_star=False)
    return size, digest


def _check_expected(key: str, staged: Staged, size: int | None, sha256: str | None) -> None:
    if size is not None and staged.size != size:
        raise ContentMismatch(
            f"The data read for {key!r} is {staged.size} bytes, not the {size} bytes expected, "
            "so nothing was written."
        )
    if sha256 is not None and staged.hex != sha256:
        raise ContentMismatch(
            f"The data read for {key!r} has SHA-256 {staged.hex}, not the {sha256} expected, "
            "so nothing was written."
        )
