"""Maintenance: garbage collection, verification, snapshots and restore.

Public:
    gc(bucket, now=None, orphan_grace=600.0, tmp_min_age=3600.0) -> dict
    gc_store(store, now=None, ...) -> dict          gc of every bucket + leftover dirs
    verify(bucket) -> dict                          read-only re-hash of everything
    verify_store(store) -> dict
    snapshot(store, dest, buckets=None) -> dict     consistent copy while writers run
    restore(bucket, key, version) -> PutInfo        bring a trashed version back

Every function returns plain dicts (JSON-ready) with a ``summary`` sentence
that says what happened in plain language, so the CLI can print them as is.

Why gc can never delete data a writer is about to use
-----------------------------------------------------

The engine guarantees (blobs.py, bucket.py):

E1. A blob file reaches its final path only through ``blobs.install()``,
    which a writer calls while holding the bucket write lock (inside its
    ``BEGIN IMMEDIATE`` transaction), immediately before inserting the
    ``blobs`` row in that same transaction.
E2. A writer never relies on a blob row or file it saw *outside* its write
    transaction: inside the transaction it looks the hash up again and, if
    the row is gone, re-creates the row (and re-installs the file from its
    own temp file). A blob row and the objects/trash row that references it
    are always created in the same transaction.
E3. In-flight data lives in ``tmp/*.part`` and the writer holds an exclusive
    ``flock`` on it until it is installed or discarded.

gc works in this order:

1. Under the write lock (transaction 1): delete expired trash rows, then
   delete every blob row nothing references. While gc holds the lock no
   writer is between ``install()`` and ``COMMIT`` (E1), so "unreferenced"
   really means garbage. A writer that saw such a row before gc ran simply
   re-creates it (E2). The transaction commits *before* any file is touched,
   so a crash here can at most leave files without rows, never rows without
   files.
2. Without the lock: walk ``blobs/`` and note files whose hash has no row.
   This list is only a set of candidates.
3. Under the write lock again (transaction 2): for every candidate, check
   once more that no ``blobs`` row with that hash exists, and only then
   unlink it. A writer that installed a file between steps 2 and 3 has
   committed its row by now (it held the lock; we waited for it), so the
   re-check keeps its file. A writer that has not reached ``install()`` yet
   cannot until we release the lock, and it then installs its own copy.
   Holding the lock while unlinking is what blobs.py invariant 2 requires.
4. Temp files are removed only when ``blobs.tmp_is_stale()`` says so: old
   *and* no process holds their flock (E3), so a file still being written is
   never touched, however slow the writer. "Old" is never less than
   ``MIN_TMP_AGE`` (60 s), whatever the caller asks: ``TempFile`` creates
   the file a moment before it takes the flock, and a brand-new file seen in
   that moment must not be mistaken for an abandoned one. Its mtime is
   fresh, so the age floor covers that gap.

Belt and braces: crash orphans of unknown origin (step 2 candidates whose
rows were not deleted by this run) are also left alone until their file is
``orphan_grace`` seconds old (default 10 minutes). The argument above does
not need it; it only protects against a writer that breaks E1 (for example a
buggy or foreign tool) at the cost of an orphan lingering a few minutes.
Files whose rows gc itself just deleted are removed at once.

Readers are not affected either: a reader whose snapshot still shows a row
whose file gc removed retries with a fresh snapshot (bucket.py).

Why a snapshot is consistent
----------------------------

The database is copied with SQLite's online backup API in one step, which
reads from a single committed snapshot (time T) while writers continue in
WAL mode. Every file blob referenced in that copy existed at T (E1) and blob
files are immutable, so linking or copying the file found under the same
name later yields exactly the bytes the copy expects. The only way such a
file can be missing is that gc removed it after T (its version expired or
was unreferenced after T); then the snapshot is simply retried with a fresh
backup. The last attempt holds the bucket write lock (blocking gc and
writers for its duration) so the process always terminates; a file still
missing then is genuine damage in the source and is reported.

Hard-linked snapshots share disk blocks with the source. They protect
against mistakes and software bugs, not against disk failure: copy the
snapshot to another disk for a real backup. When hard links are impossible
(another filesystem, no permission) files are copied instead.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import time
from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING

from . import blobs as _blobs
from . import keys as _keys
from .bucket import REASON_OVERWRITTEN
from .codec import CHUNK, check_blob
from .errors import InvalidArgument, Refused, VersionNotFound
from .models import STATUS_CREATED, STATUS_UNCHANGED, STATUS_UPDATED, PutInfo
from .schema import DB_NAME, UNREFERENCED_BLOBS_SQL, connect, translate_sqlite_error

if TYPE_CHECKING:
    from .bucket import Bucket
    from .store import Store

#: Test hook: called with a point name ("gc_scanned"). Tests use it to act
#: between gc's unlocked scan and its locked re-check.
_hook = None

DEFAULT_ORPHAN_GRACE = 600.0
DEFAULT_TMP_MIN_AGE = 3600.0
#: Temp files younger than this are never removed (see step 4 in the module docstring).
MIN_TMP_AGE = 60.0
LEFTOVER_DIR_MIN_AGE = 3600.0
SNAPSHOT_ATTEMPTS = 4
_ID_CHUNK = 500


def _point(name: str) -> None:
    if _hook is not None:
        _hook(name)


def _plural(n: int, word: str, many: str | None = None) -> str:
    return f"{n} {word if n == 1 else (many or word + 's')}"


def _human_bytes(n: int) -> str:
    value = float(n)
    for unit in ("bytes", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{int(value)} bytes" if unit == "bytes" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{n} bytes"  # pragma: no cover


def _has_row(conn: sqlite3.Connection, digest: bytes) -> bool:
    return conn.execute("SELECT 1 FROM blobs WHERE hash = ?", (digest,)).fetchone() is not None


def _check_seconds(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise InvalidArgument(
            f"{name} must be a number of seconds >= 0, got {value!r}.", hint="Leave it out to use the default."
        )
    return float(value)


def _remove_empty_dirs(path: Path, stop: Path) -> None:
    """Remove ``path`` and its parents up to (not including) ``stop`` while they are empty."""
    while path != stop and stop in path.parents:
        try:
            path.rmdir()
        except OSError:
            return
        _blobs.fsync_dir(path.parent)
        path = path.parent


# -- gc ----------------------------------------------------------------------


def gc(
    bucket: Bucket,
    now: int | None = None,
    orphan_grace: float = DEFAULT_ORPHAN_GRACE,
    tmp_min_age: float = DEFAULT_TMP_MIN_AGE,
) -> dict:
    """Free space: expire old trash, drop unreferenced content, remove stale temp files.

    ``now`` (unix ms) overrides the store clock for deciding which trash
    versions have expired. ``orphan_grace`` (seconds) is how old a blob file
    with no row and unknown origin must be before it is removed;
    ``tmp_min_age`` (seconds) the same for abandoned temp files (values
    below ``MIN_TMP_AGE``, 60 s, count as 60 s). See the
    module docstring for why this is safe while other processes write.

    Raises ReadOnlyError on a read-only store and Busy when another writer
    holds the lock for longer than the store timeout.
    """
    bucket._require_writable()
    if now is not None and (isinstance(now, bool) or not isinstance(now, int)):
        raise InvalidArgument(
            f"now must be a unix time in milliseconds, got {now!r}.",
            hint="Leave it out to use the current time.",
        )
    orphan_grace = _check_seconds("orphan_grace", orphan_grace)
    tmp_min_age = max(MIN_TMP_AGE, _check_seconds("tmp_min_age", tmp_min_age))
    name = bucket.name

    # 1. Rows: expired trash, then content nothing references any more.
    with bucket._write_txn(f"clean up bucket {name!r}") as txn:
        conn = txn.conn
        cutoff = txn.now if now is None else now
        trash_expired = conn.execute("DELETE FROM trash WHERE expires_at <= ?", (cutoff,)).rowcount
        garbage = conn.execute(
            f"SELECT u.id, u.hash, u.in_file, b.stored_size FROM ({UNREFERENCED_BLOBS_SQL}) u "
            "JOIN blobs b ON b.id = u.id"
        ).fetchall()
        ids = [row[0] for row in garbage]
        for start in range(0, len(ids), _ID_CHUNK):
            part = ids[start : start + _ID_CHUNK]
            conn.execute(f"DELETE FROM blobs WHERE id IN ({','.join('?' * len(part))})", part)
    inline_removed = sum(1 for row in garbage if not row[2])
    inline_bytes = sum(row[3] for row in garbage if not row[2])
    dropped_files = {row[1].hex() for row in garbage if row[2]}

    # 2. Scan blobs/ without the lock: which files have no row?
    candidates = set(dropped_files)
    orphans_kept = 0
    wall = time.time()
    for hex_hash, path in _blobs.iter_blob_files(bucket.path):
        if hex_hash in candidates:
            continue
        with bucket._locked_conn() as conn:
            try:
                if _has_row(conn, bytes.fromhex(hex_hash)):
                    continue
            except sqlite3.Error as exc:
                raise translate_sqlite_error(exc, f"read bucket {name!r}") from exc
        try:
            age = wall - path.stat().st_mtime
        except FileNotFoundError:
            continue
        if age >= orphan_grace:
            candidates.add(hex_hash)
        else:
            orphans_kept += 1
    _point("gc_scanned")

    # 3. Under the lock: re-check every candidate, then unlink.
    files_removed = orphans_removed = file_bytes = 0
    failures: list[str] = []
    if candidates:
        with bucket._write_txn(f"remove unused files of bucket {name!r}") as txn:
            for hex_hash in sorted(candidates):
                if _has_row(txn.conn, bytes.fromhex(hex_hash)):
                    continue  # a writer committed this content meanwhile: keep it
                path = _blobs.blob_path(bucket.path, hex_hash)
                try:
                    size = path.stat().st_size
                    if not _blobs.remove_blob_file(bucket.path, hex_hash):
                        continue
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    failures.append(f"{_blobs.blob_relpath(hex_hash)}: {exc.strerror or exc}")
                    continue
                file_bytes += size
                if hex_hash in dropped_files:
                    files_removed += 1
                else:
                    orphans_removed += 1
                _remove_empty_dirs(path.parent, bucket.blobs_dir)

    # 4. Abandoned temp files (never one a live writer holds).
    tmp_removed = tmp_bytes = 0
    bucket._check_alive()  # never clean the tmp/ of a bucket that replaced ours
    for path in _blobs.iter_tmp_files(bucket.path):
        if not path.is_file():
            continue
        try:
            size = _blobs.remove_stale_tmp(path, min_age=tmp_min_age)  # unlinks holding the flock
        except OSError as exc:
            failures.append(f"tmp/{path.name}: {exc.strerror or exc}")
            continue
        if size is None:
            continue
        tmp_removed += 1
        tmp_bytes += size

    # 5. Give freed database pages back to the filesystem.
    pages_freed = _vacuum(bucket) if (trash_expired or garbage) else 0

    freed = inline_bytes + file_bytes + tmp_bytes
    parts = []
    if trash_expired:
        parts.append(f"expired {_plural(trash_expired, 'old version')}")
    if garbage:
        parts.append(f"dropped {_plural(len(garbage), 'unused stored content', 'unused stored contents')}")
    if orphans_removed:
        parts.append(f"removed {_plural(orphans_removed, 'leftover file')} from interrupted writes")
    if tmp_removed:
        parts.append(f"removed {_plural(tmp_removed, 'abandoned temp file')}")
    if parts:
        summary = f"Bucket {name!r}: " + ", ".join(parts) + (f"; freed about {_human_bytes(freed)}." if freed else ".")
    else:
        summary = f"Bucket {name!r}: nothing to clean up."
    if orphans_kept:
        summary += (
            f" {_plural(orphans_kept, 'recent leftover file')} kept for now; a later gc removes "
            f"{'it' if orphans_kept == 1 else 'them'} once older than {int(orphan_grace)} seconds."
        )
    if failures:
        summary += f" {_plural(len(failures), 'file')} could not be removed: " + "; ".join(failures[:5]) + "."
    return {
        "bucket": name,
        "trash_expired": trash_expired,
        "blobs_removed": len(garbage),
        "inline_blobs_removed": inline_removed,
        "file_blobs_removed": files_removed,
        "orphan_files_removed": orphans_removed,
        "orphan_files_kept": orphans_kept,
        "tmp_files_removed": tmp_removed,
        "bytes_freed": freed,
        "database_pages_freed": pages_freed,
        "errors": failures,
        "summary": summary,
    }


def _vacuum(bucket: Bucket) -> int:
    with bucket._write_txn(f"compact bucket {bucket.name!r}") as txn:
        before = txn.conn.execute("PRAGMA freelist_count").fetchone()[0]
        if before:
            txn.conn.execute("PRAGMA incremental_vacuum").fetchall()  # must be stepped to the end
    # Moving WAL pages into the main file lets SQLite truncate it. PASSIVE never
    # waits for readers or writers; whatever it cannot do now happens later.
    with bucket._locked_conn() as conn:
        try:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall()
        except sqlite3.Error:
            pass
    return before


def gc_store(
    store: Store,
    now: int | None = None,
    orphan_grace: float = DEFAULT_ORPHAN_GRACE,
    tmp_min_age: float = DEFAULT_TMP_MIN_AGE,
) -> dict:
    """gc every bucket, and remove leftovers of interrupted bucket creation or deletion."""
    store._require_writable()
    results = [
        gc(store.bucket(name), now=now, orphan_grace=orphan_grace, tmp_min_age=tmp_min_age)
        for name in store.list_buckets()
    ]
    leftovers = 0
    root = store.buckets_dir
    if root.is_dir():
        wall = time.time()
        for entry in root.iterdir():
            if not entry.is_dir() or not entry.name.startswith((".creating-", ".deleting-")):
                continue
            try:
                # A .creating- directory may belong to a create_bucket() in progress.
                if entry.name.startswith(".creating-") and wall - entry.stat().st_mtime < LEFTOVER_DIR_MIN_AGE:
                    continue
            except FileNotFoundError:
                continue
            shutil.rmtree(entry, ignore_errors=True)
            if not entry.exists():
                leftovers += 1
    freed = sum(r["bytes_freed"] for r in results)
    summary = f"Cleaned {_plural(len(results), 'bucket')}" + (
        f"; freed about {_human_bytes(freed)}." if freed else "; nothing needed freeing."
    )
    if leftovers:
        summary += f" Removed {_plural(leftovers, 'leftover directory', 'leftover directories')}."
    return {
        "buckets": results,
        "bytes_freed": freed,
        "leftover_dirs_removed": leftovers,
        "summary": summary,
    }


# -- verify ------------------------------------------------------------------


def verify(bucket: Bucket) -> dict:
    """Check everything in a bucket without changing anything.

    Re-reads and re-hashes every stored content (inline and files), checks
    that every object and trash row points at existing content, runs
    SQLite's integrity and foreign key checks, validates every key, and
    lists files under ``blobs/`` that no row refers to.

    ``ok`` is False when anything is damaged (data cannot be read back
    exactly). ``clean`` is additionally False when there is garbage that gc
    would remove (orphan files, unreferenced content, stale temp files);
    garbage never affects data. Works on read-only stores and while other
    processes write: content that gc removes during the run is skipped, not
    reported as missing.
    """
    name = bucket.name
    try:
        with bucket._read_txn() as conn:
            database = [r[0] for r in conn.execute("PRAGMA integrity_check(100)").fetchall()]
            if database == ["ok"]:
                database = []
            for table, rowid, parent, _fk in conn.execute("PRAGMA foreign_key_check").fetchall():
                database.append(f"row {rowid} of table {table} refers to a missing row in {parent}")
            objects = conn.execute("SELECT count(*) FROM objects").fetchone()[0]
            trash = conn.execute("SELECT count(*) FROM trash").fetchone()[0]
            missing_content = [
                {"key": k, "version": v, "current": bool(c)}
                for k, v, c in conn.execute(
                    "SELECT o.key, o.version, 1 FROM objects o LEFT JOIN blobs b ON b.id = o.blob_id "
                    "WHERE b.id IS NULL UNION ALL "
                    "SELECT t.key, t.version, 0 FROM trash t LEFT JOIN blobs b ON b.id = t.blob_id "
                    "WHERE b.id IS NULL"
                )
            ]
            invalid_keys = sorted(
                {
                    k
                    for (k,) in conn.execute("SELECT key FROM objects UNION SELECT key FROM trash")
                    if not _keys.is_valid_key(k)
                }
            )
            last_version = conn.execute("SELECT v FROM meta WHERE k = 'last_version'").fetchone()
            max_version = conn.execute(
                "SELECT max(v) FROM (SELECT max(version) AS v FROM objects UNION ALL SELECT max(version) FROM trash)"
            ).fetchone()[0]
            if last_version is None:
                database.append("the version counter (meta.last_version) is missing")
            elif max_version is not None and int(last_version[0]) < max_version:
                database.append(
                    f"the version counter is {last_version[0]} but version {max_version} exists; "
                    "new writes could collide with old versions"
                )
            unreferenced = conn.execute(f"SELECT count(*) FROM ({UNREFERENCED_BLOBS_SQL})").fetchone()[0]
            blob_ids = [r[0] for r in conn.execute("SELECT id FROM blobs ORDER BY id")]
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, f"check bucket {name!r}") from exc

    damaged: list[dict] = []
    checked = checked_bytes = 0
    for blob_id in blob_ids:
        try:
            with bucket._locked_conn() as conn:
                row = bucket._blob_row(conn, blob_id)
        except sqlite3.Error as exc:
            raise translate_sqlite_error(exc, f"check bucket {name!r}") from exc
        if row is None:
            continue  # removed by gc since the listing
        problem = check_blob(bucket.path, row)
        if problem is not None:
            with bucket._locked_conn() as conn:
                again = bucket._blob_row(conn, blob_id)
                if again is None or again.hash != row.hash:
                    continue  # gc removed it while we were reading it
                users = [
                    {"key": k, "version": v, "current": bool(c)}
                    for k, v, c in conn.execute(
                        "SELECT key, version, 1 FROM objects WHERE blob_id = ? UNION ALL "
                        "SELECT key, version, 0 FROM trash WHERE blob_id = ? ORDER BY 1, 2",
                        (blob_id, blob_id),
                    )
                ]
            entry = {
                "etag": row.hash.hex(),
                "location": "inline" if row.data is not None else "file",
                "path": None if row.data is not None else _blobs.blob_relpath(row.hash.hex()),
                "problem": problem,
                "versions": users,
            }
            damaged.append(entry)
        checked += 1
        checked_bytes += row.stored_size

    orphan_files, unexpected = _scan_blob_dir(bucket)
    tmp_files = stale_tmp = 0
    for path in _blobs.iter_tmp_files(bucket.path):
        tmp_files += 1
        if _blobs.tmp_is_stale(path, min_age=DEFAULT_TMP_MIN_AGE):
            stale_tmp += 1

    ok = not (damaged or database or missing_content or invalid_keys)
    clean = ok and not (orphan_files or unexpected or unreferenced or stale_tmp)
    affected = sum(len(d["versions"]) for d in damaged) + len(missing_content)
    if ok:
        summary = (
            f"Bucket {name!r} is intact: {_plural(checked, 'stored content')} "
            f"({_human_bytes(checked_bytes)}) re-read and matched their SHA-256."
        )
        hint = ""
        garbage = []
        if orphan_files:
            garbage.append(_plural(len(orphan_files), "leftover file"))
        if unreferenced:
            garbage.append(_plural(unreferenced, "unused stored content", "unused stored contents"))
        if stale_tmp:
            garbage.append(_plural(stale_tmp, "abandoned temp file"))
        if garbage:
            summary += f" There is garbage that does not affect your data: {', '.join(garbage)}."
            hint = "Run gc to remove it."
        if unexpected:
            summary += f" {_plural(len(unexpected), 'unexpected file')} sit under blobs/ (mybucket ignores them)."
            hint = (hint + " " if hint else "") + "Move files you did not put there out of the bucket directory."
    else:
        parts = []
        if damaged:
            parts.append(f"{_plural(len(damaged), 'stored content')} damaged")
        if missing_content:
            parts.append(f"{_plural(len(missing_content), 'version')} pointing at content that does not exist")
        if invalid_keys:
            parts.append(f"{_plural(len(invalid_keys), 'invalid key')}")
        if database:
            parts.append("database structure problems")
        summary = (
            f"Bucket {name!r} has problems: {', '.join(parts)}, affecting "
            f"{_plural(affected, 'object version')}. Reading the affected objects fails instead of "
            "returning wrong data."
        )
        hint = (
            "If you still have the original data of a damaged object, put it again (or import a good "
            "export): that repairs the stored copy. Otherwise restore the bucket from a snapshot or "
            "backup. Objects not listed under 'damaged' are fine and can be exported first."
        )
    return {
        "bucket": name,
        "ok": ok,
        "clean": clean,
        "objects": objects,
        "trash_versions": trash,
        "blobs_checked": checked,
        "bytes_checked": checked_bytes,
        "damaged": damaged,
        "missing_content": missing_content,
        "invalid_keys": invalid_keys,
        "database": database,
        "orphan_files": orphan_files,
        "unexpected_files": unexpected,
        "unreferenced_blobs": unreferenced,
        "tmp_files": tmp_files,
        "stale_tmp_files": stale_tmp,
        "summary": summary,
        "hint": hint,
    }


def _scan_blob_dir(bucket: Bucket) -> tuple[list[dict], list[str]]:
    """Files under blobs/ with no row (orphans) and entries that are not blob files at all."""
    orphans: list[dict] = []
    unexpected: list[str] = []
    root = bucket.blobs_dir
    if not root.is_dir():
        return orphans, unexpected
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            rel = path.relative_to(bucket.path).as_posix()
            if not _blobs.HEX_RE.fullmatch(filename) or rel != _blobs.blob_relpath(filename):
                unexpected.append(rel)
                continue
            with bucket._locked_conn() as conn:
                found = conn.execute(
                    "SELECT data IS NULL FROM blobs WHERE hash = ?", (bytes.fromhex(filename),)
                ).fetchone()
            if found is None:
                try:
                    size = path.stat().st_size
                except FileNotFoundError:
                    continue  # removed by gc meanwhile
                orphans.append({"etag": filename, "path": rel, "bytes": size})
            elif not found[0]:
                unexpected.append(rel)  # content is stored inline; this file is an extra copy
    return orphans, unexpected


def verify_store(store: Store) -> dict:
    """verify() every bucket of the store."""
    results = [verify(store.bucket(name)) for name in store.list_buckets()]
    ok = all(r["ok"] for r in results)
    bad = [r["bucket"] for r in results if not r["ok"]]
    if ok:
        summary = f"All {_plural(len(results), 'bucket')} intact."
    else:
        summary = f"{_plural(len(bad), 'bucket')} with problems: {', '.join(bad)}."
    return {
        "ok": ok,
        "clean": ok and all(r["clean"] for r in results),
        "buckets": results,
        "summary": summary,
    }


# -- snapshot ----------------------------------------------------------------


def snapshot(store: Store, dest: str | os.PathLike, buckets: list[str] | None = None) -> dict:
    """Write a consistent copy of the store (or of some buckets) to ``dest``.

    ``dest`` must be a new path or an empty directory, outside the store.
    The result is itself a store (open it with ``open_store(dest)``). Safe
    while other processes write; see the module docstring. Works on
    read-only stores too (it then cannot hold the write lock on the last
    attempt, so in the unlikely case that gc keeps removing files under it,
    the missing files are reported instead).
    """
    from .store import BUCKETS_DIR, _write_marker

    dest = Path(dest).expanduser()
    src_root = store.path.resolve()
    target = dest.resolve()
    if target == src_root or src_root in target.parents:
        raise InvalidArgument(
            f"The snapshot destination {dest} is inside the store it would copy.",
            hint="Choose a path outside the store directory.",
        )
    if dest.exists():
        if not dest.is_dir():
            raise Refused(f"{dest} already exists and is a file.", hint="Choose a new path or an empty directory.")
        if any(dest.iterdir()):
            raise Refused(
                f"{dest} is not empty, so the snapshot was not written.",
                hint="Choose a new path or an empty directory; nothing is ever overwritten.",
            )
        created_root = False
    else:
        created_root = True
    if buckets is None:
        names = store.list_buckets()
    else:
        names = [_keys.validate_bucket_name(n) for n in buckets]
        for n in names:
            store.bucket(n)  # raises BucketNotFound early

    results: list[dict] = []
    try:
        dest.mkdir(mode=0o700, parents=True, exist_ok=True)
        root = dest / BUCKETS_DIR
        root.mkdir(mode=0o700)
        for n in names:
            results.append(_snapshot_bucket(store.bucket(n), root / n))
        _blobs.fsync_dir(root)
        _write_marker(dest)  # last: a half-written snapshot is never taken for a store
        _blobs.fsync_dir(dest.parent)
    except BaseException:
        if created_root:
            shutil.rmtree(dest, ignore_errors=True)
        else:
            for child in list(dest.iterdir()) if dest.is_dir() else []:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
        raise

    linked = sum(r["linked"] for r in results)
    copied = sum(r["copied"] for r in results)
    copied_bytes = sum(r["bytes_copied"] for r in results)
    missing = sum(len(r["missing"]) for r in results)
    summary = f"Snapshot of {_plural(len(results), 'bucket')} written to {dest}"
    if linked or copied:
        summary += f" ({linked} files hard-linked, {copied} copied, {_human_bytes(copied_bytes)} copied)"
    summary += "."
    hint = ""
    if linked:
        hint = "Hard-linked files share disk space with the store; copy the snapshot to another disk for a backup."
    if missing:
        summary += f" {_plural(missing, 'file')} could not be included because they are missing in the store."
        hint = "Run verify on the store: those objects were already damaged before the snapshot."
    return {
        "dest": str(dest),
        "ok": missing == 0,
        "buckets": results,
        "linked": linked,
        "copied": copied,
        "bytes_copied": copied_bytes,
        "summary": summary,
        "hint": hint,
    }


def _snapshot_bucket(bucket: Bucket, bdir: Path) -> dict:
    bdir.mkdir(mode=0o700)
    (bdir / _blobs.BLOBS_DIR).mkdir(mode=0o700)
    (bdir / _blobs.TMP_DIR).mkdir(mode=0o700)
    db_path = bdir / DB_NAME
    os.close(os.open(db_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
    linker = _Linker()
    present: set[str] = set()
    missing: list[str] = []
    attempts = 0
    counts = (0, 0, 0)
    for attempts in range(1, SNAPSHOT_ATTEMPTS + 1):
        final = attempts == SNAPSHOT_ATTEMPTS
        lock = (
            bucket._write_txn(f"take a snapshot of bucket {bucket.name!r}")
            if final and not bucket.readonly
            else nullcontext()
        )
        with lock:
            _backup(bucket.db_path, db_path, bucket.store.timeout)
            wanted, counts = _snapshot_contents(db_path)
            # Files left by an earlier attempt that this copy does not reference.
            for hex_hash in sorted(present - wanted):
                (bdir / _blobs.blob_relpath(hex_hash)).unlink(missing_ok=True)
                present.discard(hex_hash)
            missing = []
            for hex_hash in sorted(wanted - present):
                try:
                    linker.place(_blobs.blob_path(bucket.path, hex_hash), bdir, hex_hash)
                except FileNotFoundError:
                    missing.append(hex_hash)
                    continue
                present.add(hex_hash)
        if not missing:
            break
    linker.sync()
    _blobs.fsync_dir(bdir)
    objects, trash, n_blobs = counts
    return {
        "bucket": bucket.name,
        "objects": objects,
        "trash_versions": trash,
        "blobs": n_blobs,
        "file_blobs": len(present) + len(missing),
        "linked": linker.linked,
        "copied": linker.copied,
        "bytes_copied": linker.bytes_copied,
        "attempts": attempts,
        "missing": missing,
    }


def _backup(src_path: Path, dst_path: Path, timeout: float) -> None:
    """Copy the database with the online backup API (one step = one consistent snapshot)."""
    src = connect(src_path, readonly=True, timeout=timeout)
    try:
        dst = sqlite3.connect(str(dst_path), isolation_level=None, timeout=timeout)
        try:
            src.backup(dst)  # pages=-1: everything from a single read transaction
        finally:
            dst.close()
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, "copy the bucket database") from exc
    finally:
        src.close()
    fd = os.open(dst_path, os.O_RDWR)
    try:
        _blobs.fsync_file(fd)
    finally:
        os.close(fd)


def _snapshot_contents(db_path: Path) -> tuple[set[str], tuple[int, int, int]]:
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        wanted = {h.hex() for (h,) in conn.execute("SELECT hash FROM blobs WHERE data IS NULL")}
        counts = (
            conn.execute("SELECT count(*) FROM objects").fetchone()[0],
            conn.execute("SELECT count(*) FROM trash").fetchone()[0],
            conn.execute("SELECT count(*) FROM blobs").fetchone()[0],
        )
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, "read the snapshot database") from exc
    finally:
        conn.close()
    return wanted, counts


class _Linker:
    """Hard-links blob files into a snapshot, falling back to copies."""

    def __init__(self) -> None:
        self.can_link = True
        self.linked = 0
        self.copied = 0
        self.bytes_copied = 0
        self._dirs: set[Path] = set()

    def place(self, src: Path, bdir: Path, hex_hash: str) -> None:
        dst = bdir / _blobs.blob_relpath(hex_hash)
        for directory in (dst.parent.parent, dst.parent):
            if not directory.is_dir():
                directory.mkdir(mode=0o700, exist_ok=True)
                self._dirs.add(directory.parent)
        self._dirs.add(dst.parent)
        if self.can_link:
            try:
                os.link(src, dst)
                self.linked += 1
                return
            except FileNotFoundError:
                raise
            except OSError:
                self.can_link = False  # another filesystem or not permitted: copy from now on
        self.bytes_copied += _copy_file(src, dst)
        self.copied += 1

    def sync(self) -> None:
        for directory in sorted(self._dirs, key=lambda p: len(p.parts), reverse=True):
            _blobs.fsync_dir(directory)


def _copy_file(src: Path, dst: Path) -> int:
    with open(src, "rb") as fin:  # once open, gc unlinking the name does not matter
        fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        size = 0
        try:
            while True:
                chunk = fin.read(CHUNK)
                if not chunk:
                    break
                view = memoryview(chunk)
                while view:
                    n = os.write(fd, view)
                    view = view[n:]
                size += len(chunk)
            _blobs.fsync_file(fd)
        except BaseException:
            os.close(fd)
            dst.unlink(missing_ok=True)
            raise
        os.close(fd)
    return size


# -- restore -----------------------------------------------------------------


def restore(bucket: Bucket, key: str, version: int) -> PutInfo:
    """Make a version kept in the trash the current version of ``key`` again.

    The restored content gets a new version number (versions only grow); the
    version it replaces goes to the trash like any overwrite, so a restore
    can itself be undone. Restoring the current version, or a version whose
    content, content type and metadata equal the current ones, changes
    nothing and returns status "unchanged". Versions stay restorable until gc
    removes them, even if they are past their expiry time.

    Raises VersionNotFound when that version is not kept, ReadOnlyError on a
    read-only store.
    """
    bucket._require_writable()
    key = _keys.validate_key(key)
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise InvalidArgument(
            f"version must be a positive whole number, got {version!r}.",
            hint="List the versions of the key to see their numbers.",
        )
    with bucket._write_txn(f"restore {key!r} version {version}") as txn:
        current = bucket._current(txn.conn, key)
        if current is not None and current.version == version:
            return PutInfo(**current.info().__dict__, status=STATUS_UNCHANGED)
        old = bucket._trash_entry(txn.conn, key, version)
        if old is None:
            raise VersionNotFound(f"Version {version} of {key!r} is not kept in bucket {bucket.name!r}.")
        if (
            current is not None
            and current.blob_id == old.blob_id
            and current.content_type == old.content_type
            and current.meta == old.meta
        ):
            return PutInfo(**current.info().__dict__, status=STATUS_UNCHANGED)
        new_version = bucket._set_current(
            txn, key, old.blob_id, old.content_type, old.meta, current, REASON_OVERWRITTEN
        )
        return PutInfo(
            key=key,
            etag=old.hash.hex(),
            size=old.size,
            content_type=old.content_type,
            metadata=_keys.parse_metadata(old.meta),
            modified=txn.now,
            version=new_version,
            stored_size=old.stored_size,
            status=STATUS_CREATED if current is None else STATUS_UPDATED,
        )


__all__ = ["gc", "gc_store", "verify", "verify_store", "snapshot", "restore"]
