"""A store: a directory that holds buckets.

Layout::

    <store>/
      mybucket-store.json         {"format": 1}  marks the directory as a store
      buckets/<bucket>/
        bucket.sqlite             metadata, inline blobs (WAL mode: -wal, -shm next to it)
        blobs/ab/cd/<sha256hex>   large blobs, immutable
        tmp/*.part                in-flight writes

There is no server: every process opens the files directly. SQLite
serialises writers per bucket (busy timeout plus ``BEGIN IMMEDIATE``) and
lets any number of readers work alongside one writer.

Public:
    open_store(path, readonly=False, create=True, timeout=10.0) -> Store
    Store.bucket(name, create=False) -> Bucket
    Store.create_bucket(name, trash_days=7) -> Bucket
    Store.list_buckets() -> list[str]
    Store.delete_bucket(name, force=False)
    Store.gc(...), Store.verify(), Store.snapshot(dest, buckets=None)
                                          see mybucket.maintenance
    Store.close(), Store.path, Store.readonly

Internal for siblings:
    Store.bucket_dir(name) -> Path        validated path of a bucket directory
    Store.buckets_dir -> Path
    Store._now() -> int                   unix ms (the injectable ``clock``)
    STORE_MARKER, BUCKETS_DIR, STORE_FORMAT
    Leftover ".creating-*" and ".deleting-*" directories under buckets/ come
    from interrupted create/delete calls and are safe to remove.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path

from . import keys as _keys
from .blobs import BLOBS_DIR, TMP_DIR, fsync_dir, fsync_file
from .bucket import Bucket
from .errors import (
    BucketExists,
    BucketNotEmpty,
    BucketNotFound,
    InvalidArgument,
    NotAStore,
    ReadOnlyError,
    StoreError,
    StoreNotFound,
    UnsupportedFormat,
)
from .schema import DB_NAME, create_db, translate_sqlite_error

STORE_MARKER = "mybucket-store.json"
BUCKETS_DIR = "buckets"
STORE_FORMAT = 1
DEFAULT_TRASH_DAYS = 7


def _write_marker(root: Path) -> None:
    tmp = root / f".{STORE_MARKER}.{secrets.token_hex(4)}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, (json.dumps({"format": STORE_FORMAT}) + "\n").encode())
        fsync_file(fd)
    finally:
        os.close(fd)
    os.replace(tmp, root / STORE_MARKER)
    fsync_dir(root)


def open_store(
    path: str | os.PathLike,
    readonly: bool = False,
    create: bool = True,
    timeout: float = 10.0,
    clock: Callable[[], int] | None = None,
) -> Store:
    """Open (and, unless ``readonly`` or ``create=False``, create) the store at ``path``.

    ``readonly=True`` opens every bucket database with SQLite's read-only
    mode and refuses all writes. ``timeout`` is how many seconds a writer waits
    for another writer before raising Busy. ``clock`` (unix ms) is for tests.
    """
    return Store(path, readonly=readonly, create=create, timeout=timeout, clock=clock)


class Store:
    def __init__(
        self,
        path: str | os.PathLike,
        readonly: bool = False,
        create: bool = True,
        timeout: float = 10.0,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.path = Path(path).expanduser()
        self.readonly = readonly
        self.timeout = timeout
        self._clock = clock
        self._buckets: dict[str, Bucket] = {}
        self._retired: list[Bucket] = []  # stale handles, closed with the store
        self._lock = threading.RLock()
        self._open_root(create and not readonly)

    def __repr__(self) -> str:
        return f"<Store {str(self.path)!r}{' ro' if self.readonly else ''}>"

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _now(self) -> int:
        if self._clock is not None:
            return int(self._clock())
        return int(time.time() * 1000)

    @property
    def buckets_dir(self) -> Path:
        return self.path / BUCKETS_DIR

    def _open_root(self, create: bool) -> None:
        root = self.path
        marker = root / STORE_MARKER
        if not root.exists():
            if not create:
                raise StoreNotFound(f"There is no store at {root}.")
            root.mkdir(mode=0o700, parents=True)
            self._initialise()
            return
        if not root.is_dir():
            raise NotAStore(f"{root} is a file, not a store directory.")
        if marker.is_file():
            try:
                info = json.loads(marker.read_text())
                fmt = int(info["format"])
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise StoreError(
                    f"The store marker {marker.name} in {root} cannot be read ({exc}).",
                    hint=f'Its content should be {{"format": {STORE_FORMAT}}}.',
                ) from None
            if fmt > STORE_FORMAT:
                raise UnsupportedFormat(f"The store at {root} uses format {fmt}; this mybucket reads up to {STORE_FORMAT}.")
            if create and not self.buckets_dir.is_dir():
                self.buckets_dir.mkdir(mode=0o700)
            return
        if any(root.iterdir()):
            raise NotAStore(f"{root} is not empty and is not a mybucket store.")
        if not create:
            raise StoreNotFound(f"{root} is an empty directory, not a store yet.")
        self._initialise()

    def _initialise(self) -> None:
        self.buckets_dir.mkdir(mode=0o700, exist_ok=True)
        fsync_dir(self.path)
        _write_marker(self.path)

    def _require_writable(self) -> None:
        if self.readonly:
            raise ReadOnlyError("Cannot change the store: it is open read-only.")

    def bucket_dir(self, name: str) -> Path:
        return self.buckets_dir / _keys.validate_bucket_name(name)

    # -- buckets -----------------------------------------------------------

    def list_buckets(self) -> list[str]:
        root = self.buckets_dir
        if not root.is_dir():
            return []
        return sorted(
            entry.name
            for entry in root.iterdir()
            if _keys.is_valid_bucket_name(entry.name) and (entry / DB_NAME).is_file()
        )

    def bucket(self, name: str, create: bool = False) -> Bucket:
        """Open bucket ``name`` (cached per store). With ``create=True`` it is created if missing."""
        path = self.bucket_dir(name)
        with self._lock:
            cached = self._buckets.get(name)
            if cached is not None:
                if cached._alive():
                    return cached
                # Closed, or deleted (and maybe re-created) by another process. A
                # caller may still hold the old handle: it keeps refusing with
                # BucketNotFound, and Store.close() closes it.
                self._buckets.pop(name, None)
                if cached._conn is not None:
                    self._retired.append(cached)
            if not (path / DB_NAME).is_file():
                if not create or self.readonly:
                    raise BucketNotFound(f"Bucket {name!r} does not exist in the store at {self.path}.")
                try:
                    return self.create_bucket(name)
                except BucketExists:
                    pass  # created concurrently by someone else: open it
            bucket = Bucket(self, name, path, readonly=self.readonly, timeout=self.timeout)
            self._buckets[name] = bucket
            return bucket

    def create_bucket(self, name: str, trash_days: int = DEFAULT_TRASH_DAYS) -> Bucket:
        """Create a new empty bucket. Raises BucketExists if it is already there."""
        self._require_writable()
        path = self.bucket_dir(name)
        if isinstance(trash_days, bool) or not isinstance(trash_days, int) or not 0 <= trash_days <= 36500:
            raise InvalidArgument(
                f"trash_days must be a whole number of days between 0 and 36500, got {trash_days!r}.",
                hint="For example 7 (the default) or 30; 0 turns the trash off.",
            )
        if path.exists():
            raise BucketExists(f"Bucket {name!r} already exists.")
        # Build the bucket in a hidden directory, then rename it into place, so a
        # crash never leaves a half-made bucket under its real name.
        staging = self.buckets_dir / f".creating-{name}-{secrets.token_hex(4)}"
        staging.mkdir(mode=0o700)
        try:
            (staging / BLOBS_DIR).mkdir(mode=0o700)
            (staging / TMP_DIR).mkdir(mode=0o700)
            create_db(staging / DB_NAME, trash_days)
            fsync_dir(staging)
            try:
                os.rename(staging, path)
            except OSError:
                if path.exists():
                    raise BucketExists(f"Bucket {name!r} already exists.") from None
                raise
            fsync_dir(self.buckets_dir)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        with self._lock:
            bucket = Bucket(self, name, path, readonly=False, timeout=self.timeout)
            self._buckets[name] = bucket
        return bucket

    def delete_bucket(self, name: str, force: bool = False) -> None:
        """Remove a bucket and everything in it.

        Refuses (BucketNotEmpty) while it holds objects or trash, unless
        ``force=True``. The write lock is held while the directory is moved
        aside, so no writer can slip an object in after the emptiness check.
        """
        self._require_writable()
        bucket = self.bucket(name)
        aside = self.buckets_dir / f".deleting-{name}-{secrets.token_hex(4)}"
        try:
            with bucket._write_txn(f"delete bucket {name!r}") as txn:
                objects = txn.conn.execute("SELECT count(*) FROM objects").fetchone()[0]
                trashed = txn.conn.execute("SELECT count(*) FROM trash").fetchone()[0]
                if (objects or trashed) and not force:
                    raise BucketNotEmpty(
                        f"Bucket {name!r} still holds {_count(objects, 'object')} and "
                        f"{_count(trashed, 'trashed version')}."
                    )
                # Mark the database first: a handle another process still has open
                # on it then refuses every later write (bucket.py), even though
                # its file is about to disappear.
                txn.conn.execute("INSERT OR REPLACE INTO meta (k, v) VALUES ('deleted', ?)", (txn.now,))
                os.rename(bucket.path, aside)
        except sqlite3.Error as exc:  # pragma: no cover - mapped by _write_txn already
            raise translate_sqlite_error(exc, f"delete bucket {name!r}") from exc
        finally:
            with self._lock:
                self._buckets.pop(name, None)
            bucket.close()
        fsync_dir(self.buckets_dir)
        shutil.rmtree(aside, ignore_errors=True)

    # -- maintenance (thin wrappers over mybucket.maintenance) -------------

    def gc(self, **kwargs) -> dict:
        """gc every bucket and remove leftovers of interrupted create/delete; see maintenance.gc_store."""
        from .maintenance import gc_store

        return gc_store(self, **kwargs)

    def verify(self) -> dict:
        """verify every bucket; see maintenance.verify_store. Changes nothing."""
        from .maintenance import verify_store

        return verify_store(self)

    def snapshot(self, dest: str | os.PathLike, buckets: list[str] | None = None) -> dict:
        """Write a consistent copy of the store to ``dest``; see maintenance.snapshot."""
        from .maintenance import snapshot

        return snapshot(self, dest, buckets=buckets)

    def close(self) -> None:
        with self._lock:
            for bucket in [*self._buckets.values(), *self._retired]:
                bucket.close()
            self._buckets.clear()
            self._retired.clear()


def _count(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")
