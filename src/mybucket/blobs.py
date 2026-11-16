"""Blob files on disk: paths, temp files, fsync and atomic install.

A blob whose stored (compressed) size is above 128 KiB lives in a file::

    <bucket>/blobs/<hex[0:2]>/<hex[2:4]>/<hex>

where ``hex`` is the lowercase SHA-256 of the *uncompressed* content. The
file holds the stored bytes exactly (a single zstd frame, or the raw bytes;
the ``blobs`` row says which). Files are immutable once installed.

File invariants (they make garbage collection safe against concurrent writers):

1. A blob file appears at its final path only through ``install()``, which a
   writer calls while it holds the bucket write lock (inside its
   ``BEGIN IMMEDIATE`` transaction), right before inserting the ``blobs`` row
   in that same transaction. The data is fsynced before the rename and the
   directory is fsynced after it, so a committed row always points to a
   complete, durable file.
2. A blob file may be deleted only while holding the bucket write lock and
   only when no ``blobs`` row with its hash exists. A file without a row is
   garbage left by a crash between ``install()`` and ``COMMIT``.
3. In-flight writes live in ``<bucket>/tmp/*.part``. The writer holds an
   exclusive ``flock`` on its temp file until it is installed or discarded,
   so ``tmp_is_stale()`` never reports a file that is still being written.
   A writer creates the file a moment before it gets the flock; in that gap
   a cleaner may take the flock and unlink the new file. ``remove_stale_tmp()``
   therefore unlinks only while it holds the flock, and ``TempFile`` checks,
   once it holds the flock, that its name still points at its file (and
   starts again with a new name if not), so neither side can lose the race.

Internal for siblings:
    BLOBS_DIR, TMP_DIR, TMP_SUFFIX
    blob_relpath(hex) -> str           "blobs/ab/cd/abcd..."
    blob_path(bucket_dir, hex) -> Path
    HEX_RE                             a 64-character lowercase hex name
    TempFile(tmp_dir)                  .write(b), .commit() (fsync), .path, .size, .discard()
    install(tmp, bucket_dir, hex)      rename into place + fsync dirs (hold the write lock!)
    remove_blob_file(bucket_dir, hex)  unlink + fsync dir (hold the write lock, no row!)
    iter_blob_files(bucket_dir)        yields (hex, Path) for every file under blobs/
    iter_tmp_files(bucket_dir)         yields Path for every entry under tmp/
    tmp_is_stale(path, min_age=3600)   True when nobody is writing it any more
    remove_stale_tmp(path, min_age) -> int | None
                                       unlink it if stale (holding its flock);
                                       the bytes freed, or None when kept
    fsync_file(fd), fsync_dir(path)
"""

from __future__ import annotations

import os
import re
import secrets
import sys
import time
from collections.abc import Iterator
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

BLOBS_DIR = "blobs"
TMP_DIR = "tmp"
TMP_SUFFIX = ".part"

HEX_RE = re.compile(r"[0-9a-f]{64}")
_HEX_RE = HEX_RE  # older name, kept for siblings


def blob_relpath(hex_hash: str) -> str:
    return f"{BLOBS_DIR}/{hex_hash[0:2]}/{hex_hash[2:4]}/{hex_hash}"


def blob_path(bucket_dir: str | os.PathLike, hex_hash: str) -> Path:
    return Path(bucket_dir) / BLOBS_DIR / hex_hash[0:2] / hex_hash[2:4] / hex_hash


def fsync_file(fd: int) -> None:
    """Flush a file to stable storage (F_FULLFSYNC on macOS, where fsync is not enough)."""
    if sys.platform == "darwin" and fcntl is not None and hasattr(fcntl, "F_FULLFSYNC"):
        try:
            fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
            return
        except OSError:
            pass
    os.fsync(fd)


def fsync_dir(path: str | os.PathLike) -> None:
    """Make a rename or a new entry in ``path`` durable."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:  # pragma: no cover - platforms without directory fds
        return
    try:
        fsync_file(fd)
    except OSError:  # pragma: no cover - some filesystems refuse directory fsync
        pass
    finally:
        os.close(fd)


class TempFile:
    """A temp file under ``tmp/``, exclusively flock-ed while it is in use."""

    def __init__(self, tmp_dir: str | os.PathLike) -> None:
        tmp_dir = Path(tmp_dir)
        tmp_dir.mkdir(mode=0o700, exist_ok=True)
        self.fd: int | None = None
        for _attempt in range(10):
            name = f"{os.getpid()}-{secrets.token_hex(8)}{TMP_SUFFIX}"
            path = tmp_dir / name
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            if fcntl is None:  # pragma: no cover - non-POSIX
                break
            fcntl.flock(fd, fcntl.LOCK_EX)
            # A cleaner may have unlinked the brand-new file before we got the
            # flock (invariant 3): then start again under a new name.
            try:
                if os.stat(path).st_ino == os.fstat(fd).st_ino:
                    break
            except FileNotFoundError:
                pass
            os.close(fd)
        else:  # pragma: no cover - would need ten lost races in a row
            raise OSError(f"could not create a temp file in {tmp_dir}")
        self.path = path
        self.fd = fd
        self.size = 0
        self.installed = False

    def write(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            n = os.write(self.fd, view)  # type: ignore[arg-type]
            view = view[n:]
        self.size += len(data)

    def commit(self) -> None:
        """fsync the data. Must be called before install()."""
        fsync_file(self.fd)  # type: ignore[arg-type]

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)  # also releases the flock
            self.fd = None

    def discard(self) -> None:
        """Close and delete the temp file (no-op after install)."""
        if not self.installed:
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass
        self.close()

    def __del__(self) -> None:  # best effort; normal paths call discard()
        try:
            self.discard()
        except Exception:
            pass


def install(tmp: TempFile, bucket_dir: str | os.PathLike, hex_hash: str) -> Path:
    """Atomically move a committed temp file to its blob path.

    The caller must hold the bucket write lock and insert the blobs row in the
    same transaction. An existing file at the destination (left by a crash) is
    replaced; it is garbage by invariant 2, and the new bytes are identical.
    """
    dest = blob_path(bucket_dir, hex_hash)
    blobs_root = Path(bucket_dir) / BLOBS_DIR
    created: list[Path] = []
    for directory in (blobs_root, dest.parent.parent, dest.parent):
        if not directory.is_dir():
            try:
                directory.mkdir(mode=0o700)
                created.append(directory)
            except FileExistsError:
                pass
    os.replace(tmp.path, dest)
    tmp.installed = True
    fsync_dir(dest.parent)
    for directory in reversed(created):
        fsync_dir(directory.parent)
    tmp.close()
    return dest


def remove_blob_file(bucket_dir: str | os.PathLike, hex_hash: str) -> bool:
    """Delete one blob file. Only call with the write lock held and no blobs row for it."""
    path = blob_path(bucket_dir, hex_hash)
    try:
        os.unlink(path)
    except FileNotFoundError:
        return False
    fsync_dir(path.parent)
    return True


def iter_blob_files(bucket_dir: str | os.PathLike) -> Iterator[tuple[str, Path]]:
    """Yield (hex, path) for every well-named blob file. Other entries are skipped."""
    root = Path(bucket_dir) / BLOBS_DIR
    if not root.is_dir():
        return
    for first in sorted(root.iterdir()):
        if not first.is_dir() or len(first.name) != 2:
            continue
        for second in sorted(first.iterdir()):
            if not second.is_dir() or len(second.name) != 2:
                continue
            for entry in sorted(second.iterdir()):
                name = entry.name
                if _HEX_RE.fullmatch(name) and name[0:2] == first.name and name[2:4] == second.name:
                    yield name, entry


def iter_tmp_files(bucket_dir: str | os.PathLike) -> Iterator[Path]:
    root = Path(bucket_dir) / TMP_DIR
    if not root.is_dir():
        return
    yield from sorted(root.iterdir())


def tmp_is_stale(path: str | os.PathLike, min_age: float = 3600.0) -> bool:
    """True when ``path`` is an abandoned temp file that can be deleted.

    Requires both: not modified for ``min_age`` seconds, and no process
    holding its flock (a live writer always holds it).
    """
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return False
    if time.time() - st.st_mtime < min_age:
        return False
    if fcntl is None:  # pragma: no cover
        return True
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    finally:
        os.close(fd)
    return True


def remove_stale_tmp(path: str | os.PathLike, min_age: float = 3600.0) -> int | None:
    """Delete ``path`` if it is an abandoned temp file; return its size, or None if kept.

    Unlike ``tmp_is_stale()`` followed by an unlink, the file is unlinked
    while this process holds its flock, so a writer that has just created it
    (and is waiting for the flock) notices and starts again (invariant 3).
    """
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    if time.time() - st.st_mtime < min_age:
        return None
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return None
    try:
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return None
        held = os.fstat(fd)
        try:
            if os.stat(path).st_ino != held.st_ino:
                return None  # replaced meanwhile: not the file we checked
        except FileNotFoundError:
            return None
        os.unlink(path)
        return held.st_size
    finally:
        os.close(fd)
