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
    iter_blob_files(bucket_dir)        yields (hex, Path) for every file under blobs/ (links
                                       are never followed: a linked folder is not entered)
    iter_tmp_files(bucket_dir)         yields Path for every entry under tmp/
    open_blob(bucket_dir, hex) -> int  read-only fd of a blob file, opened without following
                                       any link (FileNotFoundError when it is missing,
                                       UnsafePath when a link or a non-regular file is in the way)
    link_problem(bucket_dir) -> str | None
                                       why the bucket directory is unsafe to use (a link
                                       or a non-regular entry where mybucket expects its
                                       own folders and files), or None
    UnsafePath                         OSError subclass for the above
    tmp_is_stale(path, min_age=3600)   True when nobody is writing it any more
    remove_stale_tmp(path, min_age) -> int | None
                                       unlink it if stale (holding its flock);
                                       the bytes freed, or None when kept
    fsync_file(fd), fsync_dir(path)

Links: a store may come from elsewhere (a snapshot, a synced copy, a
restored backup) and must never make mybucket read, write or delete
anything outside it. So nothing inside a bucket is reached through a
symbolic link: folders are opened one at a time relative to their parent
with ``O_NOFOLLOW``, files are created, renamed and unlinked relative to a
folder descriptor, walkers never descend into linked folders, and every
file is opened non-blocking (a FIFO can never hang a reader).
"""

from __future__ import annotations

import errno
import os
import re
import secrets
import stat
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
_FANOUT_RE = re.compile(r"[0-9a-f]{2}")

_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)
_DIR_FLAGS = os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC | _O_NONBLOCK


class UnsafePath(OSError):
    """A symbolic link or a non-regular entry where mybucket expects its own file or folder."""


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
                if os.lstat(path).st_ino == os.fstat(fd).st_ino:
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


def _open_dirs(bucket_dir: str | os.PathLike, parts: list[str], create: bool = False) -> tuple[list[int], list[int]]:
    """Open ``bucket_dir`` and then each of ``parts`` below it, never following a link.

    Returns (fds, created): one fd per level (the bucket folder first) and
    the indexes of the levels this call created. The caller closes the fds.
    A missing level raises FileNotFoundError (unless ``create``); a link or
    a non-folder raises UnsafePath.
    """
    fds: list[int] = []
    created: list[int] = []
    try:
        fds.append(_open_dir_at(None, os.fspath(bucket_dir)))
        for i, name in enumerate(parts, start=1):
            try:
                fds.append(_open_dir_at(fds[-1], name))
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(name, 0o700, dir_fd=fds[-1])
                    created.append(i)
                except FileExistsError:
                    pass
                fds.append(_open_dir_at(fds[-1], name))
    except BaseException:
        for fd in fds:
            os.close(fd)
        raise
    return fds, created


def _open_dir_at(dir_fd: int | None, name: str) -> int:
    try:
        return os.open(name, _DIR_FLAGS, dir_fd=dir_fd)
    except FileNotFoundError:
        raise
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR, errno.EMLINK):
            raise UnsafePath(exc.errno, f"{name} is a symbolic link or not a folder", name) from None
        raise


def _close_all(fds: list[int]) -> None:
    for fd in fds:
        try:
            os.close(fd)
        except OSError:
            pass


def _blob_parts(hex_hash: str) -> list[str]:
    return [BLOBS_DIR, hex_hash[0:2], hex_hash[2:4]]


def open_blob(bucket_dir: str | os.PathLike, hex_hash: str) -> int:
    """A read-only fd of the blob file of ``hex_hash``, reached without following any link.

    Raises FileNotFoundError when it is missing and UnsafePath when a link or
    a non-regular file is in the way. The caller closes the fd.
    """
    fds, _ = _open_dirs(bucket_dir, _blob_parts(hex_hash))
    try:
        try:
            fd = os.open(hex_hash, _FILE_FLAGS, dir_fd=fds[-1])
        except FileNotFoundError:
            raise
        except OSError as exc:
            if exc.errno in (errno.ELOOP, errno.EMLINK):
                raise UnsafePath(exc.errno, "the blob file is a symbolic link", hex_hash) from None
            raise
    finally:
        _close_all(fds)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise UnsafePath(errno.EINVAL, "the blob file is not a regular file", hex_hash)
    return fd


def install(tmp: TempFile, bucket_dir: str | os.PathLike, hex_hash: str) -> Path:
    """Atomically move a committed temp file to its blob path.

    The caller must hold the bucket write lock and insert the blobs row in the
    same transaction. An existing file at the destination (left by a crash) is
    replaced; it is garbage by invariant 2, and the new bytes are identical.
    The folders on the way are opened (or made) without following links, and
    the rename is made relative to the last one.
    """
    fds, created = _open_dirs(bucket_dir, _blob_parts(hex_hash), create=True)
    try:
        os.replace(tmp.path, hex_hash, dst_dir_fd=fds[-1])
        tmp.installed = True
        _fsync_fd(fds[-1])
        for level in reversed(created):
            _fsync_fd(fds[level - 1])
    finally:
        _close_all(fds)
    tmp.close()
    return blob_path(bucket_dir, hex_hash)


def _fsync_fd(fd: int) -> None:
    try:
        fsync_file(fd)
    except OSError:  # pragma: no cover - some filesystems refuse directory fsync
        pass


def remove_blob_file(bucket_dir: str | os.PathLike, hex_hash: str, prune: bool = False) -> int | None:
    """Delete one blob file; returns the bytes it held, or None when it was not there.

    Only call with the write lock held and no blobs row for it. The unlink
    is made relative to its folder, opened without following links, so a
    linked folder can never make it delete a file elsewhere. ``prune``
    also removes the two fan-out folders when they are left empty.
    """
    try:
        fds, _ = _open_dirs(bucket_dir, _blob_parts(hex_hash))
    except FileNotFoundError:
        return None
    try:
        try:
            st = os.stat(hex_hash, dir_fd=fds[-1], follow_symlinks=False)
            os.unlink(hex_hash, dir_fd=fds[-1])
        except FileNotFoundError:
            return None
        _fsync_fd(fds[-1])
        if prune:
            for level in (3, 2):
                try:
                    os.rmdir(_blob_parts(hex_hash)[level - 1], dir_fd=fds[level - 1])
                except OSError:
                    break
                _fsync_fd(fds[level - 1])
        return st.st_size if stat.S_ISREG(st.st_mode) else 0
    finally:
        _close_all(fds)


def iter_blob_files(bucket_dir: str | os.PathLike) -> Iterator[tuple[str, Path]]:
    """Yield (hex, path) for every well-named blob file. Other entries are skipped.

    Linked folders are never entered. A link that has a blob file's name is
    yielded like a file (removing it removes only the link).
    """
    root = Path(bucket_dir) / BLOBS_DIR
    for first in _scan_dirs(root):
        for second in _scan_dirs(root / first):
            try:
                entries = sorted((e.name, e.is_dir(follow_symlinks=False)) for e in os.scandir(root / first / second))
            except OSError:
                continue
            for name, is_dir in entries:
                if not is_dir and _HEX_RE.fullmatch(name) and name[0:2] == first and name[2:4] == second:
                    yield name, root / first / second / name


def _scan_dirs(path: Path) -> list[str]:
    """Names of the two-hex-digit folders directly in ``path`` (links excluded)."""
    try:
        with os.scandir(path) as it:
            return sorted(e.name for e in it if _FANOUT_RE.fullmatch(e.name) and e.is_dir(follow_symlinks=False))
    except (FileNotFoundError, NotADirectoryError):
        return []


def iter_tmp_files(bucket_dir: str | os.PathLike) -> Iterator[Path]:
    root = Path(bucket_dir) / TMP_DIR
    try:
        if not stat.S_ISDIR(os.lstat(root).st_mode):
            return
        with os.scandir(root) as it:
            names = sorted(e.name for e in it)
    except FileNotFoundError:
        return
    for name in names:
        yield root / name


def link_problem(bucket_dir: str | os.PathLike) -> str | None:
    """Why the bucket folder is unsafe to use, or None.

    Checks, without following links: the bucket folder itself, its database
    files, ``blobs/``, ``tmp/`` and the two levels of fan-out folders under
    ``blobs/``. (Blob files themselves are checked whenever one is opened.)
    """
    root = Path(bucket_dir)
    expected = [
        ("", "folder", True),
        ("bucket.sqlite", "file", False),
        ("bucket.sqlite-wal", "file", False),
        ("bucket.sqlite-shm", "file", False),
        (BLOBS_DIR, "folder", False),
        (TMP_DIR, "folder", False),
    ]
    for rel, kind, required in expected:
        path = root / rel if rel else root
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            if required:
                return None  # the caller reports a missing bucket its own way
            continue
        shown = rel or "the bucket folder"
        if stat.S_ISLNK(st.st_mode):
            return f"{shown} is a symbolic link"
        if kind == "folder" and not stat.S_ISDIR(st.st_mode):
            return f"{shown} is not a folder"
        if kind == "file" and not stat.S_ISREG(st.st_mode):
            return f"{shown} is not a regular file"
    blobs = root / BLOBS_DIR
    try:
        with os.scandir(blobs) as it:
            first = [(e.name, e.is_symlink(), e.is_dir(follow_symlinks=False)) for e in it]
    except FileNotFoundError:
        return None
    for name, is_link, is_dir in first:
        if is_link:
            return f"{BLOBS_DIR}/{name} is a symbolic link"
        if not is_dir:
            continue
        try:
            with os.scandir(blobs / name) as it:
                for e in it:
                    if e.is_symlink() and _FANOUT_RE.fullmatch(e.name):
                        return f"{BLOBS_DIR}/{name}/{e.name} is a symbolic link"
        except OSError:
            continue
    return None


def tmp_is_stale(path: str | os.PathLike, min_age: float = 3600.0) -> bool:
    """True when ``path`` is an abandoned temp file that can be deleted.

    Requires both: not modified for ``min_age`` seconds, and no process
    holding its flock (a live writer always holds it).
    """
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(st.st_mode) or time.time() - st.st_mtime < min_age:
        return False
    if fcntl is None:  # pragma: no cover
        return True
    try:
        fd = os.open(path, _FILE_FLAGS)
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
        st = os.lstat(path)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(st.st_mode) or time.time() - st.st_mtime < min_age:
        return None
    try:
        fd = os.open(path, _FILE_FLAGS)
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
            if os.lstat(path).st_ino != held.st_ino:
                return None  # replaced meanwhile: not the file we checked
        except FileNotFoundError:
            return None
        os.unlink(path)
        return held.st_size
    finally:
        os.close(fd)
