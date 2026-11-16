"""Getting data out of a bucket as plain files, and plain files into a bucket.

These two functions are the "no lock-in" tools: they need nothing but a
directory on the other side.

Public:
    export_dir(bucket, dest, prefix="", overwrite=False, skip_conflicts=False) -> dict
    import_dir(bucket, src, prefix="", guess_types=True) -> dict
    ExportConflict (a Refused error, exit code 5)
    MANIFEST_NAME

Export
------
Every current object under ``prefix`` becomes a regular file at its key path
under ``dest`` (key ``a/b.txt`` -> ``dest/a/b.txt``; the prefix is *not*
stripped). Each file is written to a hidden temp file, fsynced and renamed,
so a crash never leaves a half-written file under a key's name, and its
modification time is set to the object's. New files are mode 0600 and new
folders 0700, like the store itself.

Safety rules:

- Paths come only from validated keys (``keys.key_segments``), and every
  folder on the way is opened with ``O_NOFOLLOW`` relative to its parent, so
  a symbolic link inside ``dest`` can never redirect a write outside it.
- ``dest`` must not be inside the store, and the store must not be inside
  ``dest``.
- A key that is also a folder of other keys (``a`` and ``a/b``) cannot be
  written as both a file and a folder. Letter-case variants (``A.txt`` and
  ``a.txt``, ``É`` and ``é``) clash on case-insensitive filesystems such as
  the macOS default; export tests the destination to find out, and compares
  names with a deliberately broad caseless form (``keys.fold_path``). These
  clashes are found before anything is written, and export refuses
  (ExportConflict) unless ``skip_conflicts=True``, which exports everything
  else and lists the keys it left out.
- Whatever rules the destination filesystem really uses to tell names
  apart, export never lets two keys end up in one file or one folder: it
  remembers the file and folder identities (device and inode) it wrote or
  opened in this run, and a key whose path turns out to be one of them is a
  conflict, even with ``overwrite=True``.
- Something already at a key's path is never replaced silently: an
  identical file counts as "unchanged", a different file (or a folder or
  link) is a conflict unless ``overwrite=True`` (files and links are then
  replaced; folders never are).
- A file or folder that cannot be read or written for lack of permission
  is a conflict for that key, not the end of the export.
- Damaged objects (bad bytes or a missing blob file) are not written;
  export finishes the rest and then raises DamagedError naming them.

Export is not a point-in-time copy of the bucket: objects changed while it
runs may come out old or new. Export from a snapshot for a frozen view.

The manifest ``dest/.mybucket-export.jsonl`` (JSON Lines) records what
plain files cannot hold. Line 1 is a header::

    {"format": "mybucket-export", "version": 1, "bucket": "...", "prefix": "...",
     "exported_at": <unix ms>}

and then one line per object exported (written or already identical)::

    {"key": "...", "etag": "<sha256 hex>", "size": N, "content_type": "..." | null,
     "metadata": {...}, "modified": <unix ms>, "version": N}

It is replaced at the end of each export, so it describes the latest one.
Its name starts with "." and is therefore never a valid key.

Import
------
Every regular file under ``src`` becomes an object whose key is its relative
path, with ``prefix`` (a key or key-like folder, "/" added when missing) in
front. Rules:

- Symbolic links inside ``src`` are never followed (and are skipped), nor are special
  files. Names that are not valid key segments (hidden files such as
  ``.DS_Store``, names that start or end with a space, ...) are skipped with
  the reason; a folder with such a name is skipped as a whole.
- Names are turned into keys in Unicode NFC form (macOS and some copy tools
  write decomposed names). Names in one folder that are different on disk
  but the same key once normalised (possible on Linux filesystems) are all
  skipped with the reason, instead of one silently replacing the other.
- Content type and metadata come from the manifest when ``src`` holds one,
  looked up by relative path (so export then import round-trips them, with
  or without a prefix); the manifest itself is never imported; otherwise an existing object
  keeps its own; otherwise the type is guessed from the file extension with
  Python's built-in table (deterministic; ``guess_types=False`` disables it).
- Idempotent: a file whose bytes, content type and metadata match the
  current object is reported "unchanged" and nothing is written (it is read
  and hashed, and the stored copy is re-checked). If that stored copy turns
  out to be damaged, the file's bytes replace it ("repaired"), so importing
  a good export again repairs a damaged bucket. Files are written in
  batches (one transaction each).
"""

from __future__ import annotations

import errno
import hashlib
import json
import mimetypes
import os
import secrets
import stat
import sys
import unicodedata
from pathlib import Path

from . import keys as _keys
from .blobs import fsync_file
from .codec import CHUNK
from .errors import (
    DamagedError,
    InvalidArgument,
    InvalidKey,
    MyBucketError,
    NotFound,
    ObjectNotFound,
    ReadOnlyError,
    Refused,
    UnsupportedFormat,
)
from .models import STATUS_CREATED, STATUS_UNCHANGED, STATUS_UPDATED

MANIFEST_NAME = ".mybucket-export.jsonl"
MANIFEST_FORMAT = "mybucket-export"
MANIFEST_VERSION = 1
_TMP_PREFIX = ".mybucket-tmp-"
_LIST_PAGE = 1000
_MAX_LISTED = 50  # problem entries kept in results and messages
_BATCH_FILES = 500
_BATCH_BYTES = 64 * 1024 * 1024
_FILE_MODE = 0o600
_DIR_MODE = 0o700

_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


class ExportConflict(Refused):
    code = "export_conflict"
    default_hint = (
        "Move the existing files out of the way, or pass overwrite=True to replace "
        "existing files that differ, or skip_conflicts=True to export everything else."
    )
    #: Hint for keys that cannot coexist as files (case or file/folder clashes).
    HINT_CLASH = (
        "Pass skip_conflicts=True to export everything else (the keys left out are "
        "listed), or export to a case-sensitive volume for letter-case clashes."
    )


def _show_list(items: list[str], limit: int = 5) -> str:
    shown = ", ".join(repr(i) for i in items[:limit])
    if len(items) > limit:
        shown += f" and {len(items) - limit} more"
    return shown


def _count(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


def _inside(child: Path, parent: Path) -> bool:
    return child == parent or parent in child.parents


def inside_store(store_path: Path, other: Path) -> bool:
    """True when ``other`` (resolved, links followed) is the store directory or inside it."""
    return _inside(Path(os.path.realpath(other)), Path(os.path.realpath(store_path)))


def _check_not_store(store_path: Path, other: Path, what: str) -> None:
    an = "an" if what[:1] in "aeiou" else "a"
    if inside_store(store_path, other):
        raise InvalidArgument(
            f"The {what} folder {other} is inside the store itself.",
            hint=f"Choose {an} {what} folder outside the store directory.",
        )
    if _inside(Path(os.path.realpath(store_path)), Path(os.path.realpath(other))):
        raise InvalidArgument(
            f"The {what} folder {other} contains the store itself.",
            hint=f"Choose {an} {what} folder that does not contain the store directory.",
        )


def _normalize_import_prefix(prefix: object) -> str:
    if prefix is None or prefix == "":
        return ""
    if not isinstance(prefix, str):
        raise InvalidArgument(
            f"Prefix must be a string, got {type(prefix).__name__}.",
            hint="An import prefix looks like a folder of keys, for example 'backups/2026'.",
        )
    stripped = prefix[:-1] if prefix.endswith("/") else prefix
    try:
        stripped = _keys.validate_key(stripped)
    except InvalidKey as exc:
        raise InvalidKey(
            f"Import prefix {prefix!r} is not usable: {exc.message}",
            hint="An import prefix looks like a folder of keys, for example 'backups/2026'.",
        ) from None
    return stripped + "/"


def _file_sha256(fd: int) -> bytes:
    hasher = hashlib.sha256()
    os.lseek(fd, 0, os.SEEK_SET)
    while True:
        chunk = os.read(fd, CHUNK)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.digest()


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        view = view[n:]


def _fsync_dir_fd(fd: int) -> None:
    try:
        fsync_file(fd)  # F_FULLFSYNC on macOS, like the engine
    except OSError:
        pass  # some filesystems do not support fsync on directories


# -- export --------------------------------------------------------------------


class _Dirs:
    """Opens folders under a root one segment at a time, never following links."""

    def __init__(self, root_fd: int) -> None:
        self.root_fd = root_fd
        self._cache: dict[tuple[str, ...], int] = {}
        self.touched: set[tuple[str, ...]] = set()
        root = os.fstat(root_fd)
        #: (st_dev, st_ino) of every folder opened -> the key folder it was opened for.
        self._identity: dict[tuple[int, int], tuple[str, ...]] = {(root.st_dev, root.st_ino): ()}

    def open(self, parts: tuple[str, ...]) -> int:
        """Return an fd for root/parts, creating folders. Raises _PathProblem."""
        if not parts:
            return self.root_fd
        cached = self._cache.get(parts)
        if cached is not None:
            return cached
        parent = self.open(parts[:-1])
        name = parts[-1]
        try:
            os.mkdir(name, _DIR_MODE, dir_fd=parent)
            self.touched.add(parts[:-1])
        except FileExistsError:
            pass
        except PermissionError as exc:
            raise _PathProblem(f"the folder {'/'.join(parts)!r} cannot be created ({exc.strerror})") from None
        try:
            fd = os.open(name, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC, dir_fd=parent)
        except OSError as exc:
            where = "/".join(parts)
            try:
                st = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except OSError:
                raise _PathProblem(f"the folder {where!r} cannot be opened ({exc.strerror})") from None
            if stat.S_ISLNK(st.st_mode):
                raise _PathProblem(f"{where!r} in the destination is a symbolic link, which export never follows") from None
            raise _PathProblem(f"{where!r} in the destination is a file, not a folder") from None
        st = os.fstat(fd)
        owner = self._identity.setdefault((st.st_dev, st.st_ino), parts)
        if owner != parts:
            os.close(fd)
            raise _PathProblem(
                f"the folder {'/'.join(parts)!r} is the same folder on this disk as {'/'.join(owner) or '.'!r} "
                "(the destination does not tell these names apart)"
            )
        if len(self._cache) >= 64:
            # Keys arrive sorted, so only folders on the current chain are reused.
            for other in [p for p in self._cache if parts[: len(p)] != p]:
                os.close(self._cache.pop(other))
        self._cache[parts] = fd
        return fd

    def close(self) -> None:
        for fd in self._cache.values():
            try:
                os.close(fd)
            except OSError:
                pass
        self._cache.clear()


class _PathProblem(Exception):
    pass


def _case_insensitive(root_fd: int) -> bool:
    name = f"{_TMP_PREFIX}case-{secrets.token_hex(4)}"
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW | _O_CLOEXEC, 0o600, dir_fd=root_fd)
    os.close(fd)
    try:
        try:
            os.stat(name.upper(), dir_fd=root_fd, follow_symlinks=False)
            return True
        except FileNotFoundError:
            return False
    finally:
        os.unlink(name, dir_fd=root_fd)


def _key_conflicts(keys: list[str], case_insensitive: bool) -> dict[str, str]:
    """Keys that cannot be written as files, with the reason."""
    folders: dict[str, str] = {}  # folder path -> one key inside it
    for key in keys:
        segments = key.split("/")
        for i in range(1, len(segments)):
            folders.setdefault("/".join(segments[:i]), key)
    conflicts: dict[str, str] = {}
    for key in keys:
        if key in folders:
            conflicts[key] = (
                f"it is also a folder of other keys (for example {folders[key]!r}), "
                "and a path cannot be both a file and a folder"
            )
    if case_insensitive:
        owners: dict[str, set[str]] = {}
        for path in list(folders) + keys:
            owners.setdefault(_keys.fold_path(path), set()).add(path)
        for key in keys:
            if key in conflicts:
                continue
            segments = key.split("/")
            for i in range(1, len(segments) + 1):
                path = "/".join(segments[:i])
                clash = owners.get(_keys.fold_path(path), set()) - {path}
                if clash:
                    other = sorted(clash)[0]
                    conflicts[key] = (
                        f"{path!r} differs only in letter case from {other!r}, and the "
                        "destination filesystem does not tell them apart"
                    )
                    break
    return conflicts


def _list_keys(bucket, prefix: str) -> list[str]:
    keys: list[str] = []
    start_after = None
    while True:
        page = bucket.list(prefix=prefix, limit=_LIST_PAGE, start_after=start_after)
        keys.extend(o.key for o in page.objects)
        if not page.truncated:
            return keys
        start_after = page.next_start_after


def _existing_state(dir_fd: int, name: str, size: int, etag: str) -> str:
    """'missing', 'same', 'different' (regular file), 'folder' or 'other'."""
    try:
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return "missing"
    if stat.S_ISDIR(st.st_mode):
        return "folder"
    if not stat.S_ISREG(st.st_mode):
        return "other"
    if st.st_size != size:
        return "different"
    fd = os.open(name, os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC | _O_NONBLOCK, dir_fd=dir_fd)
    try:
        return "same" if _file_sha256(fd).hex() == etag else "different"
    finally:
        os.close(fd)


def _identity_of(dir_fd: int, name: str) -> tuple[int, int] | None:
    try:
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    return st.st_dev, st.st_ino


def _write_object(stream, dir_fd: int, name: str, modified_ms: int) -> tuple[int, int]:
    """Write the stream to ``name`` (temp file, fsync, rename); returns the new file's identity."""
    tmp = f"{_TMP_PREFIX}{secrets.token_hex(8)}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW | _O_CLOEXEC, _FILE_MODE, dir_fd=dir_fd)
    try:
        try:
            while True:
                chunk = stream.read(CHUNK)
                if not chunk:
                    break
                _write_all(fd, chunk)
            fsync_file(fd)
            ns = int(modified_ms) * 1_000_000
            os.utime(fd, ns=(ns, ns))
            st = os.fstat(fd)
        finally:
            os.close(fd)
        os.rename(tmp, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
    except BaseException:
        try:
            os.unlink(tmp, dir_fd=dir_fd)
        except OSError:
            pass
        raise
    return st.st_dev, st.st_ino


def export_dir(
    bucket,
    dest: str | os.PathLike,
    prefix: str = "",
    overwrite: bool = False,
    skip_conflicts: bool = False,
) -> dict:
    """Write every current object under ``prefix`` as a plain file at its key path under ``dest``.

    Returns counts: ``written``, ``unchanged`` (an identical file was already
    there), ``skipped`` (conflicts left out with ``skip_conflicts``), ``gone``
    (deleted while the export ran), ``bytes`` (content size of all objects
    exported) and ``written_bytes``. ``conflicts`` lists up to 50 skipped keys
    with the reason. See the module docstring for the safety rules.
    """
    prefix = _keys.validate_prefix(prefix)
    dest_path = Path(dest).expanduser()
    if dest_path.exists() and not dest_path.is_dir():
        raise InvalidArgument(
            f"The export destination {dest_path} is a file.",
            hint="Give a folder that is empty, new or holds an earlier export.",
        )
    _check_not_store(bucket.store.path, dest_path, "export")
    dest_path.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)

    keys = _list_keys(bucket, prefix)
    root_fd = os.open(dest_path, os.O_RDONLY | _O_DIRECTORY | _O_CLOEXEC)
    dirs = _Dirs(root_fd)
    manifest_tmp = f"{_TMP_PREFIX}manifest-{secrets.token_hex(4)}"
    manifest_fd = None
    try:
        conflicts = _key_conflicts(keys, _case_insensitive(root_fd))
        if conflicts and not skip_conflicts:
            names = sorted(conflicts)
            first = names[0]
            raise ExportConflict(
                f"Nothing was exported: {_count(len(names), 'key')} cannot be written as plain files "
                f"({_show_list(names)}). For example {first!r}: {conflicts[first]}.",
                hint=ExportConflict.HINT_CLASH,
            )

        manifest_fd = os.open(
            manifest_tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW | _O_CLOEXEC, _FILE_MODE, dir_fd=root_fd
        )
        header = {
            "format": MANIFEST_FORMAT,
            "version": MANIFEST_VERSION,
            "bucket": bucket.name,
            "prefix": prefix,
            "exported_at": bucket._now(),
        }
        _write_all(manifest_fd, (json.dumps(header, sort_keys=True) + "\n").encode())

        result = {
            "bucket": bucket.name,
            "destination": str(dest_path),
            "prefix": prefix,
            "objects": len(keys),
            "written": 0,
            "unchanged": 0,
            "skipped": 0,
            "gone": 0,
            "bytes": 0,
            "written_bytes": 0,
            "conflicts": [],
            "manifest": str(dest_path / MANIFEST_NAME),
        }
        late_conflicts: list[str] = []
        damaged: list[str] = []
        files_of_this_run: dict[tuple[int, int], str] = {}  # identity -> the key exported there

        def conflict(key: str, reason: str) -> None:
            result["skipped"] += 1
            if len(result["conflicts"]) < _MAX_LISTED:
                result["conflicts"].append({"key": key, "reason": reason})

        for key in sorted(conflicts):
            conflict(key, conflicts[key])
        for key in keys:
            if key in conflicts:
                continue
            segments = _keys.key_segments(key)  # the one validator, again, right before use
            try:
                dir_fd = dirs.open(tuple(segments[:-1]))
            except _PathProblem as exc:
                conflict(key, str(exc))
                late_conflicts.append(key)
                continue
            name = segments[-1]
            try:
                stream = bucket.open(key)
            except ObjectNotFound:
                result["gone"] += 1
                continue
            except (DamagedError, UnsupportedFormat):
                damaged.append(key)  # e.g. its blob file is missing
                continue
            with stream:
                info = stream.info
                identity = _identity_of(dir_fd, name)
                if identity is not None and identity in files_of_this_run:
                    other = files_of_this_run[identity]
                    conflict(
                        key,
                        f"the destination does not tell {key!r} and {other!r} apart, and {other!r} "
                        "was already exported there",
                    )
                    late_conflicts.append(key)
                    continue
                try:
                    state = _existing_state(dir_fd, name, info.size, info.etag)
                except PermissionError as exc:
                    conflict(key, f"the existing file at {key!r} cannot be read ({exc.strerror})")
                    late_conflicts.append(key)
                    continue
                if state == "same":
                    result["unchanged"] += 1
                    if identity is not None:
                        files_of_this_run[identity] = key
                elif state == "folder" or (state != "missing" and not overwrite):
                    what = {"folder": "a folder", "other": "a link or special file"}.get(state, "a different file")
                    conflict(key, f"the destination already has {what} at {key!r}")
                    late_conflicts.append(key)
                    continue
                else:
                    try:
                        files_of_this_run[_write_object(stream, dir_fd, name, info.modified)] = key
                    except DamagedError:
                        damaged.append(key)
                        continue
                    except PermissionError as exc:
                        conflict(key, f"the file {key!r} cannot be written ({exc.strerror})")
                        late_conflicts.append(key)
                        continue
                    dirs.touched.add(tuple(segments[:-1]))
                    result["written"] += 1
                    result["written_bytes"] += info.size
            result["bytes"] += info.size
            entry = {
                "key": key,
                "etag": info.etag,
                "size": info.size,
                "content_type": info.content_type,
                "metadata": info.metadata,
                "modified": info.modified,
                "version": info.version,
            }
            _write_all(manifest_fd, (json.dumps(entry, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8"))

        fsync_file(manifest_fd)
        os.close(manifest_fd)
        manifest_fd = None
        os.rename(manifest_tmp, MANIFEST_NAME, src_dir_fd=root_fd, dst_dir_fd=root_fd)
        for parts in sorted(dirs.touched | {()}, key=len, reverse=True):
            try:
                _fsync_dir_fd(dirs.open(parts))
            except _PathProblem:
                pass

        done = f"Exported {_count(result['written'], 'object')} ({result['unchanged']} already there)"
        if damaged:
            raise DamagedError(
                f"{done}, but {_count(len(damaged), 'object')} damaged and not written: {_show_list(damaged)}.",
                hint="Run verify on this bucket to see the damage, and restore it from a snapshot or backup.",
            )
        if late_conflicts and not skip_conflicts:
            raise ExportConflict(
                f"{done}, but {_count(len(late_conflicts), 'object')} not written because something "
                f"different is already at their path in the destination: {_show_list(late_conflicts)}.",
            )
        return result
    except BaseException:
        if manifest_fd is not None:
            os.close(manifest_fd)
            try:
                os.unlink(manifest_tmp, dir_fd=root_fd)
            except OSError:
                pass
        raise
    finally:
        dirs.close()
        os.close(root_fd)


# -- import --------------------------------------------------------------------


def _read_manifest(src: Path) -> dict[str, dict]:
    """Key -> manifest entry from ``src/.mybucket-export.jsonl``, or {} when absent or unreadable."""
    path = src / MANIFEST_NAME
    try:
        fd = os.open(path, os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC | _O_NONBLOCK)
    except OSError:
        return {}
    entries: dict[str, dict] = {}
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return {}
        with os.fdopen(fd, "r", encoding="utf-8", closefd=False) as fp:
            header = json.loads(fp.readline() or "null")
            if not isinstance(header, dict) or header.get("format") != MANIFEST_FORMAT:
                return {}
            if not isinstance(header.get("version"), int) or header["version"] > MANIFEST_VERSION:
                return {}
            for line in fp:
                entry = json.loads(line)
                if isinstance(entry, dict) and isinstance(entry.get("key"), str):
                    entries[entry["key"]] = entry
    except (ValueError, OSError, UnicodeDecodeError):
        return {}
    finally:
        os.close(fd)
    return entries


class _Guesser:
    def __init__(self) -> None:
        # MimeTypes() starts from Python's built-in table only, so a guess does
        # not depend on the machine's /etc/mime.types.
        self._types = mimetypes.MimeTypes()

    def __call__(self, key: str) -> str | None:
        guessed, encoding = self._types.guess_type("x/" + key.rsplit("/", 1)[-1], strict=True)
        if encoding is not None:
            return None  # "data.json.gz" is not JSON
        return guessed


class _Importer:
    def __init__(self, bucket, prefix: str, manifest: dict[str, dict], guess) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.manifest = manifest
        self.guess = guess
        self.pending = 0  # files queued in the open batch
        self.batch = None
        self.batch_bytes = 0
        self.result = {
            "bucket": bucket.name,
            "source": "",
            "prefix": prefix,
            "files": 0,
            "added": 0,
            "updated": 0,
            "unchanged": 0,
            "repaired": 0,
            "skipped": 0,
            "bytes": 0,
            "new_bytes": 0,
            "skipped_entries": [],
        }

    def skip(self, path: str, reason: str) -> None:
        self.result["skipped"] += 1
        if len(self.result["skipped_entries"]) < _MAX_LISTED:
            self.result["skipped_entries"].append({"path": path, "reason": reason})

    def _wanted_attrs(self, rel: str, key: str, current) -> tuple[str | None, dict[str, str] | None]:
        # Export writes each object at its key path, so manifest keys are relative paths
        # (in NFC form, like every key; the names on disk may have been decomposed since).
        entry = self.manifest.get(unicodedata.normalize("NFC", rel))
        if entry is not None:
            ctype = entry.get("content_type")
            meta = entry.get("metadata") or None
            try:
                return _keys.validate_content_type(ctype), (meta if _keys.validate_metadata(meta) else None)
            except MyBucketError:
                pass  # a hand-edited manifest: fall through to the other sources
        if current is not None:
            return current.content_type, (current.metadata or None)
        return (self.guess(key) if self.guess else None), None

    def add_file(self, rel: str, dir_fd: int, name: str) -> None:
        try:
            key = _keys.validate_key(self.prefix + rel)
        except InvalidKey as exc:
            self.skip(rel, exc.message)
            return
        try:
            fd = os.open(name, os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC | _O_NONBLOCK, dir_fd=dir_fd)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                self.skip(rel, "it is a symbolic link, which import never follows")
            else:
                self.skip(rel, f"it cannot be opened ({exc.strerror})")
            return
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                self.skip(rel, "it is not a regular file")
                return
            self.result["files"] += 1
            self.result["bytes"] += st.st_size
            try:
                current = self.bucket.head(key)
            except ObjectNotFound:
                current = None
            ctype, meta = self._wanted_attrs(rel, key, current)
            if (
                current is not None
                and current.size == st.st_size
                and current.content_type == ctype
                and (current.metadata or None) == meta
                and _file_sha256(fd).hex() == current.etag
                and not self.bucket._stored_copy_damaged(bytes.fromhex(current.etag))
            ):
                self.result["unchanged"] += 1
                return
            os.lseek(fd, 0, os.SEEK_SET)
            if self.batch is None:
                self.batch = self.bucket.batch()
                self.batch.__enter__()
            with os.fdopen(fd, "rb", closefd=False) as fp:
                self.batch.put(key, fp, content_type=ctype, metadata=meta)
            self.pending += 1
            self.batch_bytes += st.st_size
        finally:
            os.close(fd)
        if self.pending >= _BATCH_FILES or self.batch_bytes >= _BATCH_BYTES:
            self.flush()

    def flush(self) -> None:
        batch, self.batch = self.batch, None
        self.pending = 0
        self.batch_bytes = 0
        if batch is None:
            return
        batch.__exit__(None, None, None)
        for info in batch.results or []:
            if getattr(info, "repaired", False):
                self.result["repaired"] += 1
            if info.status == STATUS_CREATED:
                self.result["added"] += 1
                self.result["new_bytes"] += info.size
            elif info.status == STATUS_UPDATED:
                self.result["updated"] += 1
                self.result["new_bytes"] += info.size
            elif info.status == STATUS_UNCHANGED:
                self.result["unchanged"] += 1

    def abort(self) -> None:
        batch, self.batch = self.batch, None
        self.pending = 0
        if batch is not None:
            exc = sys.exc_info()
            batch.__exit__(exc[0] or RuntimeError, exc[1], exc[2])


def _same_key_names(names: list[str]) -> dict[str, str]:
    """Entries of one folder whose names differ on disk but are the same key in NFC form."""
    groups: dict[str, list[str]] = {}
    for name in names:
        groups.setdefault(unicodedata.normalize("NFC", name), []).append(name)
    clashes: dict[str, str] = {}
    for group in groups.values():
        if len(group) > 1:
            shown = " and ".join(repr(n) for n in sorted(group))
            for name in group:
                clashes[name] = f"the names {shown} are different on disk but the same key in Unicode normal form; rename one"
    return clashes


def import_dir(bucket, src: str | os.PathLike, prefix: str = "", guess_types: bool = True) -> dict:
    """Load every regular file under ``src`` into ``bucket`` (key = prefix + relative path).

    Idempotent. Returns counts: ``files`` (regular files read), ``added``,
    ``updated``, ``unchanged``, ``repaired`` (damaged stored copies replaced
    by the file's bytes), ``skipped`` (entries that cannot become keys),
    ``bytes`` (size of all files read) and ``new_bytes`` (size of files
    added or updated). ``skipped_entries`` lists up to 50 skipped paths with
    the reason. See the module docstring for the rules.
    """
    if bucket.readonly:
        raise ReadOnlyError(f"Cannot import into bucket {bucket.name!r}: the store is open read-only.")
    prefix = _normalize_import_prefix(prefix)
    src_path = Path(src).expanduser()
    if not src_path.is_dir():
        if src_path.exists():
            raise InvalidArgument(
                f"The import source {src_path} is not a folder.",
                hint="Give a folder; to store a single file use put.",
            )
        raise NotFound(f"The import source folder {src_path} does not exist.", hint="Check the path.")
    _check_not_store(bucket.store.path, src_path, "import")

    # The folder given may itself be a link (the user named it); links inside it are never followed.
    walk_root = os.path.realpath(src_path)
    manifest = _read_manifest(Path(walk_root))
    importer = _Importer(bucket, prefix, manifest, _Guesser() if guess_types else None)
    importer.result["source"] = str(src_path)
    try:
        for dirpath, dirnames, filenames, dir_fd in os.fwalk(walk_root, follow_symlinks=False):
            rel_dir = os.path.relpath(dirpath, walk_root)
            rel_dir = "" if rel_dir == "." else rel_dir.replace(os.sep, "/") + "/"
            same_key = _same_key_names(dirnames + filenames)
            keep = []
            for name in sorted(dirnames):
                rel = rel_dir + name
                if name in same_key:
                    importer.skip(rel + "/", same_key[name] + ", so everything in it was skipped")
                    continue
                try:
                    st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
                except OSError as exc:
                    importer.skip(rel + "/", f"it cannot be read ({exc.strerror})")
                    continue
                if stat.S_ISLNK(st.st_mode):
                    importer.skip(rel, "it is a symbolic link, which import never follows")
                    continue
                try:
                    _keys.validate_key(prefix + rel)
                except InvalidKey as exc:
                    importer.skip(
                        rel + "/",
                        f"the folder name cannot be part of a key, so everything in it was skipped: {exc.message}",
                    )
                    continue
                keep.append(name)
            dirnames[:] = keep
            for name in sorted(filenames):
                if not rel_dir and name == MANIFEST_NAME:
                    continue  # export's own record, never an object
                rel = rel_dir + name
                if name in same_key:
                    importer.skip(rel, same_key[name])
                    continue
                try:
                    st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
                except OSError as exc:
                    importer.skip(rel, f"it cannot be read ({exc.strerror})")
                    continue
                if stat.S_ISLNK(st.st_mode):
                    importer.skip(rel, "it is a symbolic link, which import never follows")
                    continue
                importer.add_file(rel, dir_fd, name)
        importer.flush()
    except BaseException:
        importer.abort()
        raise
    return importer.result


__all__ = ["export_dir", "import_dir", "ExportConflict", "MANIFEST_NAME", "inside_store"]
