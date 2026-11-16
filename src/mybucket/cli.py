"""The ``mybucket`` command-line tool.

Usage::

    mybucket [--store PATH] [--json] [--timeout SECONDS] COMMAND ...

The store path comes from ``--store`` or the ``MYBUCKET_STORE`` environment
variable; it may be ``ssh://host/path`` for a store on another machine (data
commands only; see mybucket.remote). ``--json`` (accepted before or after the command) makes stdout
carry only JSON: one object per run. Errors are then printed as
``{"error": {"code", "message", "hint"}}``; without ``--json`` they go to
stderr as plain text.

Exit codes: 0 ok, 1 error, 2 usage, 3 not found, 4 not allowed, 5 refused
for safety (for example a failed precondition), 75 temporary (the bucket is
locked by another writer; retry).

Commands that only read open the store read-only, so they can never change
it. gc, verify, snapshot and restore call ``mybucket.maintenance``; ``serve``
is the serving side of remote access (mybucket.server), ``sync`` copies
between stores (mybucket.replication), ``dict`` manages trained dictionaries
(mybucket.dicts).

Public: ``main(argv=None) -> int``.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import secrets
import shlex
import sys
from collections.abc import Callable
from pathlib import Path

from . import __version__
from .blobs import fsync_dir, fsync_file
from .errors import (
    EXIT_ERROR,
    EXIT_NOT_ALLOWED,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_REFUSED,
    EXIT_USAGE,
    BucketNotEmpty,
    BucketNotFound,
    DamagedError,
    InvalidArgument,
    MyBucketError,
    NotAvailable,
    NotFound,
    ObjectNotFound,
    PreconditionFailed,
    StoreNotFound,
    VersionNotFound,
)
from .store import DEFAULT_TRASH_DAYS, Store, open_store

ENV_STORE = "MYBUCKET_STORE"
CHUNK = 1024 * 1024
_LS_PAGE = 1000


# -- errors and output ---------------------------------------------------------


class UsageError(InvalidArgument):
    code = "usage"


class FolderNotFound(NotFound):
    code = "folder_not_found"


class _ParserError(Exception):
    def __init__(self, message: str, usage: str, prog: str) -> None:
        super().__init__(message)
        self.message = message
        self.usage = usage
        self.prog = prog


class _Parser(argparse.ArgumentParser):
    def error(self, message: str):  # type: ignore[override]
        raise _ParserError(message, self.format_usage(), self.prog)


def _plural(n: int, word: str, many: str | None = None) -> str:
    return f"{n} {word if n == 1 else (many or word + 's')}"


_TYPE_WORDS = {"int": "a whole number such as 3", "float": "a number such as 2.5"}


def _usage_message(message: str) -> str:
    """argparse's wording, rewritten as a plain sentence."""
    m = re.fullmatch(r"argument (\S+?)(?:/\S+)?: invalid (int|float) value: (.*)", message)
    if m:
        name, kind, value = m.groups()
        label = f"The value of {name}" if name.startswith("-") else f"The {name.replace('_', ' ')}"
        return f"{label} must be {_TYPE_WORDS[kind]}; got {value}."
    m = re.fullmatch(r"the following arguments are required: (.*)", message)
    if m:
        names = [n.strip().upper() for n in m.group(1).split(",")]
        return f"Missing {' and '.join(names)}."
    m = re.fullmatch(r"argument COMMAND: invalid choice: '([^']*)' \(choose from (.*)\)", message)
    if m:
        commands = ", ".join(c.strip().strip("'") for c in m.group(2).split(","))
        return f"There is no command {m.group(1)!r}. The commands are: {commands}."
    m = re.fullmatch(r"unrecognized arguments: (.*)", message)
    if m:
        return f"Unexpected extra arguments or unknown options: {m.group(1)}."
    return message[:1].upper() + message[1:] + "."


# SDK hints name keyword arguments; on the command line the options say the same thing.
_CLI_HINTS = {
    BucketNotEmpty.default_hint: (
        "Delete or export its objects first, or run 'mybucket rb --force NAME' to remove "
        "everything in it, including its trash."
    ),
    BucketNotFound.default_hint: (
        "Run 'mybucket buckets' to see the names that exist, or create it with 'mybucket mb NAME'."
    ),
    ObjectNotFound.default_hint: "Run 'mybucket ls BUCKET' to see which keys exist.",
    "List the bucket to see which keys exist. To ignore missing keys, pass missing_ok=True.": (
        "Run 'mybucket ls BUCKET' to see which keys exist, or pass -f to ignore missing keys."
    ),
    VersionNotFound.default_hint: "Run 'mybucket versions BUCKET KEY' to see which versions are still kept.",
    PreconditionFailed.default_hint: (
        "Someone else may have changed the object. Read it again ('mybucket head BUCKET KEY') "
        "and retry with the current ETag, or drop the condition."
    ),
    PreconditionFailed.HINT_EXISTS: (
        "The key already exists, so it was left unchanged. Use another key, or read the existing "
        "object if you meant to update it (pass --if-match with its ETag)."
    ),
    PreconditionFailed.HINT_MISSING: (
        "The object was deleted or never existed. Create it with --if-none-match or without a condition."
    ),
    DamagedError.default_hint: (
        "Run 'mybucket verify' to see everything affected. If you still have the original data, "
        "put it again ('mybucket put'): that repairs the stored copy. Otherwise restore the bucket "
        "from a snapshot or backup."
    ),
}


def _cli_hints() -> dict[str, str]:
    from .transfer import ExportConflict

    hints = dict(_CLI_HINTS)
    hints[ExportConflict.default_hint] = (
        "Move the existing files out of the way, pass --overwrite to replace existing files "
        "that differ, or --skip-conflicts to export everything else."
    )
    hints[ExportConflict.HINT_CLASH] = (
        "Pass --skip-conflicts to export everything else (the keys left out are listed), or "
        "export to a case-sensitive volume for letter-case clashes."
    )
    return hints


_UNSAFE_TEXT_RE = re.compile(
    "[\x00-\x1f\x7f-\x9f\u061c\u200b\u200e\u200f\u2028\u2029\u202a-\u202e\u2066-\u2069\ufeff\ud800-\udfff]"
)


def _escape_char(m: re.Match) -> str:
    c = ord(m.group())
    return f"\\x{c:02x}" if c < 0x100 else f"\\u{c:04x}"


def _safe(text: object) -> str:
    """``text`` for a terminal: control and invisible characters shown as \\xNN / \\uNNNN.

    A store from elsewhere may hold keys, content types or metadata with
    characters no valid key has (terminal escape sequences, direction
    overrides); printing them raw could rewrite what the screen shows.
    """
    return _UNSAFE_TEXT_RE.sub(_escape_char, str(text))


def _human_size(n: int | None) -> str:
    if n is None:
        return "-"
    if n < 1024:
        return f"{n} B"
    value = float(n)
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        value /= 1024
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
    return f"{n} B"  # pragma: no cover


def _when(ms: int | None) -> str:
    if ms is None:
        return "-"
    return _dt.datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


class Context:
    def __init__(self, ns: argparse.Namespace) -> None:
        self.ns = ns
        self.json = bool(getattr(ns, "json", False))
        self.store_arg = getattr(ns, "store", None)
        self.timeout = getattr(ns, "timeout", 10.0)
        self._stores: dict[bool, Store] = {}

    # output
    def emit(self, data: dict, human: str | list[str] | None = None) -> None:
        """Print the result: ``data`` as JSON with --json, else the human text."""
        if self.json:
            print(json.dumps(data, ensure_ascii=True), flush=True)
            return
        if human is None:
            return
        lines = [human] if isinstance(human, str) else human
        for line in lines:
            print(line)
        sys.stdout.flush()

    def note(self, text: str) -> None:
        """A side remark for people: stderr, and never in --json mode."""
        if not self.json:
            print(text, file=sys.stderr)

    # store
    def store_text(self) -> str:
        path = self.store_arg or os.environ.get(ENV_STORE)
        if not path:
            raise UsageError(
                "No store given.",
                hint=f"Pass --store PATH or set the {ENV_STORE} environment variable.",
            )
        return path

    @property
    def remote(self) -> bool:
        return self.store_text().startswith("ssh://")

    def store_path(self) -> Path:
        return Path(self.store_text()).expanduser()

    def local_only(self, command: str) -> None:
        """Maintenance commands work on the machine that holds the store, not over ssh://."""
        if self.remote:
            raise NotAvailable(
                f"'mybucket {command}' works only on a store on this machine, not on {self.store_text()}.",
                hint="Run it on the machine that holds the store (for example over ssh).",
            )

    def store(self, write: bool = False, create: bool = False) -> Store:
        """The store, read-only unless ``write``; only ``create`` may make a new one."""
        store = self._stores.get(write)
        if store is None:
            path = self.store_text() if self.remote else self.store_path()
            try:
                store = open_store(path, readonly=not write, create=create, timeout=self.timeout)
            except StoreNotFound as exc:
                raise StoreNotFound(
                    exc.message,
                    hint="Check --store or MYBUCKET_STORE. To start a new store there, create a bucket: mybucket mb NAME",
                ) from None
            self._stores[write] = store
        return store

    def bucket(self, name: str, write: bool = False):
        return self.store(write=write).bucket(name)

    def close(self) -> None:
        for store in self._stores.values():
            store.close()
        self._stores.clear()


def _maintenance(name: str, command: str) -> Callable:
    try:
        from . import maintenance
    except ImportError:
        maintenance = None
    func = getattr(maintenance, name, None)
    if func is None:
        raise NotAvailable(
            f"The '{command}' command is not available: this installation of mybucket has no "
            f"mybucket.maintenance.{name}.",
            hint="Install a complete, up-to-date mybucket release, then retry.",
        )
    return func


def _plain(value):
    """Make a maintenance result JSON-friendly (dataclasses, paths, bytes)."""
    if hasattr(value, "to_dict"):
        return _plain(value.to_dict())
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_plain(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _describe(data: dict, indent: str = "  ") -> list[str]:
    """Generic human rendering of a nested result dict."""
    lines: list[str] = []
    for key, value in data.items():
        label = str(key).replace("_", " ")
        if isinstance(value, dict):
            if not value:
                lines.append(f"{indent}{label}: none")
                continue
            lines.append(f"{indent}{label}:")
            lines.extend(_describe(value, indent + "  "))
        elif isinstance(value, list):
            if not value:
                lines.append(f"{indent}{label}: none")
                continue
            lines.append(f"{indent}{label}:")
            for item in value:
                if isinstance(item, dict):
                    lines.append(f"{indent}  - " + ", ".join(f"{k}: {v}" for k, v in item.items()))
                else:
                    lines.append(f"{indent}  - {item}")
        else:
            if isinstance(value, int) and not isinstance(value, bool) and key.endswith("bytes"):
                value = f"{value} ({_human_size(value)})"
            lines.append(f"{indent}{label}: {value}")
    return lines


def _found_damage(report: dict) -> bool:
    """verify() reports ``ok``; fall back to a non-empty ``damaged`` list."""
    if isinstance(report.get("ok"), bool):
        return not report["ok"]
    return bool(report.get("damaged"))


# -- commands --------------------------------------------------------------------


def cmd_buckets(ctx: Context, ns) -> int:
    names = ctx.store().list_buckets()
    ctx.emit({"buckets": names}, names if names else None)
    if not names:
        ctx.note("The store has no buckets yet. Create one with: mybucket mb NAME")
    return EXIT_OK


def cmd_mb(ctx: Context, ns) -> int:
    bucket = ctx.store(write=True, create=True).create_bucket(ns.bucket, trash_days=ns.trash_days)
    days = bucket.trash_days
    kept = f"old versions are kept for {days} days" if days else "old versions are not kept"
    ctx.emit({"bucket": bucket.name, "created": True, "trash_days": days}, f"Created bucket {bucket.name!r} ({kept}).")
    return EXIT_OK


def cmd_rb(ctx: Context, ns) -> int:
    ctx.local_only("rb")
    ctx.store(write=True).delete_bucket(ns.bucket, force=ns.force)
    ctx.emit({"bucket": ns.bucket, "deleted": True}, f"Deleted bucket {ns.bucket!r}.")
    return EXIT_OK


def _parse_meta(items: list[str] | None) -> dict[str, str] | None:
    if not items:
        return None
    meta: dict[str, str] = {}
    for item in items:
        name, sep, value = item.partition("=")
        if not sep:
            raise UsageError(f"Metadata {item!r} is not NAME=VALUE.", hint="Write it like: --meta source=phone")
        meta[name] = value
    return meta


def cmd_put(ctx: Context, ns) -> int:
    bucket = ctx.bucket(ns.bucket, write=True)
    content_type = ns.content_type
    if content_type is None and not ns.no_guess_type:
        from .transfer import _Guesser

        content_type = _Guesser()(ns.key)
    metadata = _parse_meta(ns.meta)
    source = ns.source
    if source == "-":
        if ns.source_given is False and sys.stdin.isatty():
            raise UsageError(
                "Nothing to store: no file was given and no data is being piped in.",
                hint="Give a file (mybucket put BUCKET KEY FILE) or pipe data in (... | mybucket put BUCKET KEY).",
            )
        info = bucket.put(
            ns.key,
            sys.stdin.buffer,
            content_type=content_type,
            metadata=metadata,
            if_match=ns.if_match,
            if_none_match=ns.if_none_match,
            expected_size=ns.expect_size,
            expected_sha256=ns.expect_sha256,
        )
    else:
        path = Path(source).expanduser()
        if path.is_dir():
            raise UsageError(
                f"{path} is a folder.", hint="Use 'mybucket import BUCKET FOLDER' to store a whole folder."
            )
        with open(path, "rb") as fp:
            info = bucket.put(
                ns.key,
                fp,
                content_type=content_type,
                metadata=metadata,
                if_match=ns.if_match,
                if_none_match=ns.if_none_match,
                expected_size=ns.expect_size,
                expected_sha256=ns.expect_sha256,
            )
    data = info.to_dict()
    if info.status == "unchanged":
        human = f"unchanged {_safe(info.key)} (the same content and metadata are already stored)"
    else:
        human = (
            f"{info.status} {_safe(info.key)} ({_human_size(info.size)}, stored as {_human_size(info.stored_size)}, "
            f"version {info.version}, etag {info.etag})"
        )
    if info.repaired:
        human += "; the damaged stored copy of this content was repaired"
    ctx.emit(data, human)
    return EXIT_OK


def _copy_stream(stream, write) -> int:
    total = 0
    while True:
        chunk = stream.read(CHUNK)
        if not chunk:
            return total
        write(chunk)
        total += len(chunk)


def _range(ns) -> tuple[int | None, int | None]:
    start, end = getattr(ns, "start", None), getattr(ns, "end", None)
    if (start is not None and start < 0) or (end is not None and end < 0):
        raise UsageError("--start and --end must be byte offsets of 0 or more.")
    if start is not None and end is not None and end < start:
        raise UsageError(f"--end ({end}) is before --start ({start}).")
    return start, end


def _to_stdout(ctx: Context, bucket, key: str, start=None, end=None) -> None:
    with bucket.open(key, start, end) as stream:
        out = sys.stdout.buffer
        try:
            _copy_stream(stream, out.write)
        finally:
            out.flush()


def cmd_get(ctx: Context, ns) -> int:
    if ns.dest == "-":
        if ctx.json:
            raise UsageError(
                "With --json, stdout carries only JSON, so get needs a destination file.",
                hint="Give a file: mybucket --json get BUCKET KEY FILE",
            )
        _to_stdout(ctx, ctx.bucket(ns.bucket), ns.key, *_range(ns))
        return EXIT_OK
    from .transfer import inside_store

    bucket = ctx.bucket(ns.bucket)
    given = ns.dest
    dest = Path(given).expanduser()
    name = ns.key.rsplit("/", 1)[-1]
    if given.endswith(("/", os.sep)):
        if not dest.is_dir():
            raise FolderNotFound(
                f"Cannot write {ns.key} into {given}: that folder does not exist.",
                hint=f"Create it first (mkdir -p {shlex.quote(given)}) or choose another destination.",
            )
        dest = dest / name
    elif dest.is_dir():
        dest = dest / name
    parent = dest.parent
    if not parent.is_dir():
        raise FolderNotFound(
            f"Cannot write {dest}: the folder {parent} does not exist.",
            hint=f"Create it first (mkdir -p {shlex.quote(str(parent))}) or choose another destination.",
        )
    if not ctx.remote and inside_store(ctx.store_path(), dest):
        raise UsageError(
            f"Refusing to write {dest}: it is inside the store, and writing there could damage it.",
            hint="Choose a destination outside the store directory.",
        )
    tmp = parent / f".{dest.name}.mybucket-{secrets.token_hex(4)}.part"
    with bucket.open(ns.key, *_range(ns)) as stream:
        info = stream.info
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wb") as fp:
                _copy_stream(stream, fp.write)
                fp.flush()
                fsync_file(fp.fileno())  # F_FULLFSYNC on macOS
            os.replace(tmp, dest)
            fsync_dir(parent)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
    data = info.to_dict()
    data["path"] = str(dest)
    if stream.start or stream.end != info.size:
        data["range"] = {"start": stream.start, "end": stream.end}
        ctx.emit(data, f"Wrote bytes {stream.start}-{stream.end} of {_safe(info.key)} to {_safe(dest)} ({_human_size(stream.end - stream.start)}).")
    else:
        ctx.emit(data, f"Wrote {_safe(info.key)} to {_safe(dest)} ({_human_size(info.size)}).")
    return EXIT_OK


def cmd_cat(ctx: Context, ns) -> int:
    if ctx.json:
        raise UsageError(
            "cat writes the raw content to stdout, which cannot be combined with --json.",
            hint="Use 'mybucket --json get BUCKET KEY FILE' or 'mybucket --json head BUCKET KEY'.",
        )
    _to_stdout(ctx, ctx.bucket(ns.bucket), ns.key, *_range(ns))
    return EXIT_OK


def _info_lines(info) -> list[str]:
    lines = [
        f"key:          {_safe(info.key)}",
        f"size:         {info.size} ({_human_size(info.size)})",
        f"stored size:  {'-' if info.stored_size is None else info.stored_size} ({_human_size(info.stored_size)})",
        f"etag:         {info.etag}",
        f"content type: {_safe(info.content_type or '-')}",
        f"modified:     {_when(info.modified)}",
        f"version:      {info.version}",
    ]
    if info.metadata:
        lines.append("metadata:")
        lines.extend(f"  {_safe(k)}: {_safe(v)}" for k, v in sorted(info.metadata.items()))
    return lines


def cmd_head(ctx: Context, ns) -> int:
    info = ctx.bucket(ns.bucket).head(ns.key)
    ctx.emit(info.to_dict(), _info_lines(info))
    return EXIT_OK


def cmd_ls(ctx: Context, ns) -> int:
    if ns.bucket is None:
        given = (ns.prefix, ns.prefix_opt, ns.delimiter, ns.limit, ns.start_after)
        if any(value is not None for value in given):
            raise UsageError("Listing options need a bucket.", hint="Write: mybucket ls BUCKET [PREFIX]")
        return cmd_buckets(ctx, ns)
    if ns.prefix is not None and ns.prefix_opt is not None and ns.prefix != ns.prefix_opt:
        raise UsageError("Two different prefixes were given.", hint="Give the prefix once, either way.")
    prefix = ns.prefix_opt if ns.prefix_opt is not None else (ns.prefix or "")
    if ns.limit is not None and ns.limit < 1:
        raise UsageError(f"--limit must be at least 1, got {ns.limit}.")
    bucket = ctx.bucket(ns.bucket)

    objects: list[dict] = []
    prefixes: list[str] = []
    start_after = ns.start_after
    remaining = ns.limit
    truncated = False
    next_start_after = None
    shown = 0
    while True:
        page_limit = _LS_PAGE if remaining is None else min(_LS_PAGE, remaining)
        page = bucket.list(prefix=prefix, delimiter=ns.delimiter, limit=page_limit, start_after=start_after)
        for entry in page:
            shown += 1
            if isinstance(entry, str):
                if ctx.json:
                    prefixes.append(entry)
                else:
                    print(f"{'':19}  {'PRE':>10}  {_safe(entry)}")
            else:
                if ctx.json:
                    objects.append(entry.to_dict())
                else:
                    print(f"{_when(entry.modified)}  {_human_size(entry.size):>10}  {_safe(entry.key)}")
        if remaining is not None:
            remaining -= len(page)
        if not page.truncated:
            break
        if remaining is not None and remaining <= 0:
            truncated, next_start_after = True, page.next_start_after
            break
        start_after = page.next_start_after
    if ctx.json:
        ctx.emit(
            {"objects": objects, "prefixes": prefixes, "truncated": truncated, "next_start_after": next_start_after}
        )
    else:
        sys.stdout.flush()
        if truncated:
            ctx.note(f"(more results; continue with --start-after {_safe(next_start_after)})")
        elif shown == 0:
            ctx.note("(nothing found)" if prefix else "(the bucket is empty)")
    return EXIT_OK


def cmd_rm(ctx: Context, ns) -> int:
    bucket = ctx.bucket(ns.bucket, write=True)
    if ns.if_match is not None and len(ns.keys) > 1:
        raise UsageError(
            "--if-match can only be used with a single key.",
            hint="Delete the keys one at a time when each needs its own --if-match.",
        )
    wanted = list(dict.fromkeys(ns.keys))
    if len(wanted) == 1:
        results = [bucket.delete(wanted[0], if_match=ns.if_match, missing_ok=ns.force)]
    else:
        with bucket.batch() as batch:  # all or nothing
            for key in wanted:
                batch.delete(key, missing_ok=ns.force)
        results = list(batch.results or [])
    deleted = [info for info in results if info is not None]
    missing = [key for key, info in zip(wanted, results, strict=True) if info is None]
    days = bucket.trash_days
    kept = f"kept in the trash for {days} days" if days else "not kept: this bucket's trash is off"
    lines = [f"deleted {_safe(info.key)} ({kept})" for info in deleted]
    lines += [f"not found {_safe(key)} (nothing to delete)" for key in missing]
    ctx.emit({"deleted": [i.to_dict() for i in deleted], "missing": missing}, lines)
    return EXIT_OK


def cmd_versions(ctx: Context, ns) -> int:
    versions = ctx.bucket(ns.bucket).list_versions(ns.key)
    lines = []
    for v in versions:
        if v.current:
            state = "current"
        else:
            state = f"{v.reason or 'trash'}, kept until {_when(v.expires_at)}"
        lines.append(f"v{v.version:<6} {_when(v.modified)}  {_human_size(v.size):>10}  {v.etag[:16]}  {state}")
    ctx.emit({"key": ns.key, "versions": [v.to_dict() for v in versions]}, lines)
    return EXIT_OK


def cmd_restore(ctx: Context, ns) -> int:
    bucket = ctx.bucket(ns.bucket, write=True)
    if ctx.remote:
        info = bucket.restore(ns.key, ns.version)
    else:
        info = _maintenance("restore", "restore")(bucket, ns.key, ns.version)
    data = _plain(info)
    if isinstance(data, dict) and data.get("status") == "unchanged":
        human = f"Nothing to restore: version {ns.version} of {ns.key} has the same content as the current one."
    elif isinstance(data, dict) and "version" in data:
        human = f"Restored version {ns.version} of {ns.key}; it is now version {data['version']}."
    else:
        human = f"Restored version {ns.version} of {ns.key}."
    ctx.emit(data if isinstance(data, dict) else {"result": data}, human)
    return EXIT_OK


def cmd_export(ctx: Context, ns) -> int:
    from .transfer import export_dir

    ctx.local_only("export")
    bucket = ctx.bucket(ns.bucket)
    result = export_dir(bucket, ns.dest, prefix=ns.prefix, overwrite=ns.overwrite, skip_conflicts=ns.skip_conflicts)
    human = (
        f"Exported {_plural(result['written'] + result['unchanged'], 'object')} ({_human_size(result['bytes'])}) "
        f"from {bucket.name!r} to {result['destination']}: {result['written']} written "
        f"({_human_size(result['written_bytes'])}), {result['unchanged']} already there"
    )
    if result["skipped"]:
        human += f", {result['skipped']} skipped"
    if result["gone"]:
        human += f", {result['gone']} deleted while exporting"
    ctx.emit(result, human + ".")
    for item in result["conflicts"][:20]:
        ctx.note(f"  skipped {_safe(item['key'])}: {_safe(item['reason'])}")
    if result["skipped"] > 20:
        ctx.note(f"  ... and {result['skipped'] - 20} more (use --json to see up to 50)")
    return EXIT_OK


def cmd_import(ctx: Context, ns) -> int:
    from .transfer import import_dir

    ctx.local_only("import")
    bucket = ctx.bucket(ns.bucket, write=True)
    result = import_dir(bucket, ns.src, prefix=ns.prefix, guess_types=not ns.no_guess_type)
    human = (
        f"Imported {_plural(result['files'], 'file')} ({_human_size(result['bytes'])}) from {result['source']} "
        f"into {bucket.name!r}: {result['added']} added, {result['updated']} updated, "
        f"{result['unchanged']} unchanged ({_human_size(result['new_bytes'])} new)."
    )
    if result.get("repaired"):
        human += f" Repaired {_plural(result['repaired'], 'damaged stored copy', 'damaged stored copies')}."
    skipped = result["skipped"]
    if skipped:
        human += f" WARNING: skipped {_plural(skipped, 'entry', 'entries')} that cannot become keys (listed below)."
    error = None
    if skipped and not ns.allow_skips:
        error = MyBucketError(
            f"{_plural(skipped, 'file or folder', 'files or folders')} under {result['source']} "
            "were not imported because their names cannot be keys.",
            hint="Rename them (a name must not start with '.', start or end with a space, or contain control "
            "characters or backslashes), or pass --allow-skips if leaving them out is fine.",
        )
        error.code = "files_skipped"
    code = _emit_with_damage(ctx, result, [human], error, quiet_error=True)
    for item in result["skipped_entries"][:20]:
        ctx.note(f"  skipped {_safe(item['path'])}: {_safe(item['reason'])}")
    if skipped > 20:
        ctx.note(f"  ... and {skipped - 20} more (use --json to see up to 50)")
    if error is not None and not ctx.json:
        print(f"mybucket: {error.message}\n  {error.hint}", file=sys.stderr)
    return code


def _report_lines(report) -> list[str]:
    """Human lines for one maintenance report: its summary sentence when it has one."""
    if not isinstance(report, dict):
        return [f"  {report}"]
    if report.get("summary"):
        lines = [str(report["summary"])]
        if report.get("hint"):
            lines.append(f"  {report['hint']}")
        return lines
    return _describe(report)


def _damage_error(message: str, hint: str) -> MyBucketError:
    error = MyBucketError(message, hint=hint)
    error.code = "damaged"
    return error


def _emit_with_damage(
    ctx: Context, data: dict, lines: list[str], error: MyBucketError | None, quiet_error: bool = False
) -> int:
    """Print a report; when it found a problem, add the error object and exit 1."""
    if error is None:
        ctx.emit(data, lines)
        return EXIT_OK
    data.update(error.to_dict())
    ctx.emit(data, lines)
    if not ctx.json and not quiet_error:
        print(f"mybucket: {error.message}\n  {error.hint}", file=sys.stderr)
    return EXIT_ERROR


_PROBLEM_PREFIX = re.compile(r"^Object \S+ is damaged \([^)]*\): ")


def _damage_lines(report: dict) -> list[str]:
    """One plain line per affected object version."""
    lines = []
    for item in report.get("damaged") or []:
        problem = _PROBLEM_PREFIX.sub("", str(item.get("problem", ""))).rstrip(".")
        if item.get("location") == "file":
            where = f"its stored file {item.get('path')}"
        else:
            where = "its copy inside the database"
        users = item.get("versions") or [{"key": f"(content {str(item.get('etag', ''))[:16]}...)", "version": "?", "current": False}]
        for user in users:
            state = "current" if user.get("current") else "in the trash"
            lines.append(f"- {_safe(user.get('key'))} (version {user.get('version')}, {state}): {where} is damaged: {_safe(problem)}.")
    for item in report.get("missing_content") or []:
        state = "current" if item.get("current") else "in the trash"
        lines.append(f"- {_safe(item.get('key'))} (version {item.get('version')}, {state}): its content is missing.")
    return lines


def cmd_gc(ctx: Context, ns) -> int:
    ctx.local_only("gc")
    gc = _maintenance("gc", "gc")
    store = ctx.store(write=True)
    gc_store = None
    if not ns.buckets:
        try:
            from .maintenance import gc_store
        except ImportError:
            gc_store = None
    if gc_store is not None:
        data = _plain(gc_store(store))  # every bucket, plus leftovers of interrupted create/delete
    else:
        names = ns.buckets or store.list_buckets()
        reports = [_plain(gc(store.bucket(name))) for name in names]
        data = {
            "buckets": reports,
            "bytes_freed": sum(r.get("bytes_freed", 0) for r in reports if isinstance(r, dict)),
        }
    lines: list[str] = []
    for report in data.get("buckets", []):
        lines.extend(_report_lines(report))
    if data.get("summary"):
        lines.append(str(data["summary"]))
    ctx.emit(data, lines or "The store has no buckets.")
    return EXIT_OK


def cmd_verify(ctx: Context, ns) -> int:
    ctx.local_only("verify")
    verify = _maintenance("verify", "verify")
    store = ctx.store()
    names = ns.buckets or store.list_buckets()
    reports = [_plain(verify(store.bucket(name))) for name in names]
    damaged = [
        name for name, r in zip(names, reports, strict=True) if isinstance(r, dict) and _found_damage(r)
    ]
    data: dict = {
        "ok": not damaged,
        "clean": not damaged and all(r.get("clean", True) for r in reports if isinstance(r, dict)),
        "buckets": reports,
    }
    lines: list[str] = []
    for name, report in zip(names, reports, strict=True):
        lines.append(f"{name}: {'DAMAGED' if name in damaged else 'ok'}")
        lines.extend("  " + line for line in _report_lines(report))
        if name in damaged and isinstance(report, dict):
            affected = _damage_lines(report)
            lines.extend("    " + line for line in affected[:20])
            if len(affected) > 20:
                lines.append(f"    ... and {len(affected) - 20} more (use --json to see them all)")
    if not names:
        lines.append("The store has no buckets.")
    error = None
    if damaged:
        error = _damage_error(
            f"Verify found damage in {_plural(len(damaged), 'bucket')}: {', '.join(damaged)}.",
            "See the report for the affected objects; export what is intact and restore the rest "
            "from a snapshot or backup.",
        )
    return _emit_with_damage(ctx, data, lines, error)


def cmd_snapshot(ctx: Context, ns) -> int:
    ctx.local_only("snapshot")
    snapshot = _maintenance("snapshot", "snapshot")
    report = _plain(snapshot(ctx.store(write=True), ns.dest))
    data = report if isinstance(report, dict) else {"result": report}
    lines = _report_lines(report) if isinstance(report, dict) else [f"Snapshot written to {ns.dest}."]
    error = None
    if data.get("ok") is False:
        error = _damage_error(
            "The snapshot was written, but some stored files were missing from the store.",
            "Run verify on the store: those objects were already damaged before the snapshot.",
        )
    return _emit_with_damage(ctx, data, lines, error)


_STAT_SUM = (
    "objects",
    "logical_bytes",
    "trash_versions",
    "trash_logical_bytes",
    "blobs",
    "blob_bytes",
    "stored_bytes",
    "inline_blobs",
    "file_blobs",
    "database_bytes",
    "file_bytes",
)


def cmd_stats(ctx: Context, ns) -> int:
    names = ns.buckets or ctx.store().list_buckets()
    stats = [ctx.bucket(name).stats() for name in names]
    total = {field: sum(getattr(s, field) for s in stats) for field in _STAT_SUM}
    lines = []
    for s in stats + ([None] if len(stats) > 1 else []):
        if s is None:
            label, d = "total", total
        else:
            label, d = s.bucket, s.to_dict()
        on_disk = d["database_bytes"] + d["file_bytes"]
        logical = d["logical_bytes"] + d["trash_logical_bytes"]
        if logical >= 1024 * 1024:
            ratio = f" ({on_disk / logical:.0%} of the content size)"
        else:
            ratio = " (mostly fixed overhead: every bucket takes at least about 40 KiB)"
        lines.append(
            f"{label}: {_plural(d['objects'], 'object')}, {_human_size(d['logical_bytes'])}; "
            f"trash {_plural(d['trash_versions'], 'version')}, {_human_size(d['trash_logical_bytes'])}; "
            f"on disk {_human_size(on_disk)}{ratio}"
        )
    ctx.emit({"buckets": [s.to_dict() for s in stats], "total": total}, lines or "The store has no buckets.")
    return EXIT_OK


def cmd_serve(ctx: Context, ns) -> int:
    from .access import Profile
    from .server import serve_stdio

    if not ns.stdio:
        raise UsageError(
            "serve needs --stdio: it talks over its standard input and output (for example as an SSH command).",
            hint="Write: mybucket serve --stdio --store PATH --read-all",
        )
    if ctx.remote:
        raise UsageError("serve needs a store on this machine, not an ssh:// address.")
    profile = Profile.parse(read=ns.read, write=ns.write, read_all=ns.read_all, write_all=ns.write_all)
    path = ctx.store_path()
    return serve_stdio(path, profile, timeout=ctx.timeout)


def _open_any(text: str, write: bool, timeout: float, create: bool = False) -> Store:
    path = text if text.startswith("ssh://") else Path(text).expanduser()
    try:
        return open_store(path, readonly=not write, create=create, timeout=timeout)
    except StoreNotFound as exc:
        raise StoreNotFound(exc.message, hint=f"Check the path {text!r}.") from None


def cmd_sync(ctx: Context, ns) -> int:
    from .replication import sync

    src = _open_any(ns.src, write=False, timeout=ctx.timeout)
    try:
        dst = _open_any(ns.dst, write=not ns.dry_run, timeout=ctx.timeout, create=not ns.dry_run)
        try:
            report = sync(
                src,
                dst,
                buckets=ns.buckets or None,
                prefix=ns.prefix,
                delete=ns.delete,
                dry_run=ns.dry_run,
                max_delete=ns.max_delete,
                force=ns.force,
            )
        finally:
            dst.close()
    finally:
        src.close()
    lines = []
    for r in report["buckets"]:
        made = " (created)" if r["created_bucket"] else ""
        line = (
            f"{r['bucket']}{made}: {r['copied']:,} new, {r['updated']:,} changed, {r['unchanged']:,} up to date"
        )
        if ns.delete:
            line += f", {r['deleted']:,} deleted"
        elif r["extra_in_destination"]:
            line += f", {r['extra_in_destination']:,} only in the destination"
        lines.append(line)
        for item in (r["damaged"] + r["conflicts"])[:20]:
            lines.append(f"  not copied {_safe(item['key'])}: {_safe(item['reason'])}")
        if r.get("delete_refused"):
            lines.append(f"  nothing deleted: {r['delete_refused']}")
    lines.append(_safe(report["summary"]))
    if report["hint"]:
        lines.append(f"  {report['hint']}")
    error = None
    if report.get("delete_refused"):
        error = MyBucketError(
            "Deleting in the destination was refused for safety (see above); everything else was synced.",
            hint="Check that the source is complete. If the deletes are intended, pass --max-delete N or --force.",
        )
        error.code, error.exit_code = "delete_refused", EXIT_REFUSED
        _emit_with_damage(ctx, report, lines, error)
        return EXIT_REFUSED
    if not report["ok"]:
        error = MyBucketError(
            "Some objects were not copied (listed above).",
            hint="Run verify on the source for damaged objects, and sync again for the others.",
        )
        error.code = "sync_incomplete"
    return _emit_with_damage(ctx, report, lines, error)


def cmd_dict(ctx: Context, ns) -> int:
    ctx.local_only("dict")
    if ns.dict_command == "list":
        bucket = ctx.bucket(ns.bucket)
        infos = bucket.dictionaries()
        lines = [
            f"dictionary {d.id}{' (active)' if d.active else ''}: {_human_size(d.size)}, trained "
            f"{_when(d.created_at)} on {d.samples:,} objects{' under ' + repr(d.prefix) if d.prefix else ''}; "
            f"{d.blobs:,} stored contents use it ({_human_size(d.stored_bytes)})"
            for d in infos
        ]
        ctx.emit({"bucket": bucket.name, "dictionaries": [d.to_dict() for d in infos]}, lines or None)
        if not infos:
            ctx.note(f"Bucket {bucket.name!r} has no dictionary. Train one with: mybucket dict train {bucket.name}")
        return EXIT_OK
    bucket = ctx.bucket(ns.bucket, write=True)
    if ns.dict_command in ("on", "off"):
        bucket.configure(use_dictionary=ns.dict_command == "on")
        state = "now compressed with its newest dictionary" if ns.dict_command == "on" else "no longer compressed with a dictionary (stored objects stay readable)"
        ctx.emit({"bucket": bucket.name, "use_dictionary": ns.dict_command == "on"}, f"New small objects in {bucket.name!r} are {state}.")
        return EXIT_OK
    report = bucket.train_dictionary(
        prefix=ns.prefix, dict_size=ns.dict_size, max_samples=ns.max_samples, recompress=not ns.no_recompress
    )
    ctx.emit(report, report["summary"])
    return EXIT_OK


# -- argument parsing ------------------------------------------------------------


def _common() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--store",
        metavar="PATH",
        default=argparse.SUPPRESS,
        help=f"the store directory (default: ${ENV_STORE})",
    )
    common.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS, help="print only JSON on stdout"
    )
    common.add_argument(
        "--timeout",
        type=float,
        metavar="SECONDS",
        default=argparse.SUPPRESS,
        help="how long to wait for another writer before giving up (default: 10)",
    )
    return common


def build_parser() -> argparse.ArgumentParser:
    common = _common()
    parser = _Parser(
        prog="mybucket",
        description="A small S3-like object store in a directory: no server, no lock-in.",
        parents=[common],
        epilog=(
            "Exit codes: 0 ok, 1 error (also: verify found damage, import skipped files), "
            "2 usage, 3 not found, 4 not allowed, 5 refused for safety (e.g. a failed "
            "--if-match), 75 temporary (locked by another writer: retry). "
            "Keys: 1-512 bytes of UTF-8 text in '/'-separated segments; a segment must not start "
            "with '.' or start or end with a space, and control characters and backslashes are not allowed. "
            "A store may be ssh://HOST/path (data commands only)."
        ),
    )
    parser.add_argument("--version", action="version", version=f"mybucket {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND", parser_class=_Parser)

    def add(name: str, func, help_text: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, parents=[common], help=help_text, description=help_text[:1].upper() + help_text[1:] + ".")
        p.set_defaults(func=func)
        return p

    add("buckets", cmd_buckets, "list the buckets in the store")

    p = add("mb", cmd_mb, "make a bucket")
    p.add_argument("bucket")
    p.add_argument(
        "--trash-days",
        type=int,
        default=DEFAULT_TRASH_DAYS,
        metavar="DAYS",
        help=f"how long overwritten and deleted versions are kept (default: {DEFAULT_TRASH_DAYS}; 0 = not kept)",
    )

    p = add("rb", cmd_rb, "remove a bucket (refused while it holds objects or trash)")
    p.add_argument("bucket")
    p.add_argument("--force", action="store_true", help="remove it even if it still holds objects and trash")

    p = add("put", cmd_put, "store a file or stdin as an object")
    p.add_argument("bucket")
    p.add_argument("key")
    p.add_argument("source", nargs="?", default=None, metavar="FILE", help="file to store; '-' or nothing reads stdin")
    p.add_argument(
        "-t",
        "--content-type",
        metavar="TYPE",
        help="content type, e.g. application/json (default: guessed from the key's extension; "
        "the Python SDK does not guess)",
    )
    p.add_argument(
        "--no-guess-type", action="store_true", help="do not guess the content type from the key's extension"
    )
    p.add_argument("-m", "--meta", action="append", metavar="NAME=VALUE", help="user metadata (repeatable)")
    p.add_argument("--if-match", metavar="ETAG", help="write only if the current ETag is this one")
    p.add_argument(
        "--if-none-match",
        action="store_const",
        const="*",
        help="write only if the key does not exist yet (like If-None-Match: *)",
    )
    p.add_argument(
        "--expect-size",
        type=int,
        metavar="BYTES",
        help="refuse to store unless exactly this many bytes were read (protects against a cut-off pipe)",
    )
    p.add_argument(
        "--expect-sha256", metavar="HEX", help="refuse to store unless the data has this SHA-256"
    )

    p = add("get", cmd_get, "write an object (or a byte range of it) to a file or stdout")
    p.add_argument("bucket")
    p.add_argument("key")
    p.add_argument("dest", nargs="?", default="-", metavar="FILE", help="destination file or folder; '-' or nothing for stdout")
    p.add_argument("--start", type=int, metavar="BYTE", help="first byte to write (default 0)")
    p.add_argument("--end", type=int, metavar="BYTE", help="stop before this byte (default: the end)")

    p = add("cat", cmd_cat, "write an object (or a byte range of it) to stdout")
    p.add_argument("bucket")
    p.add_argument("key")
    p.add_argument("--start", type=int, metavar="BYTE", help="first byte to write (default 0)")
    p.add_argument("--end", type=int, metavar="BYTE", help="stop before this byte (default: the end)")

    p = add("head", cmd_head, "show an object's details")
    p.add_argument("bucket")
    p.add_argument("key")

    p = add("ls", cmd_ls, "list objects (or buckets, without a bucket name)")
    p.add_argument("bucket", nargs="?")
    p.add_argument("prefix", nargs="?", help="only keys starting with this")
    p.add_argument("--prefix", dest="prefix_opt", metavar="PREFIX", help="only keys starting with this")
    p.add_argument("-d", "--delimiter", metavar="D", help="roll keys up into common prefixes at D, e.g. '/'")
    p.add_argument("--limit", type=int, metavar="N", help="at most N entries")
    p.add_argument("--start-after", metavar="KEY", help="start after this key (to continue a listing)")

    p = add("rm", cmd_rm, "delete objects (they stay in the trash for the bucket's trash days)")
    p.add_argument("bucket")
    p.add_argument("keys", nargs="+", metavar="KEY")
    p.add_argument("--if-match", metavar="ETAG", help="delete only if the current ETag is this one")
    p.add_argument("-f", "--force", action="store_true", help="ignore keys that do not exist")

    p = add("versions", cmd_versions, "list the current and trashed versions of a key")
    p.add_argument("bucket")
    p.add_argument("key")

    p = add("restore", cmd_restore, "make a trashed version current again")
    p.add_argument("bucket")
    p.add_argument("key")
    p.add_argument("version", type=int)

    p = add("export", cmd_export, "write a bucket's objects as plain files under a folder")
    p.add_argument("bucket")
    p.add_argument("dest", metavar="FOLDER")
    p.add_argument("--prefix", default="", help="only keys starting with this")
    p.add_argument("--overwrite", action="store_true", help="replace existing files that differ")
    p.add_argument(
        "--skip-conflicts",
        action="store_true",
        help="export everything else when some keys cannot be plain files",
    )

    p = add("import", cmd_import, "store every file under a folder as an object")
    p.add_argument("bucket")
    p.add_argument("src", metavar="FOLDER")
    p.add_argument("--prefix", default="", help="put this folder-like prefix in front of every key")
    p.add_argument(
        "--no-guess-type", action="store_true", help="do not guess content types from file extensions"
    )
    p.add_argument(
        "--allow-skips",
        action="store_true",
        help="exit 0 even when some files were skipped because their names cannot be keys",
    )

    p = add(
        "gc",
        cmd_gc,
        "remove expired trash and unused stored content (leftovers of interrupted writes once "
        "they are 10 minutes old, abandoned temp files after 1 hour)",
    )
    p.add_argument("buckets", nargs="*", metavar="BUCKET", help="buckets to clean (default: all)")

    p = add("verify", cmd_verify, "re-check every stored object and report damage")
    p.add_argument("buckets", nargs="*", metavar="BUCKET", help="buckets to check (default: all)")

    p = add(
        "snapshot",
        cmd_snapshot,
        "write a consistent copy of the whole store (hard-linked where possible: copy it to another disk for a backup)",
    )
    p.add_argument("dest", metavar="FOLDER")

    p = add("stats", cmd_stats, "show object counts and space used")
    p.add_argument("buckets", nargs="*", metavar="BUCKET", help="buckets to show (default: all)")

    p = add(
        "sync",
        cmd_sync,
        "copy new and changed objects from one store to another (local paths or ssh://HOST/path); "
        "incremental, verified, safe to interrupt and run again",
    )
    p.add_argument("src", metavar="SRC")
    p.add_argument("dst", metavar="DST")
    p.add_argument("-b", "--bucket", dest="buckets", action="append", metavar="BUCKET", help="only this bucket (repeatable; default: all)")
    p.add_argument("--prefix", default="", help="only keys starting with this")
    p.add_argument("--delete", action="store_true", help="also delete destination objects the source no longer has (they go to its trash)")
    p.add_argument(
        "--max-delete",
        type=int,
        metavar="N",
        help="with --delete, delete at most N objects per bucket (default: half of the destination's objects, at least 10)",
    )
    p.add_argument("--force", action="store_true", help="with --delete, no limit on how many objects are deleted")
    p.add_argument("-n", "--dry-run", action="store_true", help="only report what would change")

    p = add(
        "serve",
        cmd_serve,
        "serve the store on stdin/stdout for remote use (the command behind ssh://HOST/path); "
        "access is limited to the grants given here",
    )
    p.add_argument("--stdio", action="store_true", help="talk over stdin and stdout (required)")
    p.add_argument("--read-all", action="store_true", help="allow reading every bucket")
    p.add_argument("--write-all", action="store_true", help="allow reading and writing every bucket, and creating buckets")
    p.add_argument("--read", action="append", metavar="BUCKET[/PREFIX]", help="allow reading this bucket, or only keys under PREFIX (repeatable)")
    p.add_argument("--write", action="append", metavar="BUCKET[/PREFIX]", help="allow reading and writing this bucket, or only keys under PREFIX (repeatable)")

    p = add("dict", cmd_dict, "train and manage zstd dictionaries (for many small, similar objects)")
    dsub = p.add_subparsers(dest="dict_command", metavar="ACTION", parser_class=_Parser, required=True)
    t = dsub.add_parser(
        "train",
        parents=[common],
        help="train a dictionary from a bucket's small objects, use it for new ones and recompress stored ones",
        description="Train a dictionary from a bucket's small objects, use it for new ones and recompress stored ones.",
    )
    t.add_argument("bucket")
    t.add_argument("--prefix", default="", help="train only on keys starting with this")
    t.add_argument("--dict-size", type=int, default=32 * 1024, metavar="BYTES", help="dictionary size (default 32768)")
    t.add_argument("--max-samples", type=int, default=2000, metavar="N", help="objects to train on (default 2000)")
    t.add_argument("--no-recompress", action="store_true", help="only compress new objects with it")
    for action, text in (
        ("list", "list a bucket's dictionaries"),
        ("off", "stop compressing new objects with a dictionary (stored ones stay readable)"),
        ("on", "compress new small objects with the newest dictionary again"),
    ):
        d = dsub.add_parser(action, parents=[common], help=text, description=text[:1].upper() + text[1:] + ".")
        d.add_argument("bucket")
    return parser


# -- entry point -----------------------------------------------------------------


def _os_error(exc: OSError) -> MyBucketError:
    where = f"{exc.filename}: " if exc.filename else ""
    reason = exc.strerror or str(exc)
    if isinstance(exc, FileNotFoundError):
        error = MyBucketError(f"{where}no such file or folder.", hint="Check the path.")
        error.code, error.exit_code = "file_not_found", EXIT_NOT_FOUND
    elif isinstance(exc, PermissionError):
        error = MyBucketError(f"{where}permission denied.", hint="Check the file permissions.")
        error.code, error.exit_code = "permission_denied", EXIT_NOT_ALLOWED
    elif isinstance(exc, (IsADirectoryError, NotADirectoryError)):
        error = MyBucketError(f"{where}{reason}.", hint="Check that the path points at the right kind of entry.")
        error.code, error.exit_code = "invalid_path", EXIT_USAGE
    else:
        error = MyBucketError(f"{where}{reason}.", hint="Check the disk and the path, then retry.")
        error.code = "os_error"
    return error


def _report(error: MyBucketError, json_mode: bool, usage: str | None = None) -> int:
    error.hint = _cli_hints().get(error.hint, error.hint)
    if json_mode:
        print(json.dumps(error.to_dict(), ensure_ascii=True), flush=True)
    else:
        if usage:
            sys.stderr.write(usage)
        sys.stderr.write(f"mybucket: {_safe(error.message)}\n")
        if error.hint:
            sys.stderr.write(f"  {_safe(error.hint)}\n")
        sys.stderr.flush()
    return error.exit_code


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    json_mode = "--json" in args[: args.index("--")] if "--" in args else "--json" in args
    parser = build_parser()
    try:
        ns = parser.parse_args(args)
    except _ParserError as exc:
        error = UsageError(_usage_message(exc.message), hint=f"See: {exc.prog} --help")
        return _report(error, json_mode, usage=exc.usage)
    except SystemExit as exc:  # --help, --version
        return exc.code if isinstance(exc.code, int) else EXIT_OK
    if getattr(ns, "func", None) is None:
        if json_mode:
            return _report(UsageError("No command given.", hint="Run 'mybucket --help' for the commands."), True)
        parser.print_help(sys.stderr)
        return EXIT_USAGE
    if ns.command == "put":
        ns.source_given = ns.source is not None
        if ns.source is None:
            ns.source = "-"
    ctx = Context(ns)
    try:
        return ns.func(ctx, ns)
    except MyBucketError as error:
        return _report(error, ctx.json)
    except BrokenPipeError:
        try:  # the reader went away (e.g. '| head'); stop quietly
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
        except OSError:
            pass
        return EXIT_ERROR
    except OSError as exc:
        return _report(_os_error(exc), ctx.json)
    except KeyboardInterrupt:
        error = MyBucketError(
            "Interrupted before the command finished.",
            hint="Anything already committed is kept; run the command again to finish.",
        )
        error.code = "interrupted"
        _report(error, ctx.json)
        return 130
    except Exception as exc:  # a bug: say so plainly, keep the details for a report
        if os.environ.get("MYBUCKET_DEBUG") and ns.command != "serve":  # never a traceback to a remote client
            raise
        error = MyBucketError(
            f"Unexpected internal error: {type(exc).__name__}: {exc}",
            hint="This is a bug in mybucket. Run again with MYBUCKET_DEBUG=1 to see the full traceback.",
        )
        error.code = "internal"
        return _report(error, ctx.json)
    finally:
        ctx.close()


if __name__ == "__main__":
    sys.exit(main())
