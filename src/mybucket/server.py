"""``mybucket serve --stdio``: the serving side of remote access, and the trust boundary.

It reads requests on stdin and answers on stdout (protocol.py), for use as
the command of an SSH connection; there is no network listener. Everything
the client sends is treated as hostile:

- framing is strict and bounded (protocol.py limits; a violation ends the
  connection), and every field is type-checked here before use;
- every bucket name, key, prefix and ETag goes through the same validators
  as local use (keys.py), and access is checked on the *normalised* key,
  against the profile given on this side's command line (access.py). The
  client can only narrow it;
- only data operations exist: nothing takes a file path, so nothing outside
  the store can be named, and maintenance (gc, verify, snapshot, export,
  import, dictionaries, deleting buckets) is not offered at all;
- memory stays bounded: object data streams through in chunks of at most
  1 MiB, a read window is at most 16 MiB, at most 16 streams and one batch
  (1,000 operations, 64 MiB of data) are open per connection;
- a put is committed only after the data's size and SHA-256, sent by the
  client at the end, match what arrived; an aborted or cut-off upload leaves
  nothing behind (only a temp file that gc removes);
- a read-only profile opens the store read-only, so SQLite itself refuses
  writes too;
- error messages are the engine's plain ones with the store's location
  replaced by "<store>"; unexpected errors are reported without details.

Public:
    serve(store_path, profile, rfile, wfile, timeout=10.0) -> int   exit status
"""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

from . import __version__
from . import keys as _keys
from .access import Profile
from .errors import (
    AccessDenied,
    ContentMismatch,
    InvalidArgument,
    MyBucketError,
    NotAvailable,
    ProtocolError,
    Refused,
    StoreError,
)
from .protocol import (
    KIND_DATA,
    MAX_DATA,
    MAX_REQUEST_JSON,
    PROTOCOL_VERSION,
    Channel,
    decode_json,
    error_to_wire,
)

MAX_STREAMS = 16
MAX_WINDOW = 16 * 1024 * 1024
MAX_LIST = 1000
MAX_BATCH_OPS = 1000
MAX_BATCH_BYTES = 64 * 1024 * 1024
MAX_VERSIONS = 10_000

_MISSING = object()


def _field(msg: dict, name: str, kind, required: bool = False, default=None):
    """A request field of the given type (``str``, ``int``, ``bool``, ``dict``), strictly."""
    value = msg.get(name, _MISSING)
    if value is _MISSING or value is None:
        if required:
            raise InvalidArgument(f"The request is missing {name!r}.")
        return default
    if kind is int:
        ok = isinstance(value, int) and not isinstance(value, bool)
    else:
        ok = isinstance(value, kind)
    if not ok:
        raise InvalidArgument(f"The request field {name!r} has the wrong type.")
    return value


class _Upload:
    """The data chunks of one put, as an iterator for the engine, checked against the end message."""

    def __init__(self, channel: Channel, limit: int | None = None) -> None:
        self._channel = channel
        self._hasher = hashlib.sha256()
        self._size = 0
        self._limit = limit
        self._draining = False
        self.finished = False

    def __iter__(self):
        while not self.finished:
            frame = self._channel.recv()
            if frame is None:
                raise ProtocolError("The client went away in the middle of an upload; nothing was written.")
            kind, payload = frame
            if kind == KIND_DATA:
                if self._draining:
                    continue
                self._size += len(payload)
                if self._limit is not None and self._size > self._limit:
                    raise Refused(
                        "This batch holds more data than one batch may (64 MiB); nothing of this object was added.",
                        hint="Commit the batch and start a new one.",
                    )
                self._hasher.update(payload)
                if payload:
                    yield payload
                continue
            self.finished = True
            end = decode_json(payload)
            if end.get("end") is not True:
                raise ProtocolError("Expected the end of the object data.")
            if not self._draining:
                self._check_end(end)

    def _check_end(self, end: dict) -> None:
        if end.get("abort") is True:
            raise ContentMismatch("The client stopped sending this object, so nothing was written.")
        size = end.get("size")
        digest = end.get("sha256")
        if isinstance(size, bool) or not isinstance(size, int) or size != self._size:
            raise ContentMismatch(f"The object arrived with {self._size} bytes, not the {size!r} announced; nothing was written.")
        if not isinstance(digest, str) or digest != self._hasher.hexdigest():
            raise ContentMismatch("The object that arrived does not have the SHA-256 announced; nothing was written.")

    @property
    def size(self) -> int:
        return self._size

    def drain(self) -> None:
        """Consume the rest of the upload (after the engine stopped early), keeping the framing in step."""
        self._draining = True
        for _ in self:
            pass


class Session:
    def __init__(self, store_path: Path, profile: Profile, channel: Channel, timeout: float) -> None:
        self.store_path = store_path
        self.profile = profile
        self.channel = channel
        self.timeout = timeout
        self.store = None
        self.streams: dict[int, object] = {}
        self._next_handle = 1
        self.batch = None  # (bucket name, Batch, ops, bytes)

    # -- plumbing ------------------------------------------------------------

    def _scrub(self, text: str) -> str:
        for path in {str(self.store_path), str(self.store_path.resolve())}:
            if path and path != ".":
                text = text.replace(path, "<store>")
        return text

    def reply(self, result) -> None:
        self.channel.send_json({"ok": True, "result": result})

    def reply_error(self, exc: MyBucketError) -> None:
        wire = error_to_wire(exc)
        wire["message"] = self._scrub(wire["message"])
        wire["hint"] = self._scrub(wire["hint"] or "")
        self.channel.send_json({"ok": False, "error": wire})

    def bucket(self, msg: dict, write: bool = False):
        name = _keys.validate_bucket_name(_field(msg, "bucket", str, required=True))
        self.profile.require_bucket(name)
        if write and not self.profile.writable(name):
            raise AccessDenied(f"This connection may not change bucket {name!r}.")
        return self.store.bucket(name)

    def key(self, msg: dict, bucket, write: bool) -> str:
        key = _keys.validate_key(_field(msg, "key", str, required=True))
        self.profile.require_key(bucket.name, key, write)
        return key

    # -- the loop --------------------------------------------------------------

    def run(self) -> int:
        try:
            first = self.channel.recv_json()
            if first is None:
                return 0
            if not self.hello(first):
                return 1
            while True:
                msg = self.channel.recv_json()
                if msg is None:
                    return 0
                self.dispatch(msg)
        except ProtocolError as exc:
            try:
                self.reply_error(exc)
            except OSError:
                pass
            return 1
        except (BrokenPipeError, ConnectionResetError):
            return 0
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        for stream in self.streams.values():
            try:
                stream.close()
            except Exception:
                pass
        self.streams.clear()
        if self.batch is not None:
            try:
                self.batch[1].__exit__(RuntimeError, RuntimeError("connection closed"), None)
            except Exception:
                pass
            self.batch = None
        if self.store is not None:
            self.store.close()

    def hello(self, msg: dict) -> bool:
        from .store import open_store

        try:
            if msg.get("op") != "hello":
                raise ProtocolError("The first message must be hello.")
            version = msg.get("protocol")
            if version != PROTOCOL_VERSION:
                raise ProtocolError(
                    f"The client speaks protocol {version!r}; this server speaks {PROTOCOL_VERSION}.",
                    hint="Install the same mybucket version on both machines.",
                )
            readonly = _field(msg, "readonly", bool, default=False)
            create = _field(msg, "create", bool, default=False)
            self.profile = self.profile.narrowed(readonly)
            can_create = create and not self.profile.readonly and _can_create(self.profile)
            self.store = open_store(self.store_path, readonly=self.profile.readonly, create=can_create, timeout=self.timeout)
        except MyBucketError as exc:
            self.reply_error(exc)
            return False
        self.reply(
            {
                "protocol": PROTOCOL_VERSION,
                "server": f"mybucket {__version__}",
                "readonly": self.profile.readonly,
                "grants": self.profile.describe(),
            }
        )
        return True

    def dispatch(self, msg: dict) -> None:
        op = msg.get("op")
        handler = _HANDLERS.get(op) if isinstance(op, str) else None
        upload = None
        try:
            if handler is None:
                raise InvalidArgument(f"Unknown operation {op!r}.", hint="Upgrade mybucket on the server.")
            if op in ("put", "batch_put"):
                upload = _Upload(self.channel, self._batch_room() if op == "batch_put" else None)
                result = handler(self, msg, upload)
                upload.drain()
            else:
                result = handler(self, msg)
            if result is not _ALREADY_REPLIED:
                self.reply(result)
        except ProtocolError:
            raise
        except MyBucketError as exc:
            if upload is not None:
                upload.drain()
            self.reply_error(exc)
        except Exception as exc:  # a bug: say so without details, keep serving the framing
            if upload is not None:
                upload.drain()
            if os.environ.get("MYBUCKET_DEBUG"):
                import traceback

                traceback.print_exc()
            else:
                print(f"mybucket serve: internal error ({type(exc).__name__})", file=sys.stderr)
            error = StoreError("Unexpected internal error on the server.", hint="This is a bug in mybucket.")
            error.code = "internal"
            self.reply_error(error)

    def _batch_room(self) -> int | None:
        if self.batch is None:
            return None
        return max(0, MAX_BATCH_BYTES - self.batch[3])


def _can_create(profile: Profile) -> bool:
    try:
        profile.require_create()
    except MyBucketError:
        return False
    return True


_ALREADY_REPLIED = object()


# -- handlers ------------------------------------------------------------------------


def op_buckets(s: Session, msg: dict):
    return [name for name in s.store.list_buckets() if s.profile.visible(name)]


def op_bucket(s: Session, msg: dict):
    b = s.bucket(msg)
    return {"bucket": b.name, "trash_days": b.trash_days, "writable": s.profile.writable(b.name) and not b.readonly}


def op_create_bucket(s: Session, msg: dict):
    name = _keys.validate_bucket_name(_field(msg, "bucket", str, required=True))
    s.profile.require_create()
    trash_days = _field(msg, "trash_days", int, default=7)
    b = s.store.create_bucket(name, trash_days=trash_days)
    return {"bucket": b.name, "trash_days": b.trash_days, "writable": True}


def op_head(s: Session, msg: dict):
    b = s.bucket(msg)
    return b.head(s.key(msg, b, write=False)).to_dict()


def op_exists(s: Session, msg: dict):
    b = s.bucket(msg)
    return b.exists(s.key(msg, b, write=False))


def op_versions(s: Session, msg: dict):
    b = s.bucket(msg)
    versions = b.list_versions(s.key(msg, b, write=False))
    if len(versions) > MAX_VERSIONS:
        raise Refused(
            f"This key has {len(versions):,} versions, more than a remote connection lists ({MAX_VERSIONS:,}).",
            hint="List them on the machine that holds the store, or let gc expire old ones.",
        )
    return [v.to_dict() for v in versions]


def op_list(s: Session, msg: dict):
    b = s.bucket(msg)
    prefix = _keys.validate_prefix(_field(msg, "prefix", str, default=""))
    s.profile.require_list(b.name, prefix)
    limit = _field(msg, "limit", int, default=MAX_LIST)
    if limit < 1:
        raise InvalidArgument("limit must be a positive whole number.")
    result = b.list(
        prefix=prefix,
        delimiter=_field(msg, "delimiter", str),
        limit=min(limit, MAX_LIST),
        start_after=_field(msg, "start_after", str),
    )
    return result.to_dict()


def op_stats(s: Session, msg: dict):
    b = s.bucket(msg)
    s.profile.require_whole_bucket(b.name)
    return b.stats().to_dict()


def _put_args(s: Session, msg: dict, b) -> dict:
    return {
        "key": s.key(msg, b, write=True),
        "content_type": _field(msg, "content_type", str),
        "metadata": _field(msg, "metadata", dict),
        "if_match": _field(msg, "if_match", str),
        "if_none_match": _field(msg, "if_none_match", str),
        "expected_size": _field(msg, "expected_size", int),
        "expected_sha256": _field(msg, "expected_sha256", str),
    }


def op_put(s: Session, msg: dict, upload: _Upload):
    b = s.bucket(msg, write=True)
    args = _put_args(s, msg, b)
    return b.put(data=upload, **args).to_dict()


def op_delete(s: Session, msg: dict):
    b = s.bucket(msg, write=True)
    info = b.delete(
        s.key(msg, b, write=True),
        if_match=_field(msg, "if_match", str),
        missing_ok=_field(msg, "missing_ok", bool, default=False),
    )
    return None if info is None else info.to_dict()


def op_restore(s: Session, msg: dict):
    b = s.bucket(msg, write=True)
    key = s.key(msg, b, write=True)
    return b.restore(key, _field(msg, "version", int, required=True)).to_dict()


def op_open(s: Session, msg: dict):
    b = s.bucket(msg)
    key = s.key(msg, b, write=False)
    if len(s.streams) >= MAX_STREAMS:
        raise Refused(
            f"This connection already has {MAX_STREAMS} open streams.",
            hint="Close streams you have finished with (use them in a with block).",
        )
    stream = b.open(key, _field(msg, "start", int), _field(msg, "end", int))
    handle = s._next_handle
    s._next_handle += 1
    s.streams[handle] = stream
    return {"handle": handle, "info": stream.info.to_dict(), "start": stream.start, "end": stream.end}


def _stream_for(s: Session, msg: dict):
    handle = _field(msg, "handle", int, required=True)
    stream = s.streams.get(handle)
    if stream is None:
        raise InvalidArgument("That stream is not open (it was closed or never opened).")
    return handle, stream


def op_read(s: Session, msg: dict):
    handle, stream = _stream_for(s, msg)
    want = min(max(1, _field(msg, "max", int, default=MAX_DATA)), MAX_WINDOW)
    sent = 0
    try:
        while sent < want:
            chunk = stream.read(min(MAX_DATA, want - sent))
            if not chunk:
                break
            s.channel.send_data(chunk)
            sent += len(chunk)
        eof = sent < want or not stream.peek(1)
    except BaseException:
        s.streams.pop(handle, None)
        stream.close()
        raise
    if eof:
        verified = stream.verified
        s.streams.pop(handle, None)
        stream.close()
        return {"eof": True, "verified": verified}
    return {"eof": False}


def op_close(s: Session, msg: dict):
    handle = _field(msg, "handle", int, required=True)
    stream = s.streams.pop(handle, None)
    if stream is not None:
        stream.close()
    return None


def op_batch_begin(s: Session, msg: dict):
    if s.batch is not None:
        raise Refused("A batch is already open on this connection.", hint="Commit or abort it first.")
    b = s.bucket(msg, write=True)
    batch = b.batch()
    batch.__enter__()
    s.batch = [b.name, batch, 0, 0]
    return None


def _open_batch(s: Session, msg: dict):
    if s.batch is None:
        raise InvalidArgument("No batch is open on this connection.", hint="Send batch_begin first.")
    name = _keys.validate_bucket_name(_field(msg, "bucket", str, required=True))
    if name != s.batch[0]:
        raise InvalidArgument(f"The open batch is for bucket {s.batch[0]!r}, not {name!r}.")
    if s.batch[2] >= MAX_BATCH_OPS:
        raise Refused(f"A batch may hold at most {MAX_BATCH_OPS} operations.", hint="Commit it and start a new one.")
    return s.store.bucket(name), s.batch[1]


def op_batch_put(s: Session, msg: dict, upload: _Upload):
    b, batch = _open_batch(s, msg)
    args = _put_args(s, msg, b)
    batch.put(data=upload, **args)
    s.batch[2] += 1
    s.batch[3] += upload.size
    return None


def op_batch_delete(s: Session, msg: dict):
    b, batch = _open_batch(s, msg)
    batch.delete(
        s.key(msg, b, write=True),
        if_match=_field(msg, "if_match", str),
        missing_ok=_field(msg, "missing_ok", bool, default=False),
    )
    s.batch[2] += 1
    return None


def op_batch_commit(s: Session, msg: dict):
    if s.batch is None:
        raise InvalidArgument("No batch is open on this connection.", hint="Send batch_begin first.")
    batch = s.batch[1]
    s.batch = None
    batch.__exit__(None, None, None)
    return [None if r is None else r.to_dict() for r in batch.results or []]


def op_batch_abort(s: Session, msg: dict):
    if s.batch is not None:
        batch = s.batch[1]
        s.batch = None
        batch.__exit__(RuntimeError, RuntimeError("aborted by the client"), None)
    return None


def op_unavailable(s: Session, msg: dict):
    raise NotAvailable(f"{msg.get('op')!r} is not available over a remote connection.")


_HANDLERS = {
    "buckets": op_buckets,
    "bucket": op_bucket,
    "create_bucket": op_create_bucket,
    "head": op_head,
    "exists": op_exists,
    "versions": op_versions,
    "list": op_list,
    "stats": op_stats,
    "put": op_put,
    "delete": op_delete,
    "restore": op_restore,
    "open": op_open,
    "read": op_read,
    "close": op_close,
    "batch_begin": op_batch_begin,
    "batch_put": op_batch_put,
    "batch_delete": op_batch_delete,
    "batch_commit": op_batch_commit,
    "batch_abort": op_batch_abort,
}


def serve(store_path: str | os.PathLike, profile: Profile, rfile, wfile, timeout: float = 10.0) -> int:
    """Serve one connection until the client closes it. Returns the exit status."""
    channel = Channel(rfile, wfile, max_json=MAX_REQUEST_JSON, max_data=MAX_DATA)
    session = Session(Path(store_path).expanduser(), profile, channel, timeout)
    return session.run()


def serve_stdio(store_path: str | os.PathLike, profile: Profile, timeout: float = 10.0) -> int:
    """serve() on this process's stdin/stdout. Anything printed by accident goes to stderr."""
    rfile = os.fdopen(os.dup(0), "rb", buffering=256 * 1024)
    wfile = os.fdopen(os.dup(1), "wb", buffering=256 * 1024)
    os.dup2(2, 1)  # stray prints can never corrupt the framing
    sys.stdout = sys.stderr
    try:
        return serve(store_path, profile, rfile, wfile, timeout)
    finally:
        for f in (wfile, rfile):
            try:
                f.close()
            except OSError:
                pass


__all__ = ["serve", "serve_stdio"]
