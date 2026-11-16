"""The client side of remote access: ``open_store("ssh://host/path")``.

The client starts ``mybucket serve --stdio`` on the other machine through
``ssh`` and talks to it over ssh's stdin/stdout (protocol.py). There is no
network listener anywhere; SSH does the authentication and encryption.

The ssh command is a plain argument list, run without a shell::

    ssh -T -x -a -o ClearAllForwardings=yes [-p PORT] -- [USER@]HOST \\
        mybucket serve --stdio --store PATH (--write-all | --read-all) --timeout SECONDS

HOST, USER and PORT are validated (a host can never start with "-"), and the
words after HOST, which ssh hands to the remote shell, are restricted to
characters no shell treats specially (the remote path may still start with
"~/"). When the account's SSH key forces a command (an access profile), the
remote side ignores these words and serves its own profile; the client only
ever asks for read-only or read-write and can never widen what it gets.

``MYBUCKET_SSH`` names another ssh program; ``MYBUCKET_REMOTE_COMMAND`` (or
``open_store(..., remote_command=[...])``) replaces ``mybucket`` on the
remote side, for example ``uv run mybucket``.

The remote store has the same API as a local one for data: buckets, put
(streamed), get / open (streamed, with ranges), head, exists, list, delete,
batch, list_versions, restore, stats. Maintenance (gc, verify, snapshot,
export, import, dictionaries, deleting buckets) raises NotAvailable: run it
on the machine that holds the store. Every full read is checked twice: by
the server against the stored SHA-256, and here against the ETag.

Public:
    open_remote(url, readonly=False, create=True, timeout=10.0, remote_command=None) -> RemoteStore
    connect(argv, url="", readonly=False, create=True) -> RemoteStore    any command that serves
    parse_ssh_url(url) -> SSHTarget
    RemoteStore, RemoteBucket, RemoteBatch, RemoteObjectStream
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import select
import shlex
import shutil
import subprocess
import threading
from collections import deque
from typing import NamedTuple

from . import keys as _keys
from .errors import (
    BucketNotFound,
    DamagedError,
    InvalidArgument,
    MyBucketError,
    NotAvailable,
    ProtocolError,
    ReadOnlyError,
    StoreError,
)
from .models import BucketStats, ListResult, ObjectInfo, PutInfo, VersionInfo
from .protocol import KIND_DATA, KIND_JSON, MAX_DATA, MAX_RESPONSE_JSON, PROTOCOL_VERSION, Channel, decode_json, error_from_wire

ENV_SSH = "MYBUCKET_SSH"
ENV_REMOTE_COMMAND = "MYBUCKET_REMOTE_COMMAND"
READ_WINDOW = 4 * 1024 * 1024
LIST_PAGE = 1000

_HOST_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]{0,252}")
_IPV6_RE = re.compile(r"[0-9A-Fa-f:.]{2,45}")
_USER_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]{0,63}")
#: Words passed to the remote shell: nothing a POSIX shell expands or splits.
_SAFE_WORD_RE = re.compile(r"[A-Za-z0-9._/+@,=:-]+")
_SAFE_PATH_RE = re.compile(r"(~/)?[A-Za-z0-9._/+@,=-]+")


class SSHTarget(NamedTuple):
    user: str | None
    host: str
    port: int | None
    path: str


def _bad_url(url: str, why: str) -> InvalidArgument:
    return InvalidArgument(
        f"{url!r} is not a usable remote store address: {why}.",
        hint="Write it as ssh://HOST/absolute/path, ssh://USER@HOST:PORT/path or ssh://HOST/~/path-in-home.",
    )


def parse_ssh_url(url: str) -> SSHTarget:
    """Split and validate ``ssh://[user@]host[:port]/path``."""
    if not isinstance(url, str) or not url.startswith("ssh://"):
        raise _bad_url(str(url), "it must start with ssh://")
    rest = url[len("ssh://") :]
    authority, slash, path = rest.partition("/")
    if not slash or not path:
        raise _bad_url(url, "the path of the store is missing")
    user = None
    if "@" in authority:
        user, _, authority = authority.rpartition("@")
        if not _USER_RE.fullmatch(user):
            raise _bad_url(url, f"the user name {user!r} is not allowed")
    port = None
    if authority.startswith("["):
        host, sep, tail = authority[1:].partition("]")
        if not sep or not _IPV6_RE.fullmatch(host):
            raise _bad_url(url, "the IPv6 address is not valid")
        if tail:
            if not tail.startswith(":"):
                raise _bad_url(url, "unexpected characters after the address")
            port = tail[1:]
    else:
        host, sep, port_text = authority.partition(":")
        port = port_text if sep else None
        if not _HOST_RE.fullmatch(host):
            raise _bad_url(url, f"the host name {host!r} is not allowed")
    if port is not None:
        if not port.isdigit() or not 0 < int(port) < 65536:
            raise _bad_url(url, f"the port {port!r} is not a number from 1 to 65535")
        port = int(port)
    path = path if path.startswith("~/") else "/" + path
    if not _SAFE_PATH_RE.fullmatch(path) or "/../" in path + "/" or path.startswith("-"):
        raise _bad_url(
            url,
            "the path may only contain letters, digits and . _ / + @ , = - (no spaces or shell characters, no '..')",
        )
    return SSHTarget(user, host, port, path)


def _remote_command(words: list[str] | None) -> list[str]:
    if words is None:
        text = os.environ.get(ENV_REMOTE_COMMAND, "").strip()
        words = text.split() if text else ["mybucket"]
    for word in words:
        if not isinstance(word, str) or not _SAFE_WORD_RE.fullmatch(word) or word.startswith("-"):
            raise InvalidArgument(
                f"The remote command word {word!r} is not allowed.",
                hint="Use plain words such as 'mybucket' or 'uv run mybucket' (letters, digits, . _ / + @ , = : -).",
            )
    return list(words)


def ssh_argv(target: SSHTarget, readonly: bool, timeout: float, remote_command: list[str] | None = None) -> list[str]:
    """The exact argument list run for an ssh:// store (no shell is involved locally)."""
    ssh = os.environ.get(ENV_SSH) or "ssh"
    argv = [ssh, "-T", "-x", "-a", "-o", "ClearAllForwardings=yes"]
    if target.port is not None:
        argv += ["-p", str(target.port)]
    argv += ["--", f"{target.user}@{target.host}" if target.user else target.host]
    argv += _remote_command(remote_command)
    argv += ["serve", "--stdio", "--store", target.path, "--read-all" if readonly else "--write-all"]
    argv += ["--timeout", f"{float(timeout):g}"]
    return argv


def open_remote(
    url: str,
    readonly: bool = False,
    create: bool = True,
    timeout: float = 10.0,
    remote_command: list[str] | None = None,
) -> RemoteStore:
    target = parse_ssh_url(url)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 <= timeout <= 86400:
        raise InvalidArgument(f"timeout must be a number of seconds, got {timeout!r}.")
    argv = ssh_argv(target, readonly, timeout, remote_command)
    if shutil.which(argv[0]) is None:
        raise StoreError(
            f"Cannot reach {url}: the program {argv[0]!r} was not found.",
            hint=f"Install OpenSSH, or set {ENV_SSH} to the ssh program to use.",
        )
    return connect(argv, url=url, readonly=readonly, create=create, host=target.host)


def connect(
    argv: list[str], url: str = "", readonly: bool = False, create: bool = True, host: str = ""
) -> RemoteStore:
    """Start ``argv`` (a command that runs ``mybucket serve --stdio``) and open it as a store."""
    if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
        raise InvalidArgument("The command must be a non-empty list of strings.")
    return RemoteStore(argv, url or shlex.join(argv), readonly, create, host)


def _build(cls, data: object):
    """A result dataclass from a server response, ignoring fields this version does not know."""
    if not isinstance(data, dict):
        raise ProtocolError("The server sent a malformed result.")
    names = cls.__dataclass_fields__.keys()
    try:
        return cls(**{k: v for k, v in data.items() if k in names})
    except TypeError:
        raise ProtocolError("The server sent a result with missing fields.") from None


def _unavailable(what: str, where: str) -> NotAvailable:
    return NotAvailable(
        f"{what} is not available on a remote store ({where}).",
        hint="Run it on the machine that holds the store.",
    )


class RemoteStore:
    """A store on another machine, served by ``mybucket serve --stdio``. Same API as Store for data."""

    is_remote = True

    def __init__(self, argv: list[str], url: str, readonly: bool, create: bool, host: str = "") -> None:
        self.path = url  # what the caller named; there is no local path
        self.url = url
        self.readonly = readonly
        self._host = host or "the remote machine"
        self._lock = threading.RLock()
        self._buckets: dict[str, RemoteBucket] = {}
        self._closed = False
        try:
            self._proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=0)
        except OSError as exc:
            raise StoreError(f"Cannot start {argv[0]!r} to reach {url}: {exc.strerror or exc}.") from None
        self._wfile = io.BufferedWriter(self._proc.stdin, buffer_size=256 * 1024)  # type: ignore[arg-type]
        self._rfile = io.BufferedReader(self._proc.stdout, buffer_size=256 * 1024)  # type: ignore[arg-type]
        self._channel = Channel(self._rfile, self._wfile, max_json=MAX_RESPONSE_JSON, max_data=MAX_DATA)
        try:
            hello = self._call("hello", protocol=PROTOCOL_VERSION, readonly=readonly, create=create)
        except BaseException:
            self.close()
            raise
        if not isinstance(hello, dict):
            self.close()
            raise ProtocolError("The server answered hello with something unexpected.")
        self.server = hello.get("server")
        #: What the serving side allows this connection (its access profile).
        self.grants = hello.get("grants") if isinstance(hello.get("grants"), list) else []
        if hello.get("readonly") is True:
            self.readonly = True

    def __repr__(self) -> str:
        return f"<RemoteStore {self.url!r}{' ro' if self.readonly else ''}>"

    def __enter__(self) -> RemoteStore:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- the connection --------------------------------------------------------

    def _gone(self) -> StoreError:
        code = self._proc.poll()
        detail = f" (exit status {code})" if code is not None else ""
        return StoreError(
            f"The connection to {self.url} closed{detail}.",
            hint=f"Check that 'ssh {self._host}' works and that mybucket is installed there "
            f"(or set {ENV_REMOTE_COMMAND}); messages from ssh or the server are shown above.",
        )

    def _send(self, op: str, fields: dict) -> None:
        if self._closed:
            raise StoreError(f"The remote store {self.url} is closed.", hint="Open it again with open_store().")
        message = {"op": op}
        message.update({k: v for k, v in fields.items() if v is not None})
        try:
            self._channel.send_json(message)
        except (BrokenPipeError, ConnectionResetError, ValueError):
            raise self._gone() from None

    def _response(self, data_sink=None):
        """Read the answer to the last request; data chunks before it go to ``data_sink``."""
        while True:
            try:
                frame = self._channel.recv()
            except ProtocolError:
                self._broken()
                raise
            if frame is None:
                self._broken()
                raise self._gone()
            kind, payload = frame
            if kind == KIND_DATA:
                if data_sink is None:
                    self._broken()
                    raise ProtocolError("The server sent object data that was not asked for.")
                data_sink(payload)
                continue
            assert kind == KIND_JSON
            message = decode_json(payload)
            if message.get("ok") is True:
                return message.get("result")
            if message.get("ok") is False:
                error = error_from_wire(message.get("error"))
                if error.code == "protocol_error":
                    self._broken()
                raise error
            self._broken()
            raise ProtocolError("The server sent a message that is not an answer.")

    def _broken(self) -> None:
        """The framing can no longer be trusted: stop using this connection."""
        self._closed = True
        try:
            self._proc.kill()
        except OSError:
            pass

    def _call(self, op: str, **fields):
        with self._lock:
            self._send(op, fields)
            return self._response()

    def _call_with_data(self, op: str, sink, **fields):
        with self._lock:
            self._send(op, fields)
            return self._response(data_sink=sink)

    def _readable_now(self) -> bool:
        """True when the server has already started answering (for example an early refusal)."""
        try:
            ready, _, _ = select.select([self._proc.stdout], [], [], 0)
        except (OSError, ValueError):
            return False
        return bool(ready)

    def _upload(self, op: str, fields: dict, data) -> object:
        """Send a request followed by object data, then read the answer."""
        with self._lock:
            self._send(op, fields)
            hasher = hashlib.sha256()
            size = 0
            failure: BaseException | None = None
            stopped = False
            try:
                for chunk in _chunks(data):
                    for start in range(0, len(chunk), MAX_DATA):
                        piece = chunk[start : start + MAX_DATA]
                        self._channel.send_data(piece)
                        hasher.update(piece)
                        size += len(piece)
                    if self._readable_now():
                        stopped = True  # the server refused already: stop sending
                        break
            except (BrokenPipeError, ConnectionResetError):
                self._broken()
                raise self._gone() from None
            except BaseException as exc:  # the caller's data source failed
                failure = exc
            try:
                if failure is not None or stopped:
                    self._channel.send_json({"end": True, "abort": True})
                else:
                    self._channel.send_json({"end": True, "size": size, "sha256": hasher.hexdigest()})
            except (BrokenPipeError, ConnectionResetError):
                self._broken()
                raise self._gone() from None
            try:
                result = self._response()
            except MyBucketError:
                if failure is not None:
                    raise failure from None
                raise
            if failure is not None:  # pragma: no cover - the server never accepts an aborted upload
                raise failure
            return result

    def close(self) -> None:
        if getattr(self, "_proc", None) is None:
            return
        with self._lock:
            if self._proc.poll() is None:
                try:
                    self._wfile.close()  # EOF: the server finishes and exits
                except OSError:
                    pass
                try:
                    self._proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self._proc.kill()
                    self._proc.wait()
            for f in (self._rfile, self._wfile):
                try:
                    f.close()
                except (OSError, ValueError):
                    pass
            self._closed = True

    def __del__(self) -> None:  # best effort; normal code calls close()
        try:
            self.close()
        except Exception:
            pass

    # -- the Store API ------------------------------------------------------------

    def _require_writable(self) -> None:
        if self.readonly:
            raise ReadOnlyError("Cannot change the store: it is open read-only.")

    def list_buckets(self) -> list[str]:
        names = self._call("buckets")
        if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
            raise ProtocolError("The server sent a malformed bucket list.")
        return names

    def bucket(self, name: str, create: bool = False) -> RemoteBucket:
        _keys.validate_bucket_name(name)
        with self._lock:
            cached = self._buckets.get(name)
            if cached is not None:
                return cached
            try:
                info = self._call("bucket", bucket=name)
            except BucketNotFound:
                if not create or self.readonly:
                    raise
                return self.create_bucket(name)
            bucket = RemoteBucket(self, name, info)
            self._buckets[name] = bucket
            return bucket

    def create_bucket(self, name: str, trash_days: int = 7) -> RemoteBucket:
        self._require_writable()
        _keys.validate_bucket_name(name)
        info = self._call("create_bucket", bucket=name, trash_days=trash_days)
        bucket = RemoteBucket(self, name, info)
        with self._lock:
            self._buckets[name] = bucket
        return bucket

    def delete_bucket(self, name: str, force: bool = False) -> None:
        raise _unavailable("Deleting a bucket", self.url)

    def gc(self, **kwargs) -> dict:
        raise _unavailable("gc", self.url)

    def verify(self) -> dict:
        raise _unavailable("verify", self.url)

    def snapshot(self, dest, buckets=None) -> dict:
        raise _unavailable("snapshot", self.url)


def _chunks(data):
    """The caller's data as bytes chunks: bytes, a binary file object or an iterable of bytes."""
    if isinstance(data, (bytes, bytearray, memoryview)):
        view = memoryview(data).cast("B")
        for start in range(0, len(view), MAX_DATA):
            yield bytes(view[start : start + MAX_DATA])
        return
    if isinstance(data, str):
        raise InvalidArgument(
            "put() needs bytes, a binary file or an iterator of bytes, not a str.",
            hint="Encode text first, for example data.encode('utf-8').",
        )
    read = getattr(data, "read", None)
    if callable(read):
        while True:
            chunk = read(MAX_DATA)
            if not chunk:
                return
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise InvalidArgument("The file returned text instead of bytes.", hint="Open the file in binary mode ('rb').")
            yield bytes(chunk)
        return
    try:
        iterator = iter(data)
    except TypeError:
        raise InvalidArgument(
            f"put() cannot read data from {type(data).__name__}.",
            hint="Pass bytes, a binary file object or an iterator of bytes chunks.",
        ) from None
    for chunk in iterator:
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise InvalidArgument(
                f"The data iterator produced {type(chunk).__name__} instead of bytes.", hint="Yield bytes chunks only."
            )
        if chunk:
            yield bytes(chunk)


def _put_fields(bucket: str, key: str, content_type, metadata, if_match, if_none_match, expected_size, expected_sha256) -> dict:
    return {
        "bucket": bucket,
        "key": key,
        "content_type": content_type,
        "metadata": metadata,
        "if_match": if_match,
        "if_none_match": if_none_match,
        "expected_size": expected_size,
        "expected_sha256": expected_sha256,
    }


class RemoteBucket:
    is_remote = True

    def __init__(self, store: RemoteStore, name: str, info: object) -> None:
        self.store = store
        self.name = name
        info = info if isinstance(info, dict) else {}
        self.readonly = store.readonly or info.get("writable") is False
        self.path = f"{store.url} bucket {name}"

    def __repr__(self) -> str:
        return f"<RemoteBucket {self.name!r} {'ro' if self.readonly else 'rw'} {self.store.url!r}>"

    def __enter__(self) -> RemoteBucket:
        return self

    def __exit__(self, *exc) -> None:
        pass

    def close(self) -> None:
        pass

    def _call(self, op: str, **fields):
        return self.store._call(op, bucket=self.name, **fields)

    def _require_writable(self) -> None:
        if self.readonly:
            raise ReadOnlyError(f"Cannot change bucket {self.name!r}: it is open read-only.")

    # writes
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
        self._require_writable()
        fields = _put_fields(self.name, key, content_type, metadata, if_match, if_none_match, expected_size, expected_sha256)
        return _build(PutInfo, self.store._upload("put", fields, data))

    def delete(self, key: str, if_match: str | None = None, missing_ok: bool = False) -> ObjectInfo | None:
        self._require_writable()
        result = self._call("delete", key=key, if_match=if_match, missing_ok=missing_ok)
        return None if result is None else _build(ObjectInfo, result)

    def batch(self) -> RemoteBatch:
        self._require_writable()
        return RemoteBatch(self)

    def restore(self, key: str, version: int) -> PutInfo:
        self._require_writable()
        return _build(PutInfo, self._call("restore", key=key, version=version))

    # reads
    def head(self, key: str) -> ObjectInfo:
        return _build(ObjectInfo, self._call("head", key=key))

    def exists(self, key: str) -> bool:
        return bool(self._call("exists", key=key))

    def open(self, key: str, start: int | None = None, end: int | None = None) -> RemoteObjectStream:
        result = self._call("open", key=key, start=start, end=end)
        if not isinstance(result, dict) or not isinstance(result.get("handle"), int):
            raise ProtocolError("The server answered open with something unexpected.")
        info = _build(ObjectInfo, result.get("info"))
        raw = _RemoteReader(self.store, result["handle"], info, result.get("start", 0), result.get("end", info.size))
        return RemoteObjectStream(raw, info)

    def get(self, key: str, start: int | None = None, end: int | None = None) -> bytes:
        with self.open(key, start, end) as stream:
            return stream.read()

    def list(
        self,
        prefix: str = "",
        delimiter: str | None = None,
        limit: int | None = None,
        start_after: str | None = None,
    ) -> ListResult:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
            raise InvalidArgument(f"limit must be a positive whole number, got {limit!r}.", hint="Leave it out (None) to list everything.")
        objects: list[ObjectInfo] = []
        prefixes: list[str] = []
        cursor = start_after
        while True:
            want = LIST_PAGE if limit is None else min(LIST_PAGE, limit - len(objects) - len(prefixes))
            page = self._call("list", prefix=prefix, delimiter=delimiter, limit=want, start_after=cursor)
            if not isinstance(page, dict):
                raise ProtocolError("The server sent a malformed listing.")
            objects += [_build(ObjectInfo, o) for o in page.get("objects") or []]
            prefixes += [p for p in page.get("prefixes") or [] if isinstance(p, str)]
            cursor = page.get("next_start_after")
            if not page.get("truncated") or not isinstance(cursor, str):
                return ListResult(objects=objects, prefixes=prefixes)
            if limit is not None and len(objects) + len(prefixes) >= limit:
                return ListResult(objects=objects, prefixes=prefixes, truncated=True, next_start_after=cursor)

    def list_versions(self, key: str) -> list[VersionInfo]:
        result = self._call("versions", key=key)
        if not isinstance(result, list):
            raise ProtocolError("The server sent a malformed version list.")
        return [_build(VersionInfo, v) for v in result]

    def stats(self) -> BucketStats:
        return _build(BucketStats, self._call("stats"))

    @property
    def trash_days(self) -> int:
        info = self._call("bucket")
        return int(info.get("trash_days", 0)) if isinstance(info, dict) else 0

    # not over the connection
    def configure(self, trash_days=None, use_dictionary=None) -> None:
        raise _unavailable("Changing bucket settings", self.store.url)

    def gc(self, **kwargs) -> dict:
        raise _unavailable("gc", self.store.url)

    def verify(self) -> dict:
        raise _unavailable("verify", self.store.url)

    def export_dir(self, *args, **kwargs) -> dict:
        raise _unavailable("export", self.store.url)

    def import_dir(self, *args, **kwargs) -> dict:
        raise _unavailable("import", self.store.url)

    def train_dictionary(self, *args, **kwargs) -> dict:
        raise _unavailable("Training a dictionary", self.store.url)

    def dictionaries(self) -> list:
        raise _unavailable("Listing dictionaries", self.store.url)


class _RemoteReader(io.RawIOBase):
    """Pulls an open stream from the server, one window at a time."""

    def __init__(self, store: RemoteStore, handle: int, info: ObjectInfo, start: int, end: int) -> None:
        super().__init__()
        self._store = store
        self._handle = handle
        self._info = info
        self.start = start if isinstance(start, int) else 0
        self.end = end if isinstance(end, int) else info.size
        self._whole = self.start == 0 and self.end == info.size
        self._chunks: deque[bytes] = deque()
        self._pending = memoryview(b"")
        self._eof = False
        self._open = True
        self._hasher = hashlib.sha256()
        self._count = 0
        self.verified = False

    def readable(self) -> bool:
        return True

    def _fill(self) -> None:
        received: list[bytes] = []
        try:
            result = self._store._call_with_data("read", received.append, handle=self._handle, max=READ_WINDOW)
        except BaseException:
            self._open = False
            self._eof = True
            raise
        for chunk in received:
            self._count += len(chunk)
            if self._count > self.end - self.start:
                self._open = False
                raise ProtocolError("The server sent more data than the object holds.")
            if self._whole:
                self._hasher.update(chunk)
            self._chunks.append(chunk)
        if isinstance(result, dict) and result.get("eof"):
            self._eof = True
            self._open = False
            if self._count != self.end - self.start:
                raise DamagedError(f"Object {self._info.key!r} arrived with {self._count} bytes instead of {self.end - self.start}.")
            if self._whole and self._hasher.hexdigest() != self._info.etag:
                raise DamagedError(f"Object {self._info.key!r} arrived with content that does not match its ETag.")
            self.verified = result.get("verified") is True

    def readinto(self, buffer) -> int:  # type: ignore[override]
        if len(buffer) == 0:
            return 0
        while not self._pending:
            if self._chunks:
                self._pending = memoryview(self._chunks.popleft())
                continue
            if self._eof:
                return 0
            self._fill()
        n = min(len(buffer), len(self._pending))
        buffer[:n] = self._pending[:n]
        self._pending = self._pending[n:]
        return n

    def close(self) -> None:
        if not self.closed:
            if self._open:
                self._open = False
                try:
                    self._store._call("close", handle=self._handle)
                except MyBucketError:
                    pass
            super().close()


class RemoteObjectStream(io.BufferedReader):
    """Like ObjectStream: ``.info``, ``.verified``, ``.start``, ``.end``."""

    def __init__(self, raw: _RemoteReader, info: ObjectInfo) -> None:
        super().__init__(raw, buffer_size=MAX_DATA)
        self.info = info
        self.start = raw.start
        self.end = raw.end

    @property
    def verified(self) -> bool:
        return bool(getattr(self.raw, "verified", False))


class RemoteBatch:
    """Same contract as Batch: nothing is visible until the block ends, then all or nothing."""

    def __init__(self, bucket: RemoteBucket) -> None:
        self._bucket = bucket
        self._state = "new"
        self._ops: list[str] = []
        self.results: list | None = None

    def __enter__(self) -> RemoteBatch:
        if self._state != "new":
            raise StoreError("This batch was already used.", hint="Start a new one with bucket.batch().")
        self._bucket._call("batch_begin")
        self._state = "open"
        return self

    def _require_open(self) -> None:
        if self._state != "open":
            raise StoreError("Batch operations must be made inside its 'with' block.", hint="Write: with bucket.batch() as b: b.put(...)")

    def put(self, key, data, content_type=None, metadata=None, if_match=None, if_none_match=None, expected_size=None, expected_sha256=None) -> None:
        self._require_open()
        fields = _put_fields(self._bucket.name, key, content_type, metadata, if_match, if_none_match, expected_size, expected_sha256)
        self._bucket.store._upload("batch_put", fields, data)
        self._ops.append("put")

    def delete(self, key, if_match=None, missing_ok=False) -> None:
        self._require_open()
        self._bucket._call("batch_delete", key=key, if_match=if_match, missing_ok=missing_ok)
        self._ops.append("delete")

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._state != "open":
            return
        self._state = "done"
        if exc_type is not None:
            try:
                self._bucket._call("batch_abort")
            except MyBucketError:
                pass
            self.results = None
            return
        try:
            results = self._bucket._call("batch_commit")
        except MyBucketError as error:
            if not error.message.endswith("No operation in the batch was applied."):
                error.message = f"{error.message} No operation in the batch was applied."
            raise
        if not isinstance(results, list) or len(results) != len(self._ops):
            raise ProtocolError("The server sent malformed batch results.")
        self.results = [
            None if r is None else _build(PutInfo if op == "put" else ObjectInfo, r) for op, r in zip(self._ops, results, strict=True)
        ]


__all__ = [
    "open_remote",
    "connect",
    "parse_ssh_url",
    "ssh_argv",
    "RemoteStore",
    "RemoteBucket",
    "RemoteBatch",
    "RemoteObjectStream",
    "SSHTarget",
]
