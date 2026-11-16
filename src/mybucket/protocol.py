"""The wire format of ``mybucket serve --stdio``: framing, limits, errors.

Both directions are a sequence of frames::

    frame   = kind (1 byte) length (4 bytes, big-endian) payload (length bytes)
    kind    = 0x4A "J": a JSON object, UTF-8
              0x44 "D": a chunk of object data

There is nothing else on the wire: no pickles, no code, no file paths from
the client. Every JSON message is a single object; the server rejects
anything else, and anything over the size limits, as a protocol error and
closes the connection.

Limits (what each side accepts from the other):

==========================  ===========  ===========
                            server reads client reads
==========================  ===========  ===========
JSON message                64 KiB       16 MiB
data chunk                  1 MiB        1 MiB
==========================  ===========  ===========

A conversation is strictly request / response, one at a time:

- the client sends a JSON request ``{"op": ..., ...}``; for ``put`` and
  ``batch_put`` it then sends the object as data chunks followed by
  ``{"end": true, "size": N, "sha256": HEX}`` (or ``{"end": true, "abort":
  true}`` when its source failed); the server checks size and SHA-256
  before anything is committed;
- the server answers with exactly one JSON message, ``{"ok": true,
  "result": ...}`` or ``{"ok": false, "error": {"code", "message", "hint",
  "exit_code"}}``. For ``read`` the data chunks come first, then that
  message.

The first request must be ``hello`` with ``"protocol": 1``.

Internal for siblings:
    PROTOCOL_VERSION, KIND_JSON, KIND_DATA, MAX_DATA, MAX_REQUEST_JSON, MAX_RESPONSE_JSON
    Channel(rfile, wfile, max_json, max_data)   .send_json(obj), .send_data(b), .flush(),
                                                .recv() -> (kind, payload) | None at clean EOF
    decode_json(payload, limit) -> dict         strict: UTF-8, an object, no NaN, bounded depth
    error_to_wire(exc) -> dict, error_from_wire(d) -> MyBucketError
"""

from __future__ import annotations

import json
import struct

from . import errors as _errors
from .errors import MyBucketError, ProtocolError

PROTOCOL_VERSION = 1
KIND_JSON = 0x4A
KIND_DATA = 0x44
MAX_DATA = 1024 * 1024
MAX_REQUEST_JSON = 64 * 1024
MAX_RESPONSE_JSON = 16 * 1024 * 1024
_HEADER = struct.Struct(">BI")
_MAX_DEPTH = 32


def _read_exact(rfile, n: int) -> bytes | None:
    """Exactly ``n`` bytes, or None when the stream ended before the first byte."""
    parts = []
    got = 0
    while got < n:
        chunk = rfile.read(n - got)
        if not chunk:
            if got == 0:
                return None
            raise ProtocolError(f"The connection ended in the middle of a message ({got} of {n} bytes).")
        parts.append(chunk)
        got += len(chunk)
    return b"".join(parts)


def _depth(value, level: int = 0) -> int:
    if level > _MAX_DEPTH:
        return level
    if isinstance(value, dict):
        return max([_depth(v, level + 1) for v in value.values()] + [level])
    if isinstance(value, list):
        return max([_depth(v, level + 1) for v in value] + [level])
    return level


def _no_constants(name: str):
    raise ValueError(f"{name} is not allowed")


def decode_json(payload: bytes) -> dict:
    """Parse one JSON message strictly; ProtocolError when it is not a plain JSON object."""
    try:
        text = payload.decode("utf-8")
        value = json.loads(text, parse_constant=_no_constants)
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise ProtocolError("A message was not valid JSON.") from None
    if not isinstance(value, dict):
        raise ProtocolError("A message was not a JSON object.")
    if _depth(value) > _MAX_DEPTH:
        raise ProtocolError("A message was nested too deeply.")
    return value


def encode_json(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


class Channel:
    """Frames over a pair of binary streams (a pipe, ssh's stdin/stdout)."""

    def __init__(self, rfile, wfile, max_json: int, max_data: int = MAX_DATA) -> None:
        self.rfile = rfile
        self.wfile = wfile
        self.max_json = max_json
        self.max_data = max_data

    def _send(self, kind: int, payload: bytes) -> None:
        self.wfile.write(_HEADER.pack(kind, len(payload)))
        if payload:
            self.wfile.write(payload)

    def send_json(self, value: dict, flush: bool = True) -> None:
        payload = encode_json(value)
        self._send(KIND_JSON, payload)
        if flush:
            self.flush()

    def send_data(self, data: bytes) -> None:
        for start in range(0, len(data), self.max_data):
            self._send(KIND_DATA, data[start : start + self.max_data])

    def flush(self) -> None:
        self.wfile.flush()

    def recv(self) -> tuple[int, bytes] | None:
        """The next frame, or None when the other side closed the connection between frames."""
        header = _read_exact(self.rfile, _HEADER.size)
        if header is None:
            return None
        kind, length = _HEADER.unpack(header)
        if kind == KIND_JSON:
            if length > self.max_json:
                raise ProtocolError(f"A message of {length} bytes is over the limit of {self.max_json}.")
        elif kind == KIND_DATA:
            if length > self.max_data:
                raise ProtocolError(f"A data chunk of {length} bytes is over the limit of {self.max_data}.")
        else:
            raise ProtocolError(f"Unknown frame type {kind}.")
        payload = _read_exact(self.rfile, length) if length else b""
        if payload is None:
            raise ProtocolError("The connection ended in the middle of a message.")
        return kind, payload

    def recv_json(self) -> dict | None:
        """The next frame, which must be a JSON message (None at clean EOF)."""
        frame = self.recv()
        if frame is None:
            return None
        kind, payload = frame
        if kind != KIND_JSON:
            raise ProtocolError("Received object data where a message was expected.")
        return decode_json(payload)


# -- errors over the wire --------------------------------------------------------


def _error_classes() -> dict[str, type[MyBucketError]]:
    classes: dict[str, type[MyBucketError]] = {}
    for name in _errors.__all__:
        cls = getattr(_errors, name)
        if isinstance(cls, type) and issubclass(cls, MyBucketError):
            classes.setdefault(cls.code, cls)
    from .transfer import ExportConflict

    classes[ExportConflict.code] = ExportConflict
    return classes


_EXIT_CODES = {
    _errors.EXIT_ERROR,
    _errors.EXIT_USAGE,
    _errors.EXIT_NOT_FOUND,
    _errors.EXIT_NOT_ALLOWED,
    _errors.EXIT_REFUSED,
    _errors.EXIT_TEMPORARY,
}


def error_to_wire(exc: MyBucketError) -> dict:
    return {"code": exc.code, "message": exc.message, "hint": exc.hint, "exit_code": exc.exit_code}


def error_from_wire(data: object) -> MyBucketError:
    """Rebuild the error the server reported, as the same class when it is a known one."""
    if not isinstance(data, dict):
        return ProtocolError("The server reported an error without details.")
    code = data.get("code") if isinstance(data.get("code"), str) else "error"
    message = data.get("message") if isinstance(data.get("message"), str) else "The server reported an error."
    hint = data.get("hint") if isinstance(data.get("hint"), str) else ""
    cls = _error_classes().get(code)
    if cls is None:
        error = MyBucketError(message[:4000], hint=hint[:4000])
        error.code = code[:64]
        exit_code = data.get("exit_code")
        if isinstance(exit_code, int) and not isinstance(exit_code, bool) and exit_code in _EXIT_CODES:
            error.exit_code = exit_code
        return error
    error = cls.__new__(cls)
    MyBucketError.__init__(error, message[:4000], hint=hint[:4000])
    return error
