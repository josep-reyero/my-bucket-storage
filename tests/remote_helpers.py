"""Helpers for driving ``mybucket serve`` directly with raw frames (no client library)."""

from __future__ import annotations

import hashlib
import io
import json
import os
import struct
import sys
import textwrap
from pathlib import Path

from mybucket.access import Profile
from mybucket.server import serve

from conftest import SRC

KIND_JSON = 0x4A
KIND_DATA = 0x44


def frame(kind: int, payload: bytes) -> bytes:
    return struct.pack(">BI", kind, len(payload)) + payload


def j(obj) -> bytes:
    return frame(KIND_JSON, json.dumps(obj).encode())


def d(data: bytes) -> bytes:
    return frame(KIND_DATA, data)


def end_of(data: bytes) -> bytes:
    return j({"end": True, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})


def upload(data: bytes) -> bytes:
    return b"".join(d(data[i : i + (1 << 20)]) for i in range(0, len(data), 1 << 20)) + end_of(data)


HELLO = j({"op": "hello", "protocol": 1})
HELLO_RO = j({"op": "hello", "protocol": 1, "readonly": True})


def parse(out: bytes) -> list:
    """Frames from the server: JSON messages as dicts, data chunks as bytes."""
    items = []
    view = memoryview(out)
    while view:
        kind, length = struct.unpack(">BI", view[:5])
        payload = bytes(view[5 : 5 + length])
        view = view[5 + length :]
        items.append(json.loads(payload) if kind == KIND_JSON else payload)
    return items


def session(store_path: Path, profile: Profile, *parts: bytes) -> tuple[int, list]:
    """Run one in-process server session on the given raw input; returns (exit status, frames out)."""
    rfile = io.BufferedReader(io.BytesIO(b"".join(parts)))
    wfile = io.BytesIO()
    status = serve(store_path, profile, rfile, wfile)
    return status, parse(wfile.getvalue())


def serve_argv(store_path: Path, *grants: str) -> list[str]:
    return [sys.executable, "-m", "mybucket", "serve", "--stdio", "--store", str(store_path), *grants]


def child_env() -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    return env


FAKE_SSH = textwrap.dedent(
    '''\
    #!{python}
    """A stand-in for ssh: records its arguments, checks their shape, runs the remote words locally."""
    import json, os, sys
    args = sys.argv[1:]
    with open({log!r}, "a") as f:
        f.write(json.dumps(args) + "\\n")
    if os.environ.get("FAKE_SSH_FAIL"):
        sys.stderr.write("ssh: connect to host example port 22: Connection refused\\n")
        sys.exit(255)
    i = args.index("--")
    host, words = args[i + 1], args[i + 2 :]
    assert not host.startswith("-"), host
    if words and words[0] == "mybucket":
        words = [sys.executable, "-m", "mybucket"] + words[1:]
    # The remote shell expands a leading ~/ (ssh://host/~/path); do the same here.
    words = [os.path.expanduser(w) if w.startswith("~/") else w for w in words]
    os.execvp(words[0], words)
    '''
)


def install_fake_ssh(directory: Path) -> Path:
    """Write a fake ``ssh`` into ``directory``; returns the path of the log of its argument lists."""
    directory.mkdir(parents=True, exist_ok=True)
    log = directory / "ssh-calls.jsonl"
    script = directory / "ssh"
    script.write_text(FAKE_SSH.format(python=sys.executable, log=str(log)))
    script.chmod(0o755)
    return log
