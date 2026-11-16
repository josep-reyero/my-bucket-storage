"""The zstd shim, including the ``zstandard`` fallback, driven through the real codec.

The fallback is only used when neither ``compression.zstd`` (Python 3.14+)
nor ``backports.zstd`` is installed. Here it is loaded as a separate copy
of ``mybucket/_zstd.py`` with those two hidden, and the codec is pointed at
it, so the same engine paths run on both backends.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import sys

import pytest

from mybucket import _zstd as native
from mybucket import codec as codec_mod
from mybucket.blobs import iter_tmp_files
from mybucket.maintenance import verify

from conftest import SRC

zstandard = pytest.importorskip("zstandard")


def _load_fallback():
    spec = importlib.util.spec_from_file_location("mybucket_zstd_fallback", SRC / "mybucket" / "_zstd.py")
    module = importlib.util.module_from_spec(spec)
    hidden = ("compression", "compression.zstd", "backports", "backports.zstd")
    saved = {name: sys.modules.get(name, KeyError) for name in hidden}
    try:
        for name in hidden:
            sys.modules[name] = None  # makes "import name" raise ImportError
        spec.loader.exec_module(module)
    finally:
        for name, value in saved.items():
            if value is KeyError:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value
    return module


fallback = _load_fallback()
BACKENDS = {"native": native, "zstandard": fallback}


def test_fallback_really_is_zstandard():
    assert fallback.BACKEND == "zstandard"
    assert native.BACKEND in ("compression.zstd", "backports.zstd")


@pytest.fixture(params=sorted(BACKENDS))
def zstd(request, monkeypatch):
    module = BACKENDS[request.param]
    monkeypatch.setattr(codec_mod, "_zstd", module)
    return module


def text(n: int, seed: int = 0) -> bytes:
    line = b'{"time": "2026-11-14T00:00", "steps": %d, "hr": 60}\n'
    out = bytearray()
    i = seed
    while len(out) < n:
        out += line % (i % 977)
        i += 1
    return bytes(out[:n])


def test_frames_are_interchangeable():
    data = text(300_000)
    for writer in BACKENDS.values():
        frame = writer.compress(data, 19)
        for reader in BACKENDS.values():
            assert reader.Decompressor().decompress(frame) == data


def test_decompressor_reports_eof_only_when_everything_was_handed_out(zstd):
    data = text(2_000_000)
    frame = zstd.compress(data, 9)
    d = zstd.Decompressor()
    out = d.decompress(frame, 1000)
    assert len(out) == 1000 and not d.eof and not d.needs_input
    while not d.eof:
        out += d.decompress(b"", 1 << 20)
    assert out == data and d.unused_data == b""


def test_frame_reader_output_is_bounded(zstd, tmp_path):
    """A small frame that expands enormously never produces more than write_size at once."""
    c = zstd.Compressor(19)
    frame = b"".join(c.compress(bytes(1 << 20)) for _ in range(64)) + c.flush()
    assert len(frame) < 64 * 1024
    path = tmp_path / "bomb.zst"
    path.write_bytes(frame)
    total = 0
    with open(path, "rb") as fp:
        reader = zstd.FrameReader(fp, 4096, 65536)
        while chunk := reader.read():
            assert len(chunk) <= 65536
            total += len(chunk)
    assert total == 64 << 20 and not reader.truncated and not reader.trailing


def test_small_objects(zstd, bucket):
    for key, data in (("empty", b""), ("text", text(50_000)), ("rand", os.urandom(50_000))):
        bucket.put(key, data)
        assert bucket.get(key) == data


def test_compressible_objects_over_one_mib(zstd, bucket):
    data = text(2 << 20)
    bucket.put("two", data)
    assert bucket.get("two") == data
    stream = text(6 << 20, seed=3)
    bucket.put("six", (stream[i : i + 65536] for i in range(0, len(stream), 65536)))
    with bucket.open("six") as f:
        assert f.read() == stream and f.verified
    assert verify(bucket)["ok"]


def test_streamed_mostly_incompressible_object_is_stored_whole(zstd, bucket, monkeypatch):
    """The input that the old fallback truncated: random data then zeros, stored raw."""
    monkeypatch.setattr(codec_mod, "DECIDE_AFTER", 1 << 20)  # exercise the decompress-to-raw path
    data = bytes(2 << 20) + os.urandom(40 << 20)
    info = bucket.put("mixed.bin", (data[i : i + (1 << 20)] for i in range(0, len(data), 1 << 20)))
    assert info.size == len(data) and info.stored_size == len(data)
    assert bucket.get("mixed.bin") == data
    assert verify(bucket)["ok"]
    assert list(iter_tmp_files(bucket.path)) == []


def test_damage_is_detected(zstd, bucket):
    data = text(3 << 20)
    info = bucket.put("big", data)
    row = bucket._existing_blob(bytes.fromhex(info.etag))
    assert row.codec == codec_mod.CODEC_ZSTD
    if row.data is None:
        path = bucket.path / "blobs" / info.etag[:2] / info.etag[2:4] / info.etag
        raw = bytearray(path.read_bytes())
        raw[len(raw) // 2] ^= 0xFF
        path.write_bytes(bytes(raw))
    else:
        import sqlite3

        raw = bytearray(row.data)
        raw[len(raw) // 2] ^= 0xFF
        conn = sqlite3.connect(bucket.db_path)
        conn.execute("UPDATE blobs SET data = ? WHERE id = ?", (bytes(raw), row.id))
        conn.commit()
        conn.close()
    assert not verify(bucket)["ok"]
    with pytest.raises(Exception) as err:
        bucket.get("big")
    assert err.type.__name__ == "DamagedError"
    assert hashlib.sha256(data).hexdigest() == info.etag
