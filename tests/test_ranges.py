"""Range reads: get(key, start, end) and open(key, start, end), always checked."""

from __future__ import annotations

import hashlib
import os
import random
import subprocess
import sys

import pytest

from mybucket import DamagedError, InvalidArgument, ObjectNotFound
from mybucket.blobs import blob_path
from mybucket.codec import CODEC_FRAMES, CODEC_RAW, FRAME_SIZE, seek_table

from conftest import SRC

MiB = 1 << 20


def text_bytes(n: int, seed: int = 0) -> bytes:
    rng = random.Random(seed)
    words = [b"alpha", b"beta", b"gamma", b"delta", b"heart_rate", b"steps", b'"2026-11-14"']
    out = bytearray()
    while len(out) < n:
        out += rng.choice(words) + b" " + str(rng.randrange(10**6)).encode() + b"\n"
    return bytes(out[:n])


def frames_of(bucket, key):
    with bucket._locked_conn() as conn:
        current = bucket._current(conn, key)
        row = bucket._blob_row(conn, current.blob_id)
        frames, _ = bucket._read_parts(conn, row)
    return row, frames


RANGES = [
    (0, 0),
    (0, 1),
    (0, None),
    (None, 10),
    (5, 5),
    (FRAME_SIZE - 1, FRAME_SIZE + 1),  # across a frame boundary
    (FRAME_SIZE, 2 * FRAME_SIZE),  # exactly one frame
    (123_457, 3 * FRAME_SIZE + 99),  # several frames
    (-1, None),
]


@pytest.mark.parametrize("kind", ["frames", "raw-frames", "small-zstd", "small-raw", "file-zstd", "empty"])
def test_ranges_match_python_slices(bucket, kind):
    data = {
        "frames": text_bytes(9 * MiB + 12345, seed=1),
        "raw-frames": os.urandom(6 * MiB + 17),
        "small-zstd": text_bytes(300_000, seed=2),
        "small-raw": os.urandom(1000),
        "file-zstd": os.urandom(100_000).hex().encode(),  # 200 KB, compresses to about half: a zstd file blob
        "empty": b"",
    }[kind]
    bucket.put("k", data)
    row, frames = frames_of(bucket, "k")
    if kind == "frames":
        assert row.codec == CODEC_FRAMES and len(frames) == 10
    if kind == "raw-frames":
        assert row.codec == CODEC_RAW and len(frames) == 7 and all(f.start == f.stored_start for f in frames)
    for start, end in RANGES + [(len(data) - 3, len(data) + 50), (len(data) + 1, None)]:
        if start is not None and start < 0:
            with pytest.raises(InvalidArgument):
                bucket.get("k", start, end)
            continue
        assert bucket.get("k", start, end) == data[start:end], (start, end)
        with bucket.open("k", start, end) as stream:
            assert stream.read() == data[start:end]
            assert stream.verified


def test_range_argument_checks(bucket):
    bucket.put("k", b"0123456789")
    for args in [(3, 2), (True, None), (None, 1.5), ("1", None)]:
        with pytest.raises(InvalidArgument):
            bucket.get("k", *args)
    with pytest.raises(ObjectNotFound):
        bucket.get("missing", 0, 1)
    with bucket.open("k", 2, 5) as s:
        assert (s.start, s.end, s.info.size) == (2, 5, 10)
    with bucket.open("k", 8, 100) as s:
        assert (s.start, s.end) == (8, 10)


def test_range_read_only_touches_its_frames(bucket):
    """Damage outside the range does not stop it; damage inside is found before any byte is returned."""
    data = text_bytes(5 * MiB + 100, seed=3)
    info = bucket.put("k", data)
    row, frames = frames_of(bucket, "k")
    path = blob_path(bucket.path, info.etag)
    raw = bytearray(path.read_bytes())
    f3 = frames[3]
    raw[f3.stored_start + f3.stored_size // 2] ^= 0x40  # damage frame 3 only
    path.write_bytes(bytes(raw))
    assert bucket.get("k", 0, 3 * FRAME_SIZE) == data[: 3 * FRAME_SIZE]
    assert bucket.get("k", 4 * FRAME_SIZE, None) == data[4 * FRAME_SIZE :]
    with pytest.raises(DamagedError) as err:
        bucket.get("k", 3 * FRAME_SIZE + 10, 3 * FRAME_SIZE + 20)
    assert f"content offset {3 * FRAME_SIZE}" in err.value.message
    with bucket.open("k") as stream:  # a full stream hands out frames 0-2, then stops before bad bytes
        assert stream.read(3 * FRAME_SIZE) == data[: 3 * FRAME_SIZE]
        with pytest.raises(DamagedError):
            stream.read(10)
    assert not bucket.verify()["ok"]


def test_frame_hash_catches_swapped_frames(bucket):
    """Frames are checked against their own SHA-256, not just decompressed."""
    data = text_bytes(5 * MiB, seed=4)
    bucket.put("k", data)
    row, frames = frames_of(bucket, "k")
    with bucket._locked_conn() as conn:
        conn.execute("UPDATE frames SET hash = ? WHERE blob_id = ? AND start = ?", (frames[0].hash, row.id, frames[1].start))
    with pytest.raises(DamagedError):
        bucket.get("k", FRAME_SIZE, FRAME_SIZE + 10)
    assert bucket.get("k", 0, 10) == data[:10]


@pytest.mark.parametrize(
    "corrupt",
    [
        "DELETE FROM frames WHERE blob_id = {id} AND start > 0",
        "DELETE FROM frames WHERE blob_id = {id}",
        "UPDATE frames SET stored_start = stored_start + 1 WHERE blob_id = {id} AND start > 0",
        "UPDATE frames SET size = size + 1 WHERE blob_id = {id} AND start = 0",
        "UPDATE frames SET stored_size = 100000000 WHERE blob_id = {id} AND start = 0",
    ],
)
def test_inconsistent_frame_tables_are_damage(bucket, corrupt):
    data = text_bytes(5 * MiB, seed=5)
    bucket.put("k", data)
    row, _ = frames_of(bucket, "k")
    with bucket._locked_conn() as conn:
        conn.execute(corrupt.format(id=row.id))
    with pytest.raises(DamagedError):
        bucket.get("k")
    assert not bucket.verify()["ok"]


def test_seek_table_is_standard_and_checked(bucket):
    data = text_bytes(5 * MiB + 5, seed=6)
    info = bucket.put("k", data)
    row, frames = frames_of(bucket, "k")
    stored = blob_path(bucket.path, info.etag).read_bytes() if row.data is None else row.data
    table = seek_table(frames)
    assert stored.endswith(table)
    assert table[:4] == (0x184D2A5E).to_bytes(4, "little") and table[-4:] == (0x8F92EAB1).to_bytes(4, "little")
    assert int.from_bytes(table[-9:-5], "little") == len(frames) == 6
    if row.data is None:
        path = blob_path(bucket.path, info.etag)
        path.write_bytes(stored[:-1] + b"\x00")
        report = bucket.verify()
        assert not report["ok"] and "seek table" in report["damaged"][0]["problem"]


def test_cli_ranges(bucket, tmp_path):
    data = text_bytes(2 * MiB + 7, seed=8)
    bucket.put("k", data)
    store = bucket.store.path
    env = dict(os.environ, PYTHONPATH=str(SRC))
    run = lambda *a: subprocess.run([sys.executable, "-m", "mybucket", "--store", str(store), *a], capture_output=True, env=env, timeout=120)
    proc = run("cat", bucket.name, "k", "--start", str(MiB - 3), "--end", str(MiB + 3))
    assert proc.returncode == 0 and proc.stdout == data[MiB - 3 : MiB + 3]
    out = tmp_path / "part"
    proc = run("--json", "get", bucket.name, "k", str(out), "--start", "10")
    assert proc.returncode == 0 and out.read_bytes() == data[10:]
    assert b'"range": {"start": 10' in proc.stdout
    assert run("cat", bucket.name, "k", "--start", "5", "--end", "2").returncode == 2


def test_large_streamed_object_is_cheap_to_range_read(bucket):
    """A range read of a framed object decompresses only the frames it needs."""
    from mybucket import _zstd

    data = text_bytes(40 * MiB, seed=9)
    bucket.put("big", (data[i : i + MiB] for i in range(0, len(data), MiB)))
    calls = {"n": 0}
    real = _zstd.Decompressor

    def counting(*args):
        calls["n"] += 1
        return real(*args)

    import mybucket.codec as codec_mod

    codec_mod._zstd.Decompressor = counting
    try:
        got = bucket.get("big", 30 * MiB + 5, 30 * MiB + 50)
    finally:
        codec_mod._zstd.Decompressor = real
    assert got == data[30 * MiB + 5 : 30 * MiB + 50] and calls["n"] == 1
    assert hashlib.sha256(bucket.get("big")).digest() == hashlib.sha256(data).digest()
