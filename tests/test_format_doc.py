"""docs/FORMAT.md is executable documentation: its worked example and recovery script are checked here."""

from __future__ import annotations

import hashlib
import os
import random
import re
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

from mybucket import open_store

FORMAT = Path(__file__).resolve().parents[1] / "docs" / "FORMAT.md"
needs_tools = pytest.mark.skipif(
    not (shutil.which("sqlite3") and shutil.which("zstd")), reason="needs the sqlite3 and zstd command-line tools"
)
EXAMPLE_TIME = 1794787200000


def _recover_script() -> str:
    text = FORMAT.read_text()
    section = text[text.index("### 5.2") :]
    match = re.search(r"```sh\n(.*?)```", section, re.S)
    assert match
    return match.group(1)


def _example_day(seed: int) -> bytes:
    rng = random.Random(seed)
    return "".join(
        '{"time": "2026-11-14T%02d:00", "steps": %d, "hr": %d}\n' % (h, rng.randrange(0, 2000), rng.randrange(55, 120))
        for h in range(24)
    ).encode()


def make_worked_example(path: Path) -> Path:
    """The bucket of FORMAT.md section 5.1, byte for byte."""
    store = open_store(path, clock=lambda: EXAMPLE_TIME)
    b = store.bucket("notes", create=True)
    meta = {"source": "example"}
    b.put("2026/11/14/day.json", _example_day(1), content_type="application/json", metadata=meta)
    b.put("photos/cat.jpg", random.Random(7).randbytes(200_000), content_type="image/jpeg")
    b.put("2026/11/14/day.json", _example_day(2), content_type="application/json", metadata=meta)
    store.close()
    return path / "buckets" / "notes"


def test_worked_example_matches_the_document(tmp_path):
    bdir = make_worked_example(tmp_path / "store")
    conn = sqlite3.connect(bdir / "bucket.sqlite")
    rows = conn.execute("SELECT id, lower(hex(hash)), size, stored_size, codec, data IS NULL FROM blobs").fetchall()
    conn.close()
    text = FORMAT.read_text()
    for blob_id, digest, size, stored, codec, in_file in rows:
        line = re.search(rf"^{blob_id}\s+{digest}\s+{size}\s+{stored}\s+{codec}\s+{in_file}\s*$", text, re.M)
        assert line, f"FORMAT.md does not show blob row {blob_id} as stored: {digest} {size} {stored}"
    current = _example_day(2)
    assert hashlib.sha256(current).hexdigest() in text
    for line in current.decode().splitlines()[:2]:
        assert line in text


@needs_tools
def test_worked_example_commands(tmp_path):
    """Run the section 5.1 commands on a copy, as the document says."""
    bdir = make_worked_example(tmp_path / "store")
    copy = tmp_path / "notes-copy"
    shutil.copytree(bdir, copy)
    out = tmp_path / "out"
    out.mkdir()
    sql = (
        f"SELECT writefile('{out}/day.json.zst', b.data) FROM objects o JOIN blobs b ON b.id = o.blob_id "
        "WHERE o.key = '2026/11/14/day.json';"
    )
    proc = subprocess.run(["sqlite3", "bucket.sqlite", sql], cwd=copy, capture_output=True, text=True, check=True)
    assert proc.stdout.strip() == "220"
    subprocess.run(["zstd", "-d", "-q", str(out / "day.json.zst"), "-o", str(out / "day.json")], check=True)
    assert (out / "day.json").read_bytes() == _example_day(2)
    cat = copy / "blobs/34/4a/344a806bb4a1637c05370a18c1317bb846dc791dc5e48beec9c936352d3ec8d5"
    assert cat.read_bytes() == random.Random(7).randbytes(200_000)


@needs_tools
def test_recover_script_on_an_idle_bucket(tmp_path):
    script = tmp_path / "recover.sh"
    script.write_text(_recover_script())
    script.chmod(0o755)
    store = open_store(tmp_path / "store")
    b = store.bucket("box", create=True)
    hexish = random.Random(3).randbytes(200_000).hex().encode()  # compresses to about half: a zstd file blob
    objects = {
        "dir": b"a file",
        "dir/child": b"a file in a folder of the same name",
        "A.txt": b"upper",
        "a.txt": b"lower",
        "-dash": b"leading dash",
        "-d/x": b"folder with a leading dash",
        "empty.txt": b"",
        "photos/cat.jpg": os.urandom(300_000),
        "logs/big.json": b'{"a": 1}\n' * 100_000,
        "hex.txt": hexish,
        "z/last": b"the last key",
    }
    for key, data in objects.items():
        b.put(key, data)
    b.put("trashed", b"old")
    b.delete("trashed")
    store.close()  # idle: no -wal or -shm files, the case that used to fail
    bdir = tmp_path / "store" / "buckets" / "box"
    assert not (bdir / "bucket.sqlite-wal").exists()
    out = tmp_path / "out"
    proc = subprocess.run([str(script), str(bdir), str(out)], capture_output=True, text=True, timeout=120)
    case_insensitive = (out / "A.TXT").exists()
    expected_skips = {"dir/child"} | ({"a.txt"} if case_insensitive else set())
    skipped = {line.split(":")[0].removeprefix("skipped ") for line in proc.stderr.splitlines() if line.startswith("skipped ")}
    assert skipped == expected_skips, proc.stderr
    assert proc.returncode == 1
    for key, data in objects.items():
        if key not in expected_skips:
            assert (out / key).read_bytes() == data, key
    assert not (out / "trashed").exists()
    assert not list(bdir.glob("bucket.sqlite-*"))  # the bucket itself was not touched
    assert sorted(p.name for p in out.iterdir() if p.name.startswith(".")) == []


@needs_tools
def test_recover_script_stops_when_sqlite_fails(tmp_path):
    script = tmp_path / "recover.sh"
    script.write_text(_recover_script())
    script.chmod(0o755)
    bdir = tmp_path / "bucket"
    bdir.mkdir()
    (bdir / "bucket.sqlite").write_bytes(b"this is not a database" * 100)
    proc = subprocess.run([str(script), str(bdir), str(tmp_path / "out")], capture_output=True, text=True)
    assert proc.returncode != 0


@needs_tools
def test_recover_script_reports_damage(tmp_path):
    script = tmp_path / "recover.sh"
    script.write_text(_recover_script())
    script.chmod(0o755)
    store = open_store(tmp_path / "store")
    b = store.bucket("box", create=True)
    data = os.urandom(300_000)
    info = b.put("photo.jpg", data)
    b.put("ok.txt", b"fine")
    store.close()
    blob = tmp_path / "store/buckets/box/blobs" / info.etag[:2] / info.etag[2:4] / info.etag
    raw = bytearray(blob.read_bytes())
    raw[100] ^= 0xFF
    blob.write_bytes(bytes(raw))
    out = tmp_path / "out"
    proc = subprocess.run([str(script), str(tmp_path / "store/buckets/box"), str(out)], capture_output=True, text=True)
    assert proc.returncode == 1 and "skipped photo.jpg: its content does not match" in proc.stderr
    assert (out / "ok.txt").read_bytes() == b"fine"
