"""docs/FORMAT.md is executable documentation: its worked example and recovery script are checked here."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
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


# -- format 2: dictionaries, frames, keys with spaces and UTF-8 --------------------------------


def _day_log(day: dt.date) -> bytes:
    rng = random.Random(day.toordinal())
    events = [
        {
            "time": f"{day.isoformat()}T{h:02d}:{rng.randrange(60):02d}",
            "level": rng.choice(["info", "info", "warn"]),
            "message": rng.choice(["sync finished", "battery low", "heart rate sample stored", "sleep detected"]),
            "value": rng.randrange(1000),
        }
        for h in range(0, 24, 2)
    ]
    return json.dumps({"date": day.isoformat(), "device": "generic-watch-2", "events": events}, indent=1).encode()


def make_dict_example(path: Path) -> Path:
    """The dictionary example of FORMAT.md section 5.1."""
    store = open_store(path, clock=lambda: EXAMPLE_TIME)
    b = store.bucket("notes", create=True)
    start = dt.date(2026, 11, 14)
    for i in range(200):
        day = start + dt.timedelta(days=i)
        b.put(f"logs/{day.isoformat()}.json", _day_log(day), content_type="application/json")
    report = b.train_dictionary()
    assert report["kept"] and report["dict_id"] == 1
    store.close()
    return path / "buckets" / "notes"


@needs_tools
def test_dictionary_example_commands(tmp_path):
    bdir = make_dict_example(tmp_path / "store")
    copy = tmp_path / "copy"
    shutil.copytree(bdir, copy)
    out = tmp_path / "out"
    out.mkdir()

    def sql(query: str) -> str:
        return subprocess.run(["sqlite3", "bucket.sqlite", query], cwd=copy, capture_output=True, text=True, check=True).stdout.strip()

    text = FORMAT.read_text()
    first = sql("SELECT o.key, b.dict_id FROM objects o JOIN blobs b ON b.id = o.blob_id WHERE b.dict_id IS NOT NULL LIMIT 1;")
    assert first == "logs/2026-11-14.json|1" and first in text
    dict_bytes = sql(f"SELECT writefile('{out}/dict-1', data) FROM dicts WHERE id = 1;")
    stored = sql(
        f"SELECT writefile('{out}/log.json.zst', b.data) FROM objects o JOIN blobs b ON b.id = o.blob_id "
        "WHERE o.key = 'logs/2026-11-14.json';"
    )
    assert re.search(rf"^{dict_bytes}$", text, re.M) and re.search(rf"^{stored}$", text, re.M), (dict_bytes, stored)
    subprocess.run(["zstd", "-d", "-q", "-D", str(out / "dict-1"), str(out / "log.json.zst"), "-o", str(out / "log.json")], check=True)
    assert (out / "log.json").read_bytes() == _day_log(dt.date(2026, 11, 14))
    plain = subprocess.run(["zstd", "-d", "-q", "-c", str(out / "log.json.zst")], capture_output=True)
    assert plain.returncode != 0 and b"Dictionary mismatch" in plain.stderr  # refuses instead of guessing


@needs_tools
def test_recover_script_on_a_format_2_bucket(tmp_path):
    script = tmp_path / "recover.sh"
    script.write_text(_recover_script())
    script.chmod(0o755)
    store = open_store(tmp_path / "store")
    b = store.bucket("box", create=True)
    objects = {
        "summer trip/IMG 1234.jpg": os.urandom(150_000),
        "café/menu du jour.txt": "plat du jour: ratatouille\n".encode() * 50,
        "日本語/ファイル.txt": "こんにちは\n".encode() * 30,
        "pipes|and;semicolons $(not run).txt": b"shell characters are only data",
        "quote'd \"name\".txt": b"quotes",
        "-starts with dash": b"dash",
        "large/log.ndjson": b"".join(b'{"t": %d, "hr": 61}\n' % i for i in range(400_000)),  # codec 2, frames
        "large/noise.bin": os.urandom(6 << 20),  # raw, with frames
    }
    for i in range(300):
        objects[f"small/{i:04d}.json"] = json.dumps({"i": i, "type": "record", "device": "generic-watch-2", "ok": i % 3 == 0}).encode()
    for key, data in objects.items():
        b.put(key, data)
    assert b.train_dictionary()["kept"]
    b.put("small/new-after-training.json", objects["small/0001.json"] + b" ")
    objects["small/new-after-training.json"] = objects["small/0001.json"] + b" "
    with b._locked_conn() as conn:
        codecs = {r[0] for r in conn.execute("SELECT codec FROM blobs")}
        with_dict = conn.execute("SELECT count(*) FROM blobs WHERE dict_id IS NOT NULL").fetchone()[0]
    assert codecs == {0, 1, 2} and with_dict > 250
    store.close()
    bdir = tmp_path / "store" / "buckets" / "box"
    out = tmp_path / "out"
    proc = subprocess.run([str(script), str(bdir), str(out)], capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, proc.stderr
    for key, data in objects.items():
        assert (out / key).read_bytes() == data, key


@needs_tools
def test_recover_script_on_a_format_1_store_before_and_after_upgrade(tmp_path):
    script = tmp_path / "recover.sh"
    script.write_text(_recover_script())
    script.chmod(0o755)
    fixture = Path(__file__).parent / "data" / "v1-store"
    expected = json.loads((Path(__file__).parent / "data" / "v1-store-expected.json").read_text())
    shutil.copytree(fixture, tmp_path / "store")
    bdir = tmp_path / "store" / "buckets" / "archive"
    for label in ("before", "after"):
        out = tmp_path / f"out-{label}"
        proc = subprocess.run([str(script), str(bdir), str(out)], capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0, proc.stderr
        for key, entry in expected["archive"].items():
            assert hashlib.sha256((out / key).read_bytes()).hexdigest() == entry["etag"], key
        if label == "before":
            open_store(tmp_path / "store").bucket("archive").close()  # a writable open upgrades it
            conn = sqlite3.connect(bdir / "bucket.sqlite")
            assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
            conn.close()


@needs_tools
def test_recover_script_skips_rows_a_mybucket_bucket_never_holds(tmp_path):
    """Audit A1: an altered copy of a bucket is read as data; unsafe rows are skipped and reported."""
    script = tmp_path / "recover.sh"
    script.write_text(_recover_script())
    script.chmod(0o755)
    store = open_store(tmp_path / "store")
    b = store.bucket("box", create=True)
    b.put("good.txt", b"x")
    b.put("dir/also-good.txt", b"fine")
    store.close()
    bdir = tmp_path / "store" / "buckets" / "box"
    conn = sqlite3.connect(bdir / "bucket.sqlite")
    blob_id = conn.execute("SELECT blob_id FROM objects WHERE key = 'good.txt'").fetchone()[0]
    for key in ("../outside.txt", "/leading-slash.txt", "dir/./dot.txt", ".hidden", "empty//segment.txt"):
        conn.execute(
            "INSERT INTO objects (key, version, blob_id, content_type, meta, modified) VALUES (?, 99, ?, NULL, NULL, 0)",
            (key, blob_id),
        )
    # A dicts table whose id column accepts text, holding a row with a non-numeric id.
    conn.execute("DROP TABLE dicts")
    conn.execute("CREATE TABLE dicts (id, hash, zstd_id, data, created_at, samples, prefix)")
    conn.execute("INSERT INTO dicts VALUES ('not-a-number', x'00', 0, x'00', 0, 0, '')")
    conn.commit()
    conn.close()
    out = tmp_path / "out" / "inner"
    proc = subprocess.run([str(script), str(bdir), str(out)], capture_output=True, text=True, timeout=120)
    assert proc.returncode == 1, proc.stderr
    for key in ("../outside.txt", "/leading-slash.txt", "dir/./dot.txt", ".hidden", "empty//segment.txt"):
        assert f"skipped {key}: it is not a safe relative path" in proc.stderr, proc.stderr
    assert "skipped 1 dictionaries: their id is not a whole number" in proc.stderr
    assert not (tmp_path / "out" / "outside.txt").exists() and not (out / ".hidden").exists()
    assert not (out / "dir" / "dot.txt").exists()
    assert (out / "good.txt").read_bytes() == b"x" and (out / "dir" / "also-good.txt").read_bytes() == b"fine"


@needs_tools
def test_recover_script_refuses_a_database_with_triggers(tmp_path):
    script = tmp_path / "recover.sh"
    script.write_text(_recover_script())
    script.chmod(0o755)
    with open_store(tmp_path / "store") as store:
        store.bucket("box", create=True).put("good.txt", b"x")
    bdir = tmp_path / "store" / "buckets" / "box"
    conn = sqlite3.connect(bdir / "bucket.sqlite")
    conn.execute("CREATE TRIGGER note AFTER INSERT ON meta BEGIN SELECT 1; END")
    conn.commit()
    conn.close()
    proc = subprocess.run([str(script), str(bdir), str(tmp_path / "out")], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 1 and "refused" in proc.stderr
    assert not (tmp_path / "out" / "good.txt").exists()
