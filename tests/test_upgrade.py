"""Format 1 stores (written by mybucket 0.1 itself, tests/data/v1-store) stay readable and upgrade safely."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from mybucket import UnsupportedFormat, open_store
from mybucket import schema as schema_mod
from mybucket.maintenance import verify_store

DATA = Path(__file__).parent / "data"
EXPECTED = json.loads((DATA / "v1-store-expected.json").read_text())


@pytest.fixture
def v1(tmp_path) -> Path:
    """A private copy of the format 1 store (never open the fixture itself: a write upgrades it)."""
    path = tmp_path / "v1-store"
    shutil.copytree(DATA / "v1-store", path)
    return path


def db_format(path: Path, bucket: str) -> tuple[int, str, list[str]]:
    conn = sqlite3.connect(f"file:{path / 'buckets' / bucket / 'bucket.sqlite'}?mode=ro", uri=True)
    try:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        fmt = conn.execute("SELECT v FROM meta WHERE k = 'format'").fetchone()[0]
        tables = sorted(r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'"))
    finally:
        conn.close()
    return version, str(fmt), tables


def check_everything_reads(store, versions: bool = True) -> None:
    for name, objects in EXPECTED.items():
        b = store.bucket(name)
        listed = {o.key: o for o in b.list().objects}
        assert sorted(listed) == sorted(objects)
        for key, entry in objects.items():
            info = listed[key]
            assert (info.etag, info.size, info.content_type) == (entry["etag"], entry["size"], entry["content_type"])
            assert info.version == entry["version"] or not versions
            assert info.metadata == (json.loads(entry["meta"]) if entry["meta"] else {})
            data = b.get(key)
            assert hashlib.sha256(data).hexdigest() == entry["etag"]


def test_fixture_really_is_format_1():
    assert db_format(DATA / "v1-store", "archive") == (1, "1", ["blobs", "meta", "objects", "trash"])


def test_read_only_open_reads_everything_and_changes_nothing(v1):
    with open_store(v1, readonly=True) as store:
        check_everything_reads(store)
        b = store.bucket("archive")
        assert b.format == 1 and b.stats().format == 1
        assert b.list_versions("days/2026-11-14.json")[1].reason == "overwritten"
        assert b.dictionaries() == []
        assert verify_store(store)["ok"]
        big = b.get("logs/big.ndjson")
        # A range of a format 1 large object (one zstd frame, no frame table) is read whole and checked.
        assert b.get("logs/big.ndjson", 1_000_000, 1_000_100) == big[1_000_000:1_000_100]
        with b.open("logs/big.ndjson", 10, 20) as s:
            assert s.read() == big[10:20] and s.verified
        assert b.get("raw/photo.jpg", 100, 200) == b.get("raw/photo.jpg")[100:200]
    assert db_format(v1, "archive")[:2] == (1, "1")


def test_writable_open_upgrades_in_place_and_keeps_every_object(v1):
    with open_store(v1) as store:
        check_everything_reads(store)  # opening the buckets upgraded them
        assert db_format(v1, "archive") == (2, "2", ["blobs", "dicts", "frames", "meta", "objects", "sqlite_sequence", "trash"])
        b = store.bucket("archive")
        assert b.format == 2
        # Old versions are still in the trash and restorable.
        old = b.list_versions("days/2026-11-14.json")[1]
        assert b.restore("days/2026-11-14.json", old.version).status == "updated"
        # New format 2 features work in the upgraded bucket.
        b.put("logs/new-big.ndjson", (b"%d\n" % i * 3 for i in range(2_000_000)))
        assert b.get("logs/new-big.ndjson", 5, 50) == b"".join(b"%d\n" % i * 3 for i in range(40))[5:50]
        b.put("unicode key é/ü.txt", b"ok")
        assert verify_store(store)["ok"]
    with open_store(v1) as store:  # and a second open finds nothing to do
        assert store.bucket("archive").format == 2


def test_the_old_blobs_are_not_rewritten(v1):
    files = {p: p.read_bytes() for p in (v1 / "buckets" / "archive" / "blobs").rglob("*") if p.is_file()}
    with open_store(v1) as store:
        store.bucket("archive").put("x", b"new")
    for path, data in files.items():
        assert path.read_bytes() == data


def test_interrupted_upgrade_leaves_format_1(v1, monkeypatch):
    real = schema_mod.SCHEMA_V2_SQL
    # Fail on the last statement: the tables created before it must be rolled back too.
    monkeypatch.setattr(schema_mod, "SCHEMA_V2_SQL", real + ";\nCREATE TABLE frames (x)")
    with pytest.raises(Exception):
        open_store(v1).bucket("archive")
    assert db_format(v1, "archive") == (1, "1", ["blobs", "meta", "objects", "trash"])
    monkeypatch.setattr(schema_mod, "SCHEMA_V2_SQL", real)
    with open_store(v1) as store:
        check_everything_reads(store)
    assert db_format(v1, "archive")[:2] == (2, "2")


def test_upgrade_under_another_writer_waits_or_reports_busy(v1):
    from mybucket import Busy

    conn = sqlite3.connect(v1 / "buckets" / "archive" / "bucket.sqlite", isolation_level=None)
    conn.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(Busy):
            open_store(v1, timeout=0.2).bucket("archive")
    finally:
        conn.execute("ROLLBACK")
        conn.close()
    assert db_format(v1, "archive")[:2] == (1, "1")


def test_a_newer_format_is_still_refused(v1):
    with open_store(v1) as store:
        store.bucket("second")
    conn = sqlite3.connect(v1 / "buckets" / "second" / "bucket.sqlite")
    conn.execute("UPDATE meta SET v = 3 WHERE k = 'format'")
    conn.commit()
    conn.close()
    with open_store(v1) as store, pytest.raises(UnsupportedFormat):
        store.bucket("second")


def test_new_buckets_are_format_2(tmp_path):
    with open_store(tmp_path / "s") as store:
        store.create_bucket("fresh")
    assert db_format(tmp_path / "s", "fresh")[:2] == (2, "2")


def test_format_1_store_can_be_synced_and_snapshotted(v1, tmp_path):
    from mybucket import sync

    with open_store(v1, readonly=True) as src, open_store(tmp_path / "copy") as dst:
        report = sync(src, dst)
        assert report["ok"] and report["copied"] == sum(len(o) for o in EXPECTED.values())
        check_everything_reads(dst, versions=False)  # version numbers are per bucket
        snap = src.snapshot(tmp_path / "snap")
        assert snap["ok"]
    with open_store(tmp_path / "snap", readonly=True) as s:
        check_everything_reads(s)
