"""Trained zstd dictionaries: train, use, recompress, retrain, gc, damage, and both zstd backends."""

from __future__ import annotations

import json
import os
import random
import sqlite3
import subprocess
import sys

import pytest

from mybucket import DamagedError, InvalidArgument, ReadOnlyError, Refused, open_store
from mybucket import bucket as bucket_mod
from mybucket import codec as codec_mod
from mybucket import dicts as dicts_mod
from mybucket.maintenance import gc, verify

from conftest import SRC


def record(i: int, seed: int = 0) -> bytes:
    rng = random.Random(i * 7919 + seed)
    return json.dumps(
        {
            "date": f"2026-{1 + i % 12:02d}-{1 + i % 28:02d}",
            "device": "generic-watch-2",
            "activity": rng.choice(["walk", "run", "cycle", "swim", "sleep"]),
            "steps": rng.randrange(20000),
            "heart_rate": {"min": rng.randrange(40, 60), "avg": rng.randrange(60, 90), "max": rng.randrange(120, 190)},
            "notes": rng.choice(["", "felt good", "rainy", "new shoes", "travel day"]),
        },
        indent=1,
    ).encode()


def fill(bucket, n: int = 400, prefix: str = "days/", seed: int = 0) -> dict[str, bytes]:
    out = {}
    with bucket.batch() as tx:
        for i in range(n):
            key, data = f"{prefix}{i:05d}.json", record(i, seed)
            tx.put(key, data, content_type="application/json")
            out[key] = data
    return out


def dict_ids(bucket) -> dict[str, int | None]:
    with bucket._locked_conn() as conn:
        rows = conn.execute("SELECT o.key, b.dict_id FROM objects o JOIN blobs b ON b.id = o.blob_id").fetchall()
    return dict(rows)


def stored_total(bucket) -> int:
    with bucket._locked_conn() as conn:
        return conn.execute("SELECT sum(stored_size) FROM blobs").fetchone()[0]


@pytest.fixture(params=["native", "zstandard"])
def backend(request, monkeypatch):
    """Run the test with the normal zstd module, and again with the zstandard fallback."""
    if request.param == "zstandard":
        import importlib.util

        pytest.importorskip("zstandard")
        spec = importlib.util.spec_from_file_location("mybucket_zstd_fallback_dicts", SRC / "mybucket" / "_zstd.py")
        module = importlib.util.module_from_spec(spec)
        blocked = {"compression.zstd": None, "compression": None, "backports.zstd": None, "backports": None}
        with pytest.MonkeyPatch.context() as m:
            for name, value in blocked.items():
                m.setitem(sys.modules, name, value)
            spec.loader.exec_module(module)
        assert module.BACKEND == "zstandard"
        for mod in (codec_mod, bucket_mod, dicts_mod):
            monkeypatch.setattr(mod, "_zstd", module)
    return request.param


def test_train_use_and_recompress(bucket, backend):
    data = fill(bucket)
    before = stored_total(bucket)
    report = bucket.train_dictionary()
    assert report["kept"] and report["dict_id"] == 1 and report["saving"] > 0.3
    assert report["samples"] + report["test_objects"] == 400 and report["recompressed"] == 400
    assert report["bytes_saved"] == before - stored_total(bucket) > 0
    assert set(dict_ids(bucket).values()) == {1}
    for key, value in data.items():
        assert bucket.get(key) == value
    new = bucket.put("days/new.json", record(9999))
    assert dict_ids(bucket)["days/new.json"] == 1 and new.stored_size < len(record(9999)) // 2
    big = os.urandom(1 << 20) + b"x"  # over 1 MiB: never compressed with the dictionary
    bucket.put("big.bin", big)
    assert dict_ids(bucket)["big.bin"] is None
    st = bucket.stats()
    assert (st.active_dict, st.dictionaries, st.dict_blobs) == (1, 1, 401)
    info = bucket.dictionaries()[0]
    assert info.active and info.blobs == 401 and info.size == report["dict_bytes"] and info.prefix == ""
    assert verify(bucket)["ok"]


def test_saving_is_measured_on_held_out_objects(bucket, monkeypatch):
    fill(bucket, n=100)
    monkeypatch.setattr(dicts_mod, "MIN_SAVING", 0.99)  # impossible to reach
    report = bucket.train_dictionary()
    assert not report["kept"] and report["dict_id"] is None and "not kept" in report["summary"]
    assert report["test_objects"] == 20 and report["samples"] == 80
    assert bucket.dictionaries() == [] and set(dict_ids(bucket).values()) == {None}


def test_random_content_gets_no_dictionary(bucket):
    with bucket.batch() as tx:
        for i in range(200):
            tx.put(f"r/{i}", os.urandom(2000))
    # Training on noise either fails (zstd cannot build a dictionary) or is not kept; both are fine.
    try:
        assert not bucket.train_dictionary()["kept"]
    except Refused:
        pass
    assert bucket.dictionaries() == []


def test_too_few_objects_is_refused(bucket):
    fill(bucket, n=10)
    with pytest.raises(Refused) as err:
        bucket.train_dictionary()
    assert "10 small objects" in err.value.message
    fill(bucket, n=30, prefix="other/")
    with pytest.raises(Refused):
        bucket.train_dictionary(prefix="days/")


def test_argument_checks(bucket):
    fill(bucket, n=30)
    for kwargs in ({"dict_size": 10}, {"dict_size": True}, {"max_samples": 5}, {"prefix": "a\x00"}):
        with pytest.raises(Exception) as err:
            bucket.train_dictionary(**kwargs)
        assert err.type.__name__ in ("InvalidArgument", "InvalidKey")


def test_prefix_training_and_no_recompress(bucket):
    fill(bucket, n=100, prefix="a/")
    fill(bucket, n=100, prefix="b/", seed=5)
    report = bucket.train_dictionary(prefix="a/", recompress=False)
    assert report["kept"] and report["recompressed"] == 0 and bucket.dictionaries()[0].prefix == "a/"
    assert set(dict_ids(bucket).values()) == {None}  # stored objects untouched
    bucket.put("b/new.json", record(5000, seed=5))
    assert dict_ids(bucket)["b/new.json"] == 1  # but every new small object uses it


def test_retrain_then_gc_removes_only_unused_dictionaries(bucket, clock):
    data = fill(bucket, n=200)
    assert bucket.train_dictionary()["dict_id"] == 1
    data.update(fill(bucket, n=200, prefix="later/", seed=3))
    second = bucket.train_dictionary(recompress=False)
    assert second["dict_id"] == 2
    ids = dict_ids(bucket)
    assert ids["days/00000.json"] == 1 and ids["later/00000.json"] == 1
    gc_report = gc(bucket)
    assert gc_report["dictionaries_removed"] == 0  # 1 is still used, 2 is active
    # Overwrite everything compressed with dictionary 1, let the trash expire, and gc.
    bucket.configure(trash_days=0)
    with bucket.batch() as tx:
        for key in data:
            tx.put(key, data[key] + b"\n")
    assert set(dict_ids(bucket).values()) == {2}
    gc_report = gc(bucket)
    assert gc_report["dictionaries_removed"] == 1
    assert [d.id for d in bucket.dictionaries()] == [2]
    bucket.configure(use_dictionary=False)
    bucket.put("plain.json", record(1))
    assert dict_ids(bucket)["plain.json"] is None
    assert gc(bucket)["dictionaries_removed"] == 0  # 2 is inactive but still used
    for key, value in data.items():
        assert bucket.get(key) == value + b"\n"
    bucket.configure(use_dictionary=True)
    assert bucket.stats().active_dict == 2
    # A third dictionary gets a new id even though 1 is gone (ids are never reused).
    assert bucket.train_dictionary(recompress=False)["dict_id"] == 3
    assert verify(bucket)["ok"]


def test_use_dictionary_on_without_any_is_an_error(bucket):
    with pytest.raises(InvalidArgument):
        bucket.configure(use_dictionary=True)


def test_dictionary_removed_while_a_write_was_prepared(bucket, monkeypatch):
    """gc can delete the dictionary between compression (outside the lock) and commit."""
    fill(bucket, n=100)
    bucket.train_dictionary(recompress=False)
    value = record(77777)
    real = bucket._keep_dictionary

    def gc_in_between(conn, staged):
        assert staged.dict_id == 1
        conn.execute("DELETE FROM meta WHERE k = 'active_dict'")
        conn.execute("DELETE FROM dicts WHERE id = 1")
        real(conn, staged)

    monkeypatch.setattr(bucket, "_keep_dictionary", gc_in_between)
    bucket.put("raced.json", value)
    assert dict_ids(bucket)["raced.json"] is None
    assert bucket.get("raced.json") == value


@pytest.mark.parametrize("how", ["delete", "damage-data", "damage-text", "damage-hash", "other-dict"])
def test_missing_or_damaged_dictionary_is_reported(bucket, how):
    fill(bucket, n=100)
    bucket.train_dictionary()  # dictionary 1, used by the 100 objects
    bucket.configure(use_dictionary=False)
    bucket.put("plain.json", record(1, 9))
    fill(bucket, n=100, prefix="z/", seed=9)
    assert bucket.train_dictionary(prefix="z/", recompress=False)["dict_id"] == 2  # a different dictionary
    conn = sqlite3.connect(bucket.db_path)
    if how == "delete":
        conn.execute("DELETE FROM dicts WHERE id = 1")
    elif how == "damage-data":
        (data,) = conn.execute("SELECT data FROM dicts WHERE id = 1").fetchone()
        conn.execute("UPDATE dicts SET data = ? WHERE id = 1", (data[:100] + bytes([data[100] ^ 1]) + data[101:],))
    elif how == "damage-text":
        conn.execute("UPDATE dicts SET data = CAST(substr(data, 1, 64) AS TEXT) WHERE id = 1")  # not even a BLOB
    elif how == "damage-hash":
        conn.execute("UPDATE dicts SET hash = zeroblob(32) WHERE id = 1")
    else:  # point the blobs at another valid dictionary: zstd refuses, nothing wrong is returned
        conn.execute("UPDATE blobs SET dict_id = 2 WHERE dict_id = 1")
    conn.commit()
    conn.close()
    with open_store(bucket.store.path) as s:
        reopened = s.bucket(bucket.name)
        with pytest.raises(DamagedError) as err:
            reopened.get("days/00001.json")
        assert "ictionary" in err.value.message
        report = verify(reopened)
        assert not report["ok"] and report["damaged"]
        assert reopened.get("plain.json") == record(1, 9)  # stored without a dictionary: fine
        if how == "other-dict":
            assert "Dictionary mismatch" in err.value.message


def test_read_only_bucket_cannot_train(store_path):
    with open_store(store_path) as s:
        fill(s.bucket("bk", create=True), n=40)
    with open_store(store_path, readonly=True) as s:
        with pytest.raises(ReadOnlyError):
            s.bucket("bk").train_dictionary()
        with pytest.raises(ReadOnlyError):
            s.bucket("bk").configure(use_dictionary=False)


def test_recompress_skips_objects_changed_meanwhile(bucket, monkeypatch):
    fill(bucket, n=100)
    real = dicts_mod._recompress

    def overwrite_first(b, dict_id):
        b.put("days/00000.json", b"changed during training")
        return real(b, dict_id)

    monkeypatch.setattr(dicts_mod, "_recompress", overwrite_first)
    report = bucket.train_dictionary()
    assert report["kept"] and bucket.get("days/00000.json") == b"changed during training"
    assert verify(bucket)["ok"]


def test_cli_dict(tmp_path):
    path = tmp_path / "store"
    with open_store(path) as s:
        fill(s.bucket("logs", create=True), n=200)
    env = dict(os.environ, PYTHONPATH=str(SRC))

    def run(*args, code=0):
        proc = subprocess.run([sys.executable, "-m", "mybucket", "--store", str(path), *args], capture_output=True, env=env, timeout=120)
        assert proc.returncode == code, (proc.stdout, proc.stderr)
        return proc

    assert b"has no dictionary" in run("dict", "list", "logs").stderr
    doc = json.loads(run("--json", "dict", "train", "logs", "--dict-size", "16384").stdout)
    assert doc["kept"] and doc["dict_bytes"] <= 16384 and doc["recompressed"] == 200
    listed = json.loads(run("--json", "dict", "list", "logs").stdout)["dictionaries"]
    assert listed[0]["active"] and listed[0]["blobs"] == 200
    assert b"(active)" in run("dict", "list", "logs").stdout
    assert b"no longer compressed" in run("dict", "off", "logs").stdout
    assert json.loads(run("--json", "dict", "list", "logs").stdout)["dictionaries"][0]["active"] is False
    run("dict", "on", "logs")
    assert json.loads(run("--json", "stats", "logs").stdout)["buckets"][0]["active_dict"] == 1
    assert json.loads(run("--json", "dict", "train", "logs", "--max-samples", "5", code=2).stdout)["error"]["code"] == "invalid_argument"
    run("dict", code=2)
    assert json.loads(run("--json", "verify", "logs").stdout)["ok"]


def test_dictionary_objects_survive_snapshot_and_sync(bucket, tmp_path):
    from mybucket import sync

    data = fill(bucket, n=150)
    bucket.train_dictionary()
    snap = bucket.store.snapshot(tmp_path / "snap")
    assert snap["ok"]
    with open_store(tmp_path / "snap", readonly=True) as s:
        assert all(s.bucket(bucket.name).get(k) == v for k, v in data.items())
    with open_store(tmp_path / "copy") as dst:
        assert sync(bucket.store, dst)["ok"]
        assert all(dst.bucket(bucket.name).get(k) == v for k, v in data.items())
