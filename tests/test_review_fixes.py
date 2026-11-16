"""Regression tests for the v1 review findings (one section per finding)."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import mybucket
from mybucket import (
    BucketNotFound,
    ContentMismatch,
    DamagedError,
    DiskFull,
    InvalidArgument,
    InvalidMetadata,
    ObjectNotFound,
    PreconditionFailed,
    StoreError,
    open_store,
)
from mybucket import blobs as blobs_mod
from mybucket import codec as codec_mod
from mybucket.blobs import TempFile, blob_path, iter_tmp_files, remove_stale_tmp
from mybucket.maintenance import gc, verify
from mybucket.schema import translate_sqlite_error

from conftest import SRC, run_python


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def flip(path: Path, offset: int = 10) -> None:
    data = bytearray(path.read_bytes())
    for i in range(3):
        data[offset + i] ^= 0xFF
    path.write_bytes(bytes(data))


# -- a Bucket handle outlived by delete_bucket ----------------------------------------


def test_stale_handle_refuses_writes_and_reads_after_delete(store_path):
    a = open_store(store_path)
    b = open_store(store_path)
    old = a.bucket("data", create=True)
    old.put("k0", b"before")
    b.delete_bucket("data", force=True)

    with pytest.raises(BucketNotFound) as err:
        old.put("k1", b"after")
    assert "deleted or replaced" in err.value.message
    with pytest.raises(BucketNotFound):
        old.put("big", os.urandom(300_000))  # used to be a raw FileNotFoundError for tmp/
    with pytest.raises(BucketNotFound):
        old.get("k0")
    with pytest.raises(BucketNotFound):
        old.head("k0")
    with pytest.raises(BucketNotFound):
        old.list()
    with pytest.raises(BucketNotFound):
        a.bucket("data")  # the cached handle is dropped, and the bucket is gone
    assert b.list_buckets() == []
    a.close()
    b.close()


def test_stale_handle_gc_never_touches_a_recreated_bucket(store_path):
    a = open_store(store_path)
    b = open_store(store_path)
    old = a.bucket("photos", create=True)
    old.put("old.jpg", os.urandom(200_000))
    b.delete_bucket("photos", force=True)
    new = b.create_bucket("photos")
    picture = os.urandom(500_000)
    new.put("new.jpg", picture)
    blob = blob_path(new.path, sha(picture))
    os.utime(blob, (0, 0))  # older than any grace period

    for call in (lambda: gc(old, orphan_grace=0), old.verify, lambda: old.restore("old.jpg", 1)):
        with pytest.raises(BucketNotFound):
            call()
    assert blob.exists()
    assert new.get("new.jpg") == picture
    assert verify(new)["ok"]
    # The store hands out a fresh handle for the new bucket.
    fresh = a.bucket("photos")
    assert fresh is not old and fresh.get("new.jpg") == picture
    with pytest.raises(BucketNotFound):  # the old handle still refuses, it is not silently reused
        old.put("x", b"y")
    a.close()
    b.close()


def test_stale_handle_in_another_process(store_path):
    store = open_store(store_path)
    store.create_bucket("data")
    store.close()
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys\n"
            "from mybucket import open_store, BucketNotFound\n"
            f"b = open_store({str(store_path)!r}).bucket('data')\n"
            "b.put('k0', b'x')\n"
            "print('ready', flush=True)\n"
            "sys.stdin.readline()\n"
            "try:\n"
            "    b.put('k1', b'lost?')\n"
            "    print('wrote')\n"
            "except BucketNotFound as e:\n"
            "    print('refused', e.code)\n",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": str(SRC)},
    )
    assert holder.stdout.readline().strip() == "ready"
    run_python(f"from mybucket import open_store; open_store({str(store_path)!r}).delete_bucket('data', force=True)")
    out, _ = holder.communicate("go\n", timeout=60)
    assert out.strip() == "refused bucket_not_found"


# -- repair of a damaged stored copy --------------------------------------------------


def test_reput_repairs_a_damaged_file_blob(bucket):
    good = os.urandom(300_000)
    bucket.put("photo.jpg", good)
    path = blob_path(bucket.path, sha(good))
    flip(path)
    assert not verify(bucket)["ok"]

    info = bucket.put("photo.jpg", good)
    assert info.status == "unchanged" and info.repaired
    assert bucket.get("photo.jpg") == good
    assert verify(bucket)["ok"]
    # Already intact now: a further put is a plain no-op.
    again = bucket.put("photo.jpg", good)
    assert again.status == "unchanged" and not again.repaired


def test_dedup_put_never_attaches_a_key_to_damaged_content(bucket):
    good = os.urandom(300_000)
    bucket.put("photo.jpg", good)
    blob_path(bucket.path, sha(good)).unlink()  # missing file
    info = bucket.put("photo-copy.jpg", good)
    assert info.status == "created" and info.repaired
    assert bucket.get("photo-copy.jpg") == good and bucket.get("photo.jpg") == good
    assert verify(bucket)["ok"]


def test_reput_repairs_damaged_inline_content(bucket):
    data = b"hello world, " * 100
    bucket.put("note.txt", data)
    conn = sqlite3.connect(bucket.db_path)
    stored = conn.execute("SELECT data FROM blobs").fetchone()[0]
    bad = bytearray(stored)
    bad[len(bad) // 2] ^= 0xFF
    conn.execute("UPDATE blobs SET data = ?", (bytes(bad),))
    conn.commit()
    conn.close()
    with pytest.raises(DamagedError) as err:
        bucket.get("note.txt")
    assert "put it again" in err.value.hint
    info = bucket.put("note.txt", data)
    assert info.repaired and bucket.get("note.txt") == data
    assert verify(bucket)["ok"]


def test_batch_put_repairs_too(bucket):
    good = os.urandom(200_000)
    bucket.put("a", good)
    flip(blob_path(bucket.path, sha(good)))
    with bucket.batch() as tx:
        tx.put("b", good)
    assert tx.results[0].repaired
    assert bucket.get("a") == good and verify(bucket)["ok"]


def test_import_of_a_good_export_repairs_the_bucket(bucket, tmp_path):
    good = os.urandom(250_000)
    bucket.put("pics/cat.jpg", good)
    bucket.put("notes.txt", b"fine")
    bucket.export_dir(tmp_path / "export")
    flip(blob_path(bucket.path, sha(good)))
    result = bucket.import_dir(tmp_path / "export")
    assert result["repaired"] == 1 and result["unchanged"] == 2
    assert bucket.get("pics/cat.jpg") == good and verify(bucket)["ok"]


# -- list(start_after=...) with a delimiter ---------------------------------------------


def test_start_after_inside_a_common_prefix(bucket):
    for key in ("a/b", "a/c", "b"):
        bucket.put(key, b"x")
    page = bucket.list(delimiter="/", start_after="a/b")
    assert page.prefixes == ["a/"] and page.keys == ["b"]
    page = bucket.list(delimiter="/", start_after="a/c")
    assert page.prefixes == [] and page.keys == ["b"]
    # The cursor list() itself returns after a common prefix skips that prefix.
    page = bucket.list(delimiter="/", start_after="a/")
    assert page.prefixes == [] and page.keys == ["b"]
    assert bucket.list(start_after="a/b").keys == ["a/c", "b"]
    first = bucket.list(delimiter="/", limit=1)
    assert first.prefixes == ["a/"] and first.next_start_after == "a/"
    assert bucket.list(delimiter="/", start_after=first.next_start_after).keys == ["b"]


# -- streamed staging: disk use and checks ----------------------------------------------


def _track_tmp_peak(monkeypatch, tmp_dir: Path) -> list[int]:
    peak = [0]
    real = TempFile.write

    def write(self, data):
        real(self, data)
        total = sum(p.stat().st_size for p in tmp_dir.iterdir() if p.is_file())
        peak[0] = max(peak[0], total)

    monkeypatch.setattr(TempFile, "write", write)
    return peak


def test_streamed_incompressible_put_does_not_need_twice_its_size(bucket, monkeypatch):
    size = 40 * 1024 * 1024
    peak = _track_tmp_peak(monkeypatch, bucket.tmp_dir)
    info = bucket.put("video.bin", (os.urandom(1 << 20) for _ in range(size >> 20)))
    assert info.size == size and info.stored_size == size  # stored raw
    # Both copies exist only for the first DECIDE_AFTER bytes.
    assert peak[0] <= size + codec_mod.DECIDE_AFTER + (2 << 20), peak[0]
    assert list(iter_tmp_files(bucket.path)) == []


def test_streamed_compressible_put_keeps_only_the_zstd_copy(bucket, monkeypatch):
    size = 40 * 1024 * 1024
    peak = _track_tmp_peak(monkeypatch, bucket.tmp_dir)
    line = b'{"time": "2026-11-14T00:00", "steps": 400, "hr": 60}\n' * 20000
    info = bucket.put("log.ndjson", (line[: 1 << 20] for _ in range(size >> 20)))
    assert info.stored_size < size / 20
    assert peak[0] <= codec_mod.DECIDE_AFTER + (2 << 20), peak[0]


def test_compressible_start_then_incompressible_rest_is_stored_raw(bucket, monkeypatch):
    """The raw copy was dropped early; the zstd copy is turned back into raw and checked."""
    monkeypatch.setattr(codec_mod, "DECIDE_AFTER", 1 << 20)
    data = bytes(2 << 20) + os.urandom(60 << 20)
    info = bucket.put("mixed.bin", iter([data[i : i + (1 << 20)] for i in range(0, len(data), 1 << 20)]))
    assert info.stored_size == len(data)  # raw: compression saved less than 5%
    assert bucket.get("mixed.bin") == data
    assert list(iter_tmp_files(bucket.path)) == []


# -- plain-language errors instead of raw exceptions ------------------------------------


def test_disk_full_while_staging_is_a_plain_error(bucket, monkeypatch):
    bucket.put("keep", b"old")

    def full(self, data):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(TempFile, "write", full)
    with pytest.raises(DiskFull) as err:
        bucket.put("big", os.urandom(300_000))
    assert "Nothing was written" in err.value.message and err.value.hint
    with pytest.raises(DiskFull):
        with bucket.batch() as tx:
            tx.put("big", os.urandom(5 << 20))
    monkeypatch.undo()
    assert bucket.get("keep") == b"old" and not bucket.exists("big")
    assert list(iter_tmp_files(bucket.path)) == []


def test_file_too_large_maps_to_disk_full(bucket, monkeypatch):
    def too_big(self, data):
        raise OSError(errno.EFBIG, "File too large")

    monkeypatch.setattr(TempFile, "write", too_big)
    with pytest.raises(DiskFull):
        bucket.put("big", (bytes(1 << 20) for _ in range(6)))


def test_sqlite_disk_full_and_io_errors_are_explained():
    assert isinstance(translate_sqlite_error(sqlite3.OperationalError("database or disk is full"), "write 'k'"), DiskFull)
    err = translate_sqlite_error(sqlite3.OperationalError("disk I/O error"), "write 'k'")
    assert "input/output error" in err.message and err.hint


def test_surrogate_in_metadata_is_invalid_metadata(bucket):
    with pytest.raises(InvalidMetadata):
        bucket.put("k", b"x", metadata={"n": "\ud800"})


def test_content_type_must_look_like_a_mime_type(bucket):
    with pytest.raises(InvalidArgument):
        bucket.put("k", b"x", content_type="not a type")
    bucket.put("k", b"x", content_type="text/plain; charset=utf-8")


# -- expected size / checksum -----------------------------------------------------------


def test_expected_size_and_sha256(bucket):
    full = os.urandom(3 << 20)
    bucket.put("db.dump", full)
    with pytest.raises(ContentMismatch) as err:
        bucket.put("db.dump", full[:1_000_000], expected_size=len(full))
    assert err.value.exit_code == 5 and "nothing was written" in err.value.message
    with pytest.raises(ContentMismatch):
        bucket.put("db.dump", (full[i : i + 65536] for i in range(0, 1_000_000, 65536)), expected_sha256=sha(full))
    assert bucket.get("db.dump") == full and bucket.head("db.dump").version == 1
    assert bucket.put("db.dump", full + b"!", expected_size=len(full) + 1, expected_sha256=sha(full + b"!")).status == "updated"
    with pytest.raises(InvalidArgument):
        bucket.put("x", b"x", expected_size=-1)
    with bucket.batch() as tx:
        tx.put("z", b"kept")
        with pytest.raises(ContentMismatch):
            tx.put("y", b"abc", expected_size=4)
    # A failed tx.put() is not part of the batch; the other operations still are.
    assert not bucket.exists("y") and bucket.get("z") == b"kept"
    assert list(iter_tmp_files(bucket.path)) == []


# -- reads: streams are verified only at the end -----------------------------------------


def test_partial_stream_read_is_not_verified(bucket):
    data = os.urandom(600_000)
    bucket.put("big", data)
    flip(blob_path(bucket.path, sha(data)))
    with bucket.open("big") as stream:
        head = stream.read(1000)
        assert head != data[:1000] and not stream.verified
        with pytest.raises(DamagedError):
            stream.read()
    with bucket.open("big") as stream:
        with pytest.raises(DamagedError):
            stream.read()


def test_full_stream_read_is_verified(bucket):
    data = os.urandom(600_000)
    bucket.put("big", data)
    with bucket.open("big") as stream:
        assert stream.read() == data and stream.verified


# -- delete of a missing key, batches -------------------------------------------------------


def test_delete_missing_ok(bucket):
    with pytest.raises(ObjectNotFound) as err:
        bucket.delete("nope")
    assert "missing_ok=True" in err.value.hint
    assert bucket.delete("nope", missing_ok=True) is None
    bucket.put("a", b"1")
    with bucket.batch() as tx:
        tx.delete("a", missing_ok=True)
        tx.delete("drafts/old.json", missing_ok=True)
        tx.put("b", b"2")
    assert tx.results[0].key == "a" and tx.results[1] is None and tx.results[2].status == "created"
    with pytest.raises(ObjectNotFound):
        bucket.delete("nope", if_match=sha(b"x"), missing_ok=True)  # a condition needs the object


def test_nested_batch_is_refused(bucket):
    with pytest.raises(StoreError) as err:
        with bucket.batch() as outer:
            outer.put("nest/outer", b"o")
            with bucket.batch() as inner:
                inner.put("nest/inner", b"i")
    assert "already open" in err.value.message
    assert not bucket.exists("nest/inner") and not bucket.exists("nest/outer")
    with bucket.batch() as tx:  # the bucket is usable again afterwards
        tx.put("after", b"ok")
    assert bucket.get("after") == b"ok"


def test_precondition_hints_fit_the_case(bucket):
    bucket.put("lock", b"me")
    with pytest.raises(PreconditionFailed) as err:
        bucket.put("lock", b"you", if_none_match="*")
    assert "already exists, so it was left unchanged" in err.value.hint
    with pytest.raises(PreconditionFailed) as err:
        bucket.put("missing", b"x", if_match=sha(b"x"))
    assert "deleted or never existed" in err.value.hint


# -- the SDK exposes maintenance and transfer ------------------------------------------------


def test_maintenance_and_transfer_are_reachable_from_the_sdk(store, tmp_path):
    for name in ("gc", "gc_store", "verify", "verify_store", "snapshot", "restore", "export_dir", "import_dir"):
        assert name in mybucket.__all__ and callable(getattr(mybucket, name))
    b = store.bucket("notes", create=True)
    b.put("k", b"1")
    b.put("k", b"2")
    assert b.restore("k", 1).status == "updated" and b.get("k") == b"1"
    assert b.verify()["ok"] and "summary" in b.gc()
    assert store.verify()["ok"] and "summary" in store.gc()
    assert store.snapshot(tmp_path / "snap")["ok"]
    assert b.export_dir(tmp_path / "out")["written"] == 1
    assert b.import_dir(tmp_path / "out", prefix="again")["added"] == 1


def test_python_dash_m(tmp_path):
    proc = subprocess.run(
        [sys.executable, "-m", "mybucket", "--version"], capture_output=True, env={**os.environ, "PYTHONPATH": str(SRC)}
    )
    assert proc.returncode == 0 and proc.stdout.startswith(b"mybucket ")


# -- temp files: a cleaner racing a brand-new temp file ------------------------------------


def test_tempfile_survives_a_cleaner_between_create_and_flock(tmp_path, monkeypatch):
    tmp_dir = tmp_path / "tmp"
    real = blobs_mod.fcntl.flock
    removed = []

    def flock(fd, op):
        if not removed and op == blobs_mod.fcntl.LOCK_EX:
            # A cleaner saw the new file without a lock and removed it.
            [victim] = list(tmp_dir.iterdir())
            assert remove_stale_tmp(victim, min_age=0) == 0
            removed.append(victim)
        return real(fd, op)

    monkeypatch.setattr(blobs_mod.fcntl, "flock", flock)
    tmp = TempFile(tmp_dir)
    monkeypatch.undo()
    assert removed and tmp.path != removed[0] and tmp.path.exists()
    tmp.write(b"data")
    assert remove_stale_tmp(tmp.path, min_age=0) is None  # the writer holds the flock
    tmp.close()
    assert remove_stale_tmp(tmp.path, min_age=0) == 4


def test_gc_removes_temp_files_left_by_killed_writers(bucket):
    tmp = TempFile(bucket.tmp_dir)
    tmp.write(b"abandoned")
    tmp.close()  # the writer died: no flock any more
    os.utime(tmp.path, (0, 0))
    assert gc(bucket)["tmp_files_removed"] == 1
    assert list(iter_tmp_files(bucket.path)) == []


# -- docs: the README quick start runs as written --------------------------------------------


def _python_blocks(text: str) -> list[str]:
    blocks, inside, current = [], False, []
    for line in text.splitlines():
        if line.startswith("```python"):
            inside, current = True, []
        elif line.startswith("```") and inside:
            inside = False
            blocks.append("\n".join(current))
        elif inside:
            current.append(line)
    return blocks


def test_readme_python_examples_run(tmp_path):
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    blocks = _python_blocks(readme)
    assert blocks
    (tmp_path / "video.mp4").write_bytes(os.urandom(6 << 20))
    (tmp_path / "home").mkdir()
    code = "\n\n".join(blocks)
    from remote_helpers import install_fake_ssh

    install_fake_ssh(tmp_path / "bin")  # the ssh:// example runs against a local "remote"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PYTHONPATH": str(SRC),
            "HOME": str(tmp_path / "home"),
            "PATH": f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}",
        },
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "home" / "data" / "mystore" / "mybucket-store.json").is_file()


def test_metadata_json_is_canonical(bucket):
    info = bucket.put("k", b"x", metadata={"b": "2", "a": "1"})
    raw = sqlite3.connect(bucket.db_path).execute("SELECT meta FROM objects").fetchone()[0]
    assert raw == json.dumps({"a": "1", "b": "2"}, separators=(",", ":")) and info.metadata == {"a": "1", "b": "2"}
