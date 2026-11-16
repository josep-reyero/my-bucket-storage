"""Tests for gc, verify, snapshot and restore (src/mybucket/maintenance.py)."""

from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
from conftest import FakeClock, popen_python, run_python
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from mybucket import (
    Busy,
    InvalidArgument,
    ObjectNotFound,
    ReadOnlyError,
    Refused,
    VersionNotFound,
    maintenance,
    open_store,
)
from mybucket import bucket as bucket_mod
from mybucket.blobs import TempFile, blob_path, iter_blob_files, iter_tmp_files
from mybucket.maintenance import gc, gc_store, restore, snapshot, verify, verify_store

DAY_MS = 86_400_000


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def big(seed: int, size: int = 200_000) -> bytes:
    """Incompressible content that is stored as a blob file (> 128 KiB stored)."""
    return random.Random(seed).randbytes(size)


def small(seed: int) -> bytes:
    """Compressible content that is stored inline."""
    return (f"small content {seed} " * 50).encode()


def blob_row_count(bucket) -> int:
    with bucket._locked_conn() as conn:
        return conn.execute("SELECT count(*) FROM blobs").fetchone()[0]


def file_hexes(bucket) -> set[str]:
    return {h for h, _ in iter_blob_files(bucket.path)}


def age(path: Path, seconds: float = 7200) -> None:
    old = time.time() - seconds
    os.utime(path, (old, old))


def tree_state(root: Path) -> dict:
    return {
        str(p.relative_to(root)): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in sorted(root.rglob("*"))
        if p.is_file() and not p.name.endswith(("-shm", "-wal"))
    }


def db_dump(db_path: Path) -> list[str]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return list(conn.iterdump())
    finally:
        conn.close()


# -- gc: basics ----------------------------------------------------------------


def test_gc_on_empty_bucket(bucket):
    result = gc(bucket)
    assert result["trash_expired"] == 0 and result["blobs_removed"] == 0
    assert result["bytes_freed"] == 0
    assert "nothing to clean up" in result["summary"]
    json.dumps(result)


def test_gc_expires_trash_only_after_retention(bucket, clock):
    old_big, old_small = big(1), small(1)
    bucket.put("file", old_big)
    bucket.put("note", old_small)
    bucket.put("file", big(2))
    bucket.delete("note")
    assert len(file_hexes(bucket)) == 2

    clock.advance(days=6)
    result = gc(bucket)
    assert result["trash_expired"] == 0
    assert {v.etag for v in bucket.list_versions("file")} == {sha(old_big), sha(big(2))}

    clock.advance(days=1, seconds=1)
    result = gc(bucket)
    assert result["trash_expired"] == 2
    assert result["blobs_removed"] == 2
    assert result["inline_blobs_removed"] == 1 and result["file_blobs_removed"] == 1
    assert result["bytes_freed"] >= len(old_big)
    assert file_hexes(bucket) == {sha(big(2))}
    assert bucket.get("file") == big(2)
    assert not bucket.exists("note")
    assert [v.current for v in bucket.list_versions("file")] == [True]
    assert verify(bucket)["clean"]


def test_gc_now_overrides_the_clock(bucket, clock):
    bucket.put("k", b"one")
    bucket.put("k", b"two")
    assert gc(bucket)["trash_expired"] == 0
    assert gc(bucket, now=clock.now + 8 * DAY_MS)["trash_expired"] == 1
    with pytest.raises(InvalidArgument):
        gc(bucket, now="tomorrow")
    with pytest.raises(InvalidArgument):
        gc(bucket, orphan_grace=-1)


def test_gc_keeps_content_shared_with_live_objects(bucket, clock):
    shared = big(3)
    bucket.put("a", shared)
    bucket.put("b", shared)
    bucket.put("a", b"replaced")
    bucket.delete("b")
    clock.advance(days=30)
    result = gc(bucket)
    assert result["trash_expired"] == 2
    # Nothing references `shared` any more: gone. Now make a key reference it again.
    assert sha(shared) not in file_hexes(bucket)
    bucket.put("c", shared)
    bucket.put("d", shared)
    bucket.delete("d")
    clock.advance(days=30)
    gc(bucket)
    assert bucket.get("c") == shared  # still referenced by "c"
    assert sha(shared) in file_hexes(bucket)


def test_gc_with_trash_disabled_removes_overwritten_content_at_once(store):
    b = store.create_bucket("notrash", trash_days=0)
    b.put("k", big(4))
    b.put("k", small(4))
    assert blob_row_count(b) == 2
    result = gc(b)
    assert result["blobs_removed"] == 1 and result["file_blobs_removed"] == 1
    assert blob_row_count(b) == 1 and file_hexes(b) == set()
    assert b.get("k") == small(4)


def test_gc_removes_empty_fanout_directories(store):
    b = store.create_bucket("dirs", trash_days=0)
    b.put("k", big(5))
    b.delete("k")
    gc(b)
    assert list(b.blobs_dir.iterdir()) == []
    b.put("k", big(5))  # install() recreates the directories
    assert b.get("k") == big(5)


def test_gc_compacts_the_database(store):
    b = store.create_bucket("compact", trash_days=0)
    for i in range(40):
        b.put(f"k{i}", random.Random(i).randbytes(60_000))  # inline, raw
    for i in range(40):
        b.delete(f"k{i}")
    before = os.path.getsize(b.db_path) + os.path.getsize(str(b.db_path) + "-wal")
    result = gc(b)
    assert result["database_pages_freed"] > 0
    with b._locked_conn() as conn:
        assert conn.execute("PRAGMA freelist_count").fetchone()[0] == 0
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert os.path.getsize(b.db_path) < before


def test_gc_refuses_read_only(store, store_path):
    store.bucket("test-bucket", create=True).put("k", b"v")
    ro = open_store(store_path, readonly=True)
    with pytest.raises(ReadOnlyError):
        gc(ro.bucket("test-bucket"))
    with pytest.raises(ReadOnlyError):
        gc_store(ro)
    ro.close()


# -- gc: orphans, temp files, concurrent writers --------------------------------


CRASH_AFTER_INSTALL = """
import os
from mybucket import open_store
from mybucket import bucket as bucket_mod
b = open_store({path!r}).bucket({name!r})
def hook(point):
    if point == "after_install":
        os._exit(9)
bucket_mod._hook = hook
b.put("victim", {data!r})
"""


def test_gc_removes_crash_orphans_after_the_grace_period(bucket, store_path):
    bucket.put("victim", b"old content")
    data = big(6)
    proc = run_python(CRASH_AFTER_INSTALL.format(path=str(store_path), name=bucket.name, data=data), check=False)
    assert proc.returncode == 9, proc.stderr
    assert file_hexes(bucket) == {sha(data)}  # file installed, row never committed

    report = verify(bucket)
    assert report["ok"] and not report["clean"]
    assert [o["etag"] for o in report["orphan_files"]] == [sha(data)]

    result = gc(bucket)  # the orphan is younger than the default grace period
    assert result["orphan_files_removed"] == 0 and result["orphan_files_kept"] == 1
    assert sha(data) in file_hexes(bucket)
    assert "kept for now" in result["summary"]

    age(blob_path(bucket.path, sha(data)))
    result = gc(bucket)
    assert result["orphan_files_removed"] == 1
    assert file_hexes(bucket) == set()
    assert bucket.get("victim") == b"old content"
    assert verify(bucket)["clean"]


def test_gc_stale_temp_files(bucket):
    abandoned = TempFile(bucket.tmp_dir)
    abandoned.write(b"left by a crashed writer")
    abandoned.close()  # nobody holds its flock any more
    in_use = TempFile(bucket.tmp_dir)
    in_use.write(b"still being written")
    age(abandoned.path)
    age(in_use.path)
    recent = TempFile(bucket.tmp_dir)
    recent.close()
    age(recent.path, 120)
    brand_new = TempFile(bucket.tmp_dir)
    brand_new.close()

    result = gc(bucket)
    assert result["tmp_files_removed"] == 1
    assert not abandoned.path.exists()
    assert in_use.path.exists()  # old, but its writer holds the flock
    assert recent.path.exists()  # abandoned but younger than the default hour
    assert gc(bucket, tmp_min_age=0)["tmp_files_removed"] == 1
    assert not recent.path.exists() and in_use.path.exists()
    # Never below the floor: a just-created temp file may not hold its flock yet.
    assert brand_new.path.exists()
    in_use.discard()


def test_gc_never_removes_a_temp_file_before_its_writer_locks_it(bucket, monkeypatch):
    """TempFile creates the file and only then flocks it. gc running in that gap
    must not take the file for abandoned, even when asked for tmp_min_age=0."""
    from mybucket import blobs

    real_flock = blobs.fcntl.flock
    seen = {}

    def flock_after_gc(fd, op):
        if not seen:  # only the writer's first flock; gc's own checks pass through
            seen["running"] = True
            seen["result"] = gc(bucket, tmp_min_age=0)
        return real_flock(fd, op)

    monkeypatch.setattr(blobs.fcntl, "flock", flock_after_gc)
    tmp = TempFile(bucket.tmp_dir)
    monkeypatch.undo()
    assert seen["result"]["tmp_files_removed"] == 0
    assert tmp.path.exists()
    tmp.write(b"x")
    tmp.discard()


def test_gc_rechecks_under_the_lock_before_unlinking(bucket, monkeypatch):
    """A file seen without a row during the unlocked scan whose row is committed
    before gc takes the lock again must be kept."""
    data = big(7)
    path = blob_path(bucket.path, sha(data))
    path.parent.mkdir(parents=True)
    path.write_bytes(data)  # as if a writer had installed it and not yet committed
    age(path)

    def committer(point):
        if point == "gc_scanned":
            bucket.put("late", data)  # the writer commits between scan and re-check

    monkeypatch.setattr(maintenance, "_hook", committer)
    result = gc(bucket, orphan_grace=0)
    assert result["orphan_files_removed"] == 0
    assert bucket.get("late") == data
    assert verify(bucket)["clean"]


BLOCK_AFTER_INSTALL = """
import os, sys, time
from mybucket import open_store
from mybucket import bucket as bucket_mod
b = open_store({path!r}, timeout=30).bucket({name!r})
def hook(point):
    if point == "after_install":
        open({ready!r}, "w").close()
        while not os.path.exists({go!r}):
            time.sleep(0.01)
bucket_mod._hook = hook
info = b.put("pending", {data!r})
print(info.etag, flush=True)
"""


def test_gc_cannot_run_between_install_and_commit(store, store_path, tmp_path):
    """The engine installs files only while holding the write lock, so gc (which
    needs that lock) can never see an installed-but-uncommitted file as garbage."""
    data = big(8)
    ready, go = tmp_path / "ready", tmp_path / "go"
    b = store.bucket("waits", create=True)
    proc = popen_python(
        BLOCK_AFTER_INSTALL.format(path=str(store_path), name="waits", data=data, ready=str(ready), go=str(go)),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.time() + 30
        while not ready.exists():
            assert proc.poll() is None, proc.communicate()[1]
            assert time.time() < deadline
            time.sleep(0.01)
        assert sha(data) in file_hexes(b)  # installed, no row yet
        age(blob_path(b.path, sha(data)))  # even an "old" file is safe
        impatient = open_store(store_path, timeout=0.3)
        with pytest.raises(Busy):
            gc(impatient.bucket("waits"), orphan_grace=0)
        impatient.close()
        assert sha(data) in file_hexes(b)
        go.touch()
        out, err = proc.communicate(timeout=30)
        assert proc.returncode == 0, err
        assert out.strip() == sha(data)
    finally:
        go.touch()
        if proc.poll() is None:
            proc.kill()
    gc(b, orphan_grace=0)
    assert b.get("pending") == data


def test_gc_between_writer_precheck_and_commit(store, store_path, monkeypatch):
    """A writer that saw its content as already stored, then lost it to gc before
    taking the lock, re-creates the content instead of pointing at nothing."""
    b = store.create_bucket("race", trash_days=0)
    other = open_store(store_path).bucket("race")
    for data in (small(9), big(9)):
        b.put("old", data)
        b.delete("old")  # data is now unreferenced garbage
        real = b._prepare_outside_lock

        def racing(staged, real=real):
            real(staged)  # sees the unreferenced row: skips compression
            gc(other, orphan_grace=0)  # ...which another process collects right now

        monkeypatch.setattr(b, "_prepare_outside_lock", racing)
        b.put("new", data)
        monkeypatch.undo()
        assert b.get("new") == data
        assert verify(b)["ok"]
    other.store.close()


def test_gc_never_touches_an_in_flight_streamed_put(store, store_path, monkeypatch):
    b = store.create_bucket("stream")
    other = open_store(store_path).bucket("stream")
    chunks = [random.Random(i).randbytes(1 << 20) for i in range(6)]

    def feed():
        for i, chunk in enumerate(chunks):
            if i == 3:  # halfway through: the temp file is old-looking but in use
                for tmp in iter_tmp_files(b.path):
                    age(tmp)
                result = gc(other, tmp_min_age=0, orphan_grace=0)
                assert result["tmp_files_removed"] == 0
            yield chunk

    info = b.put("big", feed())
    assert info.etag == sha(b"".join(chunks))
    assert b.get("big") == b"".join(chunks)
    other.store.close()


def test_gc_store_cleans_every_bucket_and_leftover_dirs(store, clock):
    a = store.create_bucket("aaa")
    z = store.create_bucket("zzz")
    a.put("k", big(10))
    a.put("k", b"x")
    z.put("k", b"1")
    z.delete("k")
    deleting = store.buckets_dir / ".deleting-old-1234"
    stale_creating = store.buckets_dir / ".creating-old-5678"
    live_creating = store.buckets_dir / ".creating-new-9abc"
    for d in (deleting, stale_creating, live_creating):
        (d / "blobs").mkdir(parents=True)
    age(stale_creating)
    clock.advance(days=8)
    result = gc_store(store)
    assert [r["bucket"] for r in result["buckets"]] == ["aaa", "zzz"]
    assert sum(r["trash_expired"] for r in result["buckets"]) == 2
    assert result["leftover_dirs_removed"] == 2
    assert not deleting.exists() and not stale_creating.exists()
    assert live_creating.exists()  # may belong to a create_bucket() in progress
    json.dumps(result)


# -- gc never removes live data: randomised model test -----------------------------

POOL = [small(i) for i in range(3)] + [random.Random(99).randbytes(3000)] + [big(100 + i, 140_000) for i in range(3)]
KEYS = ["a", "b/c", "b/d", "e"]
CTYPES = [None, "text/plain"]


class GcMachine(RuleBasedStateMachine):
    """Random puts, overwrites, deletes, restores, clock jumps and gc runs.

    The model is the exact set of current objects and kept trash versions; after
    every gc the bucket must match it and verify clean.
    """

    def __init__(self) -> None:
        super().__init__()
        self.dir = tempfile.mkdtemp(prefix="mybucket-gc-")
        self.clock = FakeClock()
        self.store = open_store(Path(self.dir) / "store", clock=self.clock)
        self.trash_days = 2
        self.b = self.store.create_bucket("model", trash_days=self.trash_days)
        self.current: dict[str, tuple[int, int, str | None]] = {}  # key -> (version, pool idx, ctype)
        self.trash: dict[tuple[str, int], tuple[int, str | None, int]] = {}  # -> (idx, ctype, expires)
        self.last_version = 0

    def teardown(self) -> None:
        self.store.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def _trash_current(self, key: str) -> None:
        version, idx, ctype = self.current[key]
        if self.trash_days > 0:
            self.trash[(key, version)] = (idx, ctype, self.clock.now + self.trash_days * DAY_MS)

    @rule(key=st.sampled_from(KEYS), idx=st.integers(0, len(POOL) - 1), ctype=st.sampled_from(CTYPES))
    def put(self, key, idx, ctype):
        info = self.b.put(key, POOL[idx], content_type=ctype)
        cur = self.current.get(key)
        if cur is not None and cur[1] == idx and cur[2] == ctype:
            assert info.status == "unchanged"
            return
        assert info.version > self.last_version
        self.last_version = info.version
        if cur is not None:
            self._trash_current(key)
        self.current[key] = (info.version, idx, ctype)

    @precondition(lambda self: self.current)
    @rule(data=st.data())
    def delete(self, data):
        key = data.draw(st.sampled_from(sorted(self.current)))
        self.b.delete(key)
        self._trash_current(key)
        del self.current[key]

    @precondition(lambda self: self.trash)
    @rule(data=st.data())
    def restore_version(self, data):
        key, version = data.draw(st.sampled_from(sorted(self.trash)))
        idx, ctype, _ = self.trash[(key, version)]
        info = restore(self.b, key, version)
        assert info.etag == sha(POOL[idx])
        cur = self.current.get(key)
        if cur is not None and cur[1] == idx and cur[2] == ctype:
            assert info.status == "unchanged"
            return
        self.last_version = info.version
        if cur is not None:
            self._trash_current(key)
        self.current[key] = (info.version, idx, ctype)

    @rule(days=st.sampled_from([0, 0.5, 1, 3]))
    def advance(self, days):
        self.clock.advance(days=days)

    @rule(days=st.integers(0, 3))
    def configure(self, days):
        self.b.configure(trash_days=days)
        self.trash_days = days

    @rule()
    def collect(self):
        gc(self.b, orphan_grace=0)
        self.trash = {k: v for k, v in self.trash.items() if v[2] > self.clock.now}
        kept = set()
        for key in KEYS:
            try:
                versions = self.b.list_versions(key)
            except Exception:
                continue
            kept |= {(key, v.version, v.etag) for v in versions if not v.current}
        assert kept == {(k, ver, sha(POOL[v[0]])) for (k, ver), v in self.trash.items()}
        report = verify(self.b)
        assert report["clean"], report["summary"]
        # Exactly the content still referenced is stored.
        live = {v[1] for v in self.current.values()} | {v[0] for v in self.trash.values()}
        assert blob_row_count(self.b) == len(live)
        assert file_hexes(self.b) == {sha(POOL[i]) for i in live if len(POOL[i]) > 128 * 1024}

    @invariant()
    def live_data_is_intact(self):
        assert sorted(self.b.list().keys) == sorted(self.current)
        for key, (version, idx, ctype) in self.current.items():
            assert self.b.get(key) == POOL[idx]
            info = self.b.head(key)
            assert (info.version, info.content_type) == (version, ctype)


TestGcModel = GcMachine.TestCase
TestGcModel.settings = settings(
    max_examples=30,
    stateful_step_count=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)


# -- gc against concurrent writer processes ----------------------------------------

CHURN_WRITER = """
import json, os, random, sys
from mybucket import open_store
w = {w}
store = open_store({path!r}, timeout=60)
b = store.bucket("churn")
pool = [random.Random(500 + i).randbytes(150_000 + i) for i in range(5)]
pool += [("shared small %d " % i).encode() * 40 for i in range(5)]
rng = random.Random(w)
state = {{}}
for i in range({n}):
    key = "w%d/k%d" % (w, rng.randrange(4))
    if key in state and rng.random() < 0.25:
        b.delete(key)
        del state[key]
    else:
        data = rng.choice(pool)
        state[key] = b.put(key, data).etag
print(json.dumps(state), flush=True)
"""


def test_gc_during_concurrent_writes_never_loses_data(store_path):
    store = open_store(store_path, timeout=60)
    b = store.create_bucket("churn", trash_days=0)  # overwrites make garbage at once
    workers = 3
    procs = [
        popen_python(CHURN_WRITER.format(path=str(store_path), w=w, n=60), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for w in range(workers)
    ]
    runs = reads = 0
    deadline = time.time() + 180
    try:
        while any(p.poll() is None for p in procs):
            assert time.time() < deadline
            gc(b, orphan_grace=0, tmp_min_age=0)
            runs += 1
            for info in b.list().objects:
                try:
                    b.get(info.key)  # verified against its SHA-256: DamagedError if gc broke it
                    reads += 1
                except ObjectNotFound:
                    pass  # deleted meanwhile
        states = []
        for p in procs:
            out, err = p.communicate(timeout=60)
            assert p.returncode == 0, err
            states.append(json.loads(out))
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
    assert runs >= 2 and reads > 0
    expected = {k: v for s in states for k, v in s.items()}
    assert sorted(b.list().keys) == sorted(expected)
    for key, etag in expected.items():
        assert sha(b.get(key)) == etag
    assert verify(b)["ok"]
    gc(b, orphan_grace=0, tmp_min_age=0)
    report = verify(b)
    assert report["clean"], report
    store.close()


# -- verify ------------------------------------------------------------------------


def test_verify_clean_bucket(bucket):
    bucket.put("inline", small(1), content_type="text/plain")
    bucket.put("file", big(1))
    bucket.put("file", big(2))  # one version in the trash
    report = verify(bucket)
    assert report["ok"] and report["clean"]
    assert report["objects"] == 2 and report["trash_versions"] == 1
    assert report["blobs_checked"] == 3
    assert report["damaged"] == [] and report["database"] == []
    assert "intact" in report["summary"]
    json.dumps(report)


def _flip_inline_byte(bucket, digest: str, offset: int = 5) -> None:
    conn = sqlite3.connect(str(bucket.db_path))
    try:
        (data,) = conn.execute("SELECT data FROM blobs WHERE hash = ?", (bytes.fromhex(digest),)).fetchone()
        damaged = bytearray(data)
        damaged[offset] ^= 0x01
        conn.execute("UPDATE blobs SET data = ? WHERE hash = ?", (bytes(damaged), bytes.fromhex(digest)))
        conn.commit()
    finally:
        conn.close()


def _flip_file_byte(bucket, digest: str, offset: int = 1000) -> None:
    path = blob_path(bucket.path, digest)
    with open(path, "r+b") as fh:
        fh.seek(offset)
        byte = fh.read(1)
        fh.seek(offset)
        fh.write(bytes([byte[0] ^ 0x80]))


@pytest.mark.parametrize("content", ["compressed", "raw"])
def test_verify_detects_flipped_byte_inline(bucket, content):
    data = small(2) if content == "compressed" else random.Random(2).randbytes(5000)
    bucket.put("victim", data)
    bucket.put("fine", small(3))
    with bucket._locked_conn() as conn:
        codec = conn.execute("SELECT codec FROM blobs WHERE hash = ?", (bytes.fromhex(sha(data)),)).fetchone()[0]
    assert codec == (1 if content == "compressed" else 0)
    _flip_inline_byte(bucket, sha(data))
    report = verify(bucket)
    assert not report["ok"] and not report["clean"]
    assert len(report["damaged"]) == 1
    entry = report["damaged"][0]
    assert entry["etag"] == sha(data) and entry["location"] == "inline" and entry["path"] is None
    assert entry["versions"] == [{"key": "victim", "version": 1, "current": True}]
    assert "damaged" in report["summary"] and report["hint"]


def test_verify_detects_flipped_byte_in_blob_file(bucket):
    data = big(11)
    bucket.put("victim", data)
    bucket.put("victim", b"newer")  # the damaged content is now only in the trash
    bucket.put("fine", big(12))
    _flip_file_byte(bucket, sha(data))
    report = verify(bucket)
    assert not report["ok"]
    [entry] = report["damaged"]
    assert entry["location"] == "file"
    assert entry["path"] == f"blobs/{sha(data)[:2]}/{sha(data)[2:4]}/{sha(data)}"
    assert "SHA-256" in entry["problem"]
    assert entry["versions"] == [{"key": "victim", "version": 1, "current": False}]
    assert report["blobs_checked"] == 3


def test_verify_detects_compressed_blob_file_damage(bucket):
    data = (b"compressible but large " * 30_000) + big(13, 150_000)
    bucket.put("z", data)
    with bucket._locked_conn() as conn:
        codec, inline = conn.execute("SELECT codec, data IS NOT NULL FROM blobs").fetchone()
    assert (codec, inline) == (1, 0)
    _flip_file_byte(bucket, sha(data), offset=50_000)
    [entry] = verify(bucket)["damaged"]
    assert entry["location"] == "file"


def test_verify_detects_deleted_and_truncated_blob_files(bucket):
    gone, short = big(14), big(15)
    bucket.put("gone", gone)
    bucket.put("short", short)
    blob_path(bucket.path, sha(gone)).unlink()
    with open(blob_path(bucket.path, sha(short)), "r+b") as fh:
        fh.truncate(1000)
    report = verify(bucket)
    assert not report["ok"]
    problems = {d["versions"][0]["key"]: d["problem"] for d in report["damaged"]}
    assert problems["gone"] == "its blob file is missing"
    assert "1000 bytes" in problems["short"]


def test_verify_reports_garbage_without_calling_it_damage(store):
    b = store.create_bucket("garbage", trash_days=0)
    b.put("k", big(16))
    b.put("k", b"replacement")  # big(16) is now an unreferenced row + file
    stray = blob_path(b.path, "ab" * 32)
    stray.parent.mkdir(parents=True, exist_ok=True)
    stray.write_bytes(b"orphan")
    (b.blobs_dir / "README.txt").write_text("not a blob")
    tmp = TempFile(b.tmp_dir)
    tmp.close()
    age(tmp.path)
    report = verify(b)
    assert report["ok"] and not report["clean"]
    assert [o["etag"] for o in report["orphan_files"]] == ["ab" * 32]
    assert report["unexpected_files"] == ["blobs/README.txt"]
    assert report["unreferenced_blobs"] == 1
    assert report["stale_tmp_files"] == 1 and report["tmp_files"] == 1
    assert "gc" in report["hint"]
    age(stray)
    gc(b, orphan_grace=0)
    after = verify(b)
    assert after["orphan_files"] == [] and after["unreferenced_blobs"] == 0 and after["stale_tmp_files"] == 0
    assert after["unexpected_files"] == ["blobs/README.txt"]  # gc never deletes what it does not own


def test_verify_detects_rows_pointing_at_missing_content_and_bad_keys(bucket):
    bucket.put("good", b"fine")
    conn = sqlite3.connect(str(bucket.db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "INSERT INTO objects (key, version, blob_id, content_type, meta, modified) VALUES ('dangling', 50, 999, NULL, NULL, 0)"
        )
        conn.execute(
            "INSERT INTO objects (key, version, blob_id, content_type, meta, modified) "
            "SELECT '../escape', 2, blob_id, NULL, NULL, 0 FROM objects WHERE key = 'good'"
        )
        conn.commit()
    finally:
        conn.close()
    report = verify(bucket)
    assert not report["ok"]
    assert report["missing_content"] == [{"key": "dangling", "version": 50, "current": True}]
    assert report["invalid_keys"] == ["../escape"]
    assert any("missing row in blobs" in d for d in report["database"])
    assert any("version counter" in d for d in report["database"])


def test_verify_changes_nothing(bucket):
    bucket.put("a", big(17))
    bucket.put("b", small(17))
    _flip_file_byte(bucket, sha(big(17)))
    orphan = blob_path(bucket.path, "cd" * 32)
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"x")
    age(orphan)
    files_before = tree_state(bucket.path)
    dump_before = db_dump(bucket.db_path)
    verify(bucket)
    assert tree_state(bucket.path) == files_before
    assert db_dump(bucket.db_path) == dump_before


def test_verify_works_read_only(store, store_path):
    b = store.create_bucket("ro")
    b.put("k", big(18))
    ro = open_store(store_path, readonly=True)
    report = verify_store(ro)
    assert report["ok"] and report["clean"]
    assert [r["bucket"] for r in report["buckets"]] == ["ro"]
    _flip_file_byte(b, sha(big(18)))
    report = verify_store(ro)
    assert not report["ok"] and "ro" in report["summary"]
    ro.close()


def test_verify_skips_content_gc_removes_meanwhile(store, store_path, monkeypatch):
    b = store.create_bucket("moving", trash_days=0)
    b.put("k", big(19))
    b.put("k", b"small now")  # big(19) is garbage
    other = open_store(store_path).bucket("moving")
    real = maintenance.check_blob
    calls = {"n": 0}

    def check_then_collect(path, row):
        calls["n"] += 1
        if calls["n"] == 1:
            gc(other, orphan_grace=0)  # collects the row and file verify is looking at
        return real(path, row)

    monkeypatch.setattr(maintenance, "check_blob", check_then_collect)
    report = verify(b)
    assert report["ok"], report["damaged"]
    other.store.close()


# -- snapshot --------------------------------------------------------------------


def _fill(store) -> dict[str, bytes]:
    b = store.bucket("photos", create=True)
    docs = store.bucket("docs", create=True)
    content = {"photos/big": big(20), "photos/old": big(21), "docs/note": small(20)}
    b.put("big", big(20), content_type="image/jpeg", metadata={"camera": "x"})
    b.put("old", big(21))
    b.put("old", b"overwritten")
    docs.put("note", small(20))
    return content


def test_snapshot_is_a_valid_independent_store(store, tmp_path):
    _fill(store)
    dest = tmp_path / "snap"
    result = snapshot(store, dest)
    assert result["ok"]
    assert [r["bucket"] for r in result["buckets"]] == ["docs", "photos"]
    assert result["linked"] == 2 and result["copied"] == 0
    json.dumps(result)
    src_file = blob_path(store.bucket("photos").path, sha(big(20)))
    assert blob_path(dest / "buckets" / "photos", sha(big(20))).stat().st_ino == src_file.stat().st_ino
    assert oct((dest / "buckets" / "photos" / "bucket.sqlite").stat().st_mode & 0o777) == "0o600"

    snap = open_store(dest, readonly=True)
    assert snap.list_buckets() == ["docs", "photos"]
    report = verify_store(snap)
    assert report["ok"] and report["clean"], report
    sp = snap.bucket("photos")
    assert sp.get("big") == big(20)
    assert sp.head("big").metadata == {"camera": "x"}
    assert [v.etag for v in sp.list_versions("old")] == [sha(b"overwritten"), sha(big(21))]
    snap.close()

    # Later changes to the store do not reach the snapshot, even after gc.
    store.bucket("photos").delete("big")
    gc(store.bucket("photos"), now=store._now() + 30 * DAY_MS)
    assert not src_file.exists()
    snap = open_store(dest, readonly=True)
    assert snap.bucket("photos").get("big") == big(20)
    snap.close()


def test_snapshot_copies_when_hard_links_fail(store, tmp_path, monkeypatch):
    _fill(store)

    def no_link(src, dst, *a, **kw):
        raise OSError(18, "Invalid cross-device link")

    monkeypatch.setattr(os, "link", no_link)
    result = snapshot(store, tmp_path / "snap")
    assert result["linked"] == 0 and result["copied"] == 2
    assert result["bytes_copied"] == len(big(20)) + len(big(21))
    snap = open_store(tmp_path / "snap", readonly=True)
    assert verify_store(snap)["clean"]
    assert snap.bucket("photos").get("big") == big(20)
    snap.close()


def test_snapshot_of_some_buckets_and_read_only_store(store, store_path, tmp_path):
    _fill(store)
    ro = open_store(store_path, readonly=True)
    result = snapshot(ro, tmp_path / "snap", buckets=["docs"])
    assert [r["bucket"] for r in result["buckets"]] == ["docs"]
    snap = open_store(tmp_path / "snap", readonly=True)
    assert snap.list_buckets() == ["docs"]
    assert snap.bucket("docs").get("note") == small(20)
    snap.close()
    ro.close()


def test_snapshot_refuses_unsafe_destinations(store, store_path, tmp_path):
    _fill(store)
    busy = tmp_path / "busy"
    busy.mkdir()
    (busy / "keep.txt").write_text("mine")
    with pytest.raises(Refused):
        snapshot(store, busy)
    assert (busy / "keep.txt").read_text() == "mine"
    afile = tmp_path / "file"
    afile.write_text("x")
    with pytest.raises(Refused):
        snapshot(store, afile)
    with pytest.raises(InvalidArgument):
        snapshot(store, store_path / "inside")
    empty = tmp_path / "empty"
    empty.mkdir()
    assert snapshot(store, empty)["ok"]


def test_failed_snapshot_leaves_nothing_behind(store, tmp_path, monkeypatch):
    _fill(store)

    def boom(*args):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(maintenance, "_backup", boom)
    with pytest.raises(OSError):
        snapshot(store, tmp_path / "snap")
    assert not (tmp_path / "snap").exists()
    (tmp_path / "empty").mkdir()
    with pytest.raises(OSError):
        snapshot(store, tmp_path / "empty")
    assert list((tmp_path / "empty").iterdir()) == []


def test_snapshot_retries_when_gc_removes_a_file_after_the_backup(store, clock, tmp_path, monkeypatch):
    b = store.create_bucket("racy")
    b.put("gone", big(22))
    b.put("stay", big(23))
    real = maintenance._snapshot_contents
    calls = {"n": 0}

    def contents_then_gc(db_path):
        out = real(db_path)
        calls["n"] += 1
        if calls["n"] == 1:  # right after the first backup, the key expires and gc runs
            b.delete("gone")
            clock.advance(days=8)
            gc(b)
        return out

    monkeypatch.setattr(maintenance, "_snapshot_contents", contents_then_gc)
    result = snapshot(store, tmp_path / "snap")
    [r] = result["buckets"]
    assert r["attempts"] == 2 and r["missing"] == []
    snap = open_store(tmp_path / "snap", readonly=True)
    assert verify_store(snap)["clean"]
    sb = snap.bucket("racy")
    assert sb.list().keys == ["stay"] and sb.get("stay") == big(23)
    snap.close()


def test_snapshot_reports_files_missing_in_the_source(store, tmp_path):
    b = store.create_bucket("hurt")
    b.put("lost", big(24))
    b.put("ok", big(25))
    blob_path(b.path, sha(big(24))).unlink()
    result = snapshot(store, tmp_path / "snap")
    assert not result["ok"]
    [r] = result["buckets"]
    assert r["missing"] == [sha(big(24))] and r["attempts"] == maintenance.SNAPSHOT_ATTEMPTS
    assert "verify" in result["hint"]
    snap = open_store(tmp_path / "snap", readonly=True)
    report = verify(snap.bucket("hurt"))
    assert not report["ok"]
    assert report["damaged"][0]["problem"] == "its blob file is missing"
    assert snap.bucket("hurt").get("ok") == big(25)
    snap.close()


SNAP_WRITER = """
import os, random, sys
from mybucket import open_store
store = open_store({path!r}, timeout=60)
b = store.bucket("live")
rng = random.Random(7)
i = 0
while not os.path.exists({stop!r}):
    data = rng.randbytes(rng.choice([2_000, 150_000, 300_000])) + str(i).encode()
    with b.batch() as tx:
        tx.put("pair/%06d/a" % i, data)
        tx.put("pair/%06d/b" % i, data)
        tx.put("latest", str(i).encode())
        if i >= 3:
            tx.delete("pair/%06d/a" % (i - 3))
            tx.delete("pair/%06d/b" % (i - 3))
    i += 1
print(i, flush=True)
"""

SNAP_GC = """
import os, time
from mybucket import open_store
from mybucket.maintenance import gc
store = open_store({path!r}, timeout=60)
b = store.bucket("live")
n = 0
while not os.path.exists({stop!r}):
    gc(b, orphan_grace=0, tmp_min_age=0)
    n += 1
    time.sleep(0.02)
print(n, flush=True)
"""


def test_snapshot_during_concurrent_writes_verifies_clean(store_path, tmp_path):
    store = open_store(store_path, timeout=60)
    store.create_bucket("live", trash_days=0)
    stop = tmp_path / "stop"
    procs = [
        popen_python(code.format(path=str(store_path), stop=str(stop)), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for code in (SNAP_WRITER, SNAP_GC)
    ]
    snaps = []
    try:
        deadline = time.time() + 60
        while not store.bucket("live").exists("latest"):  # the writer has committed once
            assert time.time() < deadline and procs[0].poll() is None, procs[0].communicate()
            time.sleep(0.02)
        for n in range(4):
            assert all(p.poll() is None for p in procs), [p.communicate() for p in procs]
            dest = tmp_path / f"snap{n}"
            snapshot(store, dest)
            snaps.append(dest)
            time.sleep(0.15)
    finally:
        stop.touch()
        outs = []
        for p in procs:
            out, err = p.communicate(timeout=60)
            assert p.returncode == 0, err
            outs.append(int(out))
    assert outs[0] > 4, "the writer barely ran"
    seen_latest = []
    for dest in snaps:
        snap = open_store(dest)
        sb = snap.bucket("live")
        report = verify(sb)
        assert report["ok"], report
        assert report["orphan_files"] == [] and report["unexpected_files"] == []
        keys = sb.list().keys
        latest = int(sb.get("latest"))
        seen_latest.append(latest)
        pairs = sorted({k.split("/")[1] for k in keys if k.startswith("pair/")})
        # Consistent: whole batches only, exactly the last three of them.
        assert [int(p) for p in pairs] == list(range(max(0, latest - 2), latest + 1))
        for p in pairs:
            assert sb.get(f"pair/{p}/a") == sb.get(f"pair/{p}/b")
            assert sb.get(f"pair/{p}/a").endswith(str(int(p)).encode())
        gc(sb, orphan_grace=0, tmp_min_age=0)
        assert verify(sb)["clean"]
        snap.close()
    assert seen_latest == sorted(seen_latest) and len(set(seen_latest)) > 1
    store.close()


# -- restore ---------------------------------------------------------------------


def test_restore_after_overwrite(bucket):
    v1 = bucket.put("doc", b"first", content_type="text/plain", metadata={"n": "1"})
    v2 = bucket.put("doc", b"second")
    info = restore(bucket, "doc", v1.version)
    assert info.status == "updated"
    assert info.version > v2.version
    assert (info.etag, info.content_type, info.metadata) == (sha(b"first"), "text/plain", {"n": "1"})
    assert bucket.get("doc") == b"first"
    assert bucket.head("doc").version == info.version
    versions = bucket.list_versions("doc")
    assert [(v.version, v.current) for v in versions] == [(info.version, True), (v2.version, False), (v1.version, False)]
    # The restore can itself be undone.
    again = restore(bucket, "doc", v2.version)
    assert bucket.get("doc") == b"second" and again.status == "updated"


def test_restore_after_delete(bucket):
    v1 = bucket.put("doc", big(30))
    bucket.delete("doc")
    info = restore(bucket, "doc", v1.version)
    assert info.status == "created"
    assert bucket.get("doc") == big(30)
    assert info.stored_size == bucket.head("doc").stored_size


def test_restore_noops_and_errors(bucket, clock):
    v1 = bucket.put("doc", b"same")
    assert restore(bucket, "doc", v1.version).status == "unchanged"
    bucket.put("doc", b"other")
    v3 = bucket.put("doc", b"same")
    restored = restore(bucket, "doc", v1.version)  # same content, type and metadata as current
    assert restored.status == "unchanged" and restored.version == v3.version
    with pytest.raises(VersionNotFound) as exc:
        restore(bucket, "doc", 999)
    assert exc.value.exit_code == 3 and exc.value.hint
    with pytest.raises(VersionNotFound):
        restore(bucket, "never-existed", 1)
    for bad in (0, -1, "1", True, 1.0):
        with pytest.raises(InvalidArgument):
            restore(bucket, "doc", bad)
    # Expired but not yet collected: still restorable. After gc: gone.
    clock.advance(days=8)
    v2 = [v for v in bucket.list_versions("doc") if v.etag == sha(b"other")][0]
    assert restore(bucket, "doc", v2.version).status == "updated"
    restore(bucket, "doc", v3.version)
    clock.advance(days=8)
    gc(bucket)
    with pytest.raises(VersionNotFound):
        restore(bucket, "doc", v2.version)


def test_restore_with_trash_disabled_does_not_keep_the_replaced_version(store):
    b = store.create_bucket("keep1", trash_days=1)
    v1 = b.put("k", b"one")
    b.put("k", b"two")
    b.configure(trash_days=0)
    info = restore(b, "k", v1.version)
    assert b.get("k") == b"one"
    assert [v.version for v in b.list_versions("k")] == [info.version, v1.version]


def test_restore_refuses_read_only(store, store_path):
    b = store.bucket("test-bucket", create=True)
    v1 = b.put("k", b"one")
    b.put("k", b"two")
    ro = open_store(store_path, readonly=True)
    with pytest.raises(ReadOnlyError):
        restore(ro.bucket("test-bucket"), "k", v1.version)
    ro.close()


def test_maintenance_does_not_leave_hooks_set():
    assert maintenance._hook is None and bucket_mod._hook is None
