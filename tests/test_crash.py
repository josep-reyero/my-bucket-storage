"""Crash safety under SIGKILL at random moments.

A child process does an endless random mix of puts (empty, small, inline,
blob-file and streamed objects over 4 MiB, some with conditions), deletes,
batches, refused conditional writes and bucket create/delete. Before each
operation it appends an "intent" line to a log file and after it returns an
"ack" line (one write() each, so a kill never leaves half a line). The parent
kills it with SIGKILL at a random moment, then checks:

- every acknowledged operation is intact (content, ETag) after reopening;
- the one in-flight operation either fully happened or did not happen at all
  (a batch is all-or-nothing);
- every object that can be read is exactly right, and every blob row in the
  database (current or trash) passes the stored-size and SHA-256 check;
- SQLite integrity_check and foreign_key_check pass;
- a read-only store can open and read the bucket right after the kill (with
  no other connection open, so SQLite has to recover the WAL index);
- blob files without a row are only ever content of an in-flight write, and
  temp files left behind are recognisably stale;
- the store still accepts writes.

Scale: MYBUCKET_CRASH_ROUNDS (default 30) and MYBUCKET_CRASH_SEED (default 1).
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest
from conftest import SRC

from mybucket import open_store
from mybucket.blobs import iter_blob_files, iter_tmp_files, tmp_is_stale
from mybucket.errors import ObjectNotFound

try:
    from mybucket import maintenance
except ImportError:  # built by another part of the project; the gc step is skipped
    maintenance = None


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


KEYS = ["a", "a/b", "a/b/c", "a-b", "ab", "big", "big/x", "e", "f/1", "f/2", "g", "h"]

CHILD = r"""
import hashlib, json, os, random, sys
from mybucket import open_store
from mybucket.errors import PreconditionFailed

path, seed, log_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
KEYS = json.loads(sys.argv[4])
rng = random.Random(seed)
log = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)

def emit(*parts):
    os.write(log, (json.dumps(parts) + "\n").encode())

def sha(data):
    return hashlib.sha256(data).hexdigest()

def make(n):
    kind = rng.random()
    if kind < 0.08:
        return b""
    if kind < 0.45:
        size = rng.choice([1, 50, 1500, 30_000])
    elif kind < 0.85:
        size = rng.choice([131_072, 131_073, 200_000, 700_000])
    else:
        size = rng.choice([4 * 1024 * 1024, 4 * 1024 * 1024 + 1, 9 * 1024 * 1024])
    body = rng.randbytes(min(size, 64))
    if rng.random() < 0.5:
        data = rng.randbytes(size)  # incompressible
    else:
        data = (body * (size // max(len(body), 1) + 1))[:size]
    return data[:-8] + n.to_bytes(8, "big") if size >= 8 else data

def chunks(data):
    for i in range(0, len(data), 1 << 20):
        yield data[i : i + (1 << 20)]

store = open_store(path, timeout=30)
b = store.bucket("crash")
cur = {}
for k in KEYS:
    try:
        cur[k] = b.head(k).etag
    except Exception:
        cur[k] = None
emit("ready")
n = seed * 1_000_000
while True:
    n += 1
    r = rng.random()
    key = rng.choice(KEYS)
    if r < 0.50:
        data = make(n)
        kwargs = {}
        if cur[key] is not None and rng.random() < 0.3:
            kwargs["if_match"] = cur[key]
        elif cur[key] is None and rng.random() < 0.3:
            kwargs["if_none_match"] = "*"
        source = chunks(data) if rng.random() < 0.3 else data
        emit("intent", n, "put", [[key, sha(data)]])
        b.put(key, source, content_type=rng.choice([None, "application/octet-stream"]), **kwargs)
        cur[key] = sha(data)
        emit("ack", n)
    elif r < 0.58:
        if cur[key] is None:
            continue
        emit("intent", n, "put", [[key, cur[key]]])  # identical re-put: unchanged
        b.put(key, b.get(key), content_type=b.head(key).content_type)
        emit("ack", n)
    elif r < 0.70:
        if cur[key] is None:
            continue
        emit("intent", n, "delete", [[key, None]])
        if rng.random() < 0.5:
            b.delete(key, if_match=cur[key])
        else:
            b.delete(key)
        cur[key] = None
        emit("ack", n)
    elif r < 0.85:
        picked = rng.sample(KEYS, rng.randint(2, 4))
        plan = []
        for k in picked:
            if cur[k] is not None and rng.random() < 0.25:
                plan.append([k, None])
            else:
                plan.append([k, make(n)])
        emit("intent", n, "batch", [[k, None if d is None else sha(d)] for k, d in plan])
        with b.batch() as tx:
            for k, d in plan:
                if d is None:
                    tx.delete(k)
                else:
                    tx.put(k, d)
        for k, d in plan:
            cur[k] = None if d is None else sha(d)
        emit("ack", n)
    elif r < 0.92:
        # A write that must be refused: wrong ETag. Nothing may change.
        emit("intent", n, "refused", [])
        try:
            b.put(key, make(n), if_match="0" * 64)
        except PreconditionFailed:
            emit("ack", n)
        else:
            emit("unexpected", n, "a put with a wrong if_match was applied")
    else:
        name = "extra-%d" % rng.randrange(4)
        if name in store.list_buckets():
            emit("intent", n, "rb", name)
            store.delete_bucket(name, force=True)
        else:
            emit("intent", n, "mb", name)
            store.create_bucket(name).put("x", b"hello")
        emit("ack", n)
"""


def _spawn(store_path: Path, seed: int, log_path: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(CHILD), str(store_path), str(seed), str(log_path), json.dumps(KEYS)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _wait_ready(log_path: Path, proc: subprocess.Popen, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if log_path.exists() and b'["ready"]' in log_path.read_bytes():
            return
        if proc.poll() is not None:
            raise AssertionError(f"child exited early: {proc.stderr.read().decode()}")
        time.sleep(0.005)
    raise AssertionError("child never got ready")


def _parse_log(log_path: Path):
    """Return (acked operations in order, the in-flight operation or None)."""
    acked, pending = [], None
    for line in log_path.read_bytes().splitlines():
        entry = json.loads(line)
        if entry[0] == "intent":
            assert pending is None, "an intent without an ack before the next intent"
            pending = entry
        elif entry[0] == "ack":
            assert pending is not None and pending[1] == entry[1]
            acked.append(pending)
            pending = None
        elif entry[0] == "unexpected":
            raise AssertionError(f"child saw a product bug: {entry}")
    return acked, pending


def _state(bucket) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for key in KEYS:
        try:
            out[key] = bucket.head(key).etag
        except ObjectNotFound:
            out[key] = None
    return out


def _check_readable(bucket) -> int:
    """Every listed object reads back exactly; returns how many there are."""
    listing = bucket.list()
    for info in listing.objects:
        data = bucket.get(info.key)
        assert sha(data) == info.etag and len(data) == info.size, info.key
    return len(listing.objects)


def test_sigkill_at_random_moments(tmp_path):
    rounds = _env_int("MYBUCKET_CRASH_ROUNDS", 30)
    seed = _env_int("MYBUCKET_CRASH_SEED", 1)
    rng = random.Random(seed)
    store_path = tmp_path / "store"
    with open_store(store_path) as s:
        s.create_bucket("crash")

    expected: dict[str, str | None] = dict.fromkeys(KEYS)
    buckets: set[str] = set()
    half_made: set[str] = set()  # created by an interrupted "mb", maybe before its first object
    in_flight_hashes: set[str] = set()  # contents an interrupted write may have left as orphan files
    totals = {"acked": 0, "in_flight": 0, "in_flight_applied": 0, "early_kills": 0}

    for rnd in range(rounds):
        log_path = tmp_path / f"log-{rnd}.jsonl"
        proc = _spawn(store_path, seed * 1000 + rnd, log_path)
        if rng.random() < 0.1:
            time.sleep(rng.random() * 0.3)  # sometimes kill during start-up
            totals["early_kills"] += 1
        else:
            _wait_ready(log_path, proc)
            time.sleep(rng.choice([0.0, 0.001, 0.01]) if rng.random() < 0.2 else rng.random() * 1.2)
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=30)
        stderr = proc.stderr.read().decode()
        proc.stderr.close()
        assert proc.returncode == -signal.SIGKILL, f"child died on its own:\n{stderr}"

        acked, pending = _parse_log(log_path) if log_path.exists() else ([], None)
        for entry in acked:
            _apply(expected, buckets, entry)
            if entry[2] in ("mb", "rb"):
                half_made.discard(entry[3])
        totals["acked"] += len(acked)

        # 1. Read-only open first, while no connection is open at all.
        with open_store(store_path, readonly=True) as ro:
            rb = ro.bucket("crash")
            ro_state = _state(rb)
            _check_readable(rb)

        # 2. Read-write: the acknowledged state plus at most the in-flight operation.
        with open_store(store_path) as s:
            b = s.bucket("crash")
            actual = _state(b)
            assert actual == ro_state, "read-only and read-write opens disagree"
            if pending is not None and pending[2] in ("put", "delete", "batch"):
                totals["in_flight"] += 1
                changes = dict((k, v) for k, v in pending[3])
                after = {**expected, **changes}
                in_flight_hashes.update(v for v in changes.values() if v)
                assert actual in (expected, after), (
                    f"round {rnd}: in-flight {pending[2]} is half-applied or an acked write is lost:\n"
                    f"expected {expected}\nor       {after}\ngot      {actual}"
                )
                if actual == after and after != expected:
                    totals["in_flight_applied"] += 1
                expected = actual
            else:
                assert actual == expected, f"round {rnd}: acked state lost\nexpected {expected}\ngot {actual}"
                if pending is not None and pending[2] in ("mb", "rb"):
                    name = pending[3]
                    if name in s.list_buckets():
                        buckets.add(name)
                        if pending[2] == "mb":
                            half_made.add(name)
                    else:
                        buckets.discard(name)
                        half_made.discard(name)
            assert set(s.list_buckets()) == {"crash"} | buckets
            for name in buckets:
                extra = s.bucket(name)
                if name in half_made:
                    assert extra.list().keys in ([], ["x"])
                else:
                    assert extra.get("x") == b"hello"

            _check_readable(b)
            for key in KEYS:
                try:
                    versions = b.list_versions(key)
                except ObjectNotFound:
                    continue
                nums = [v.version for v in versions]
                assert nums == sorted(set(nums), reverse=True)
            with b._locked_conn() as conn:
                assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
                rows = list(b._blob_rows(conn))
            for row in rows:
                problem = b._check_blob(row)
                assert problem is None, f"round {rnd}: blob {row.hash.hex()} is damaged: {problem}"
            row_hashes = {row.hash.hex() for row in rows}
            orphans = {h for h, _ in iter_blob_files(b.path)} - row_hashes
            assert orphans <= in_flight_hashes, f"round {rnd}: unexplained blob files {orphans - in_flight_hashes}"
            for tmp in iter_tmp_files(b.path):
                assert tmp_is_stale(tmp, min_age=0), f"round {rnd}: {tmp.name} is still locked after the kill"

            # Every few rounds, clean up (when gc exists) and check the bucket again.
            if maintenance is not None and rnd % 5 == 4:
                # Expire all trash too (now = 8 days ahead), so long runs stay small.
                future = int(time.time() * 1000) + 8 * 86_400_000
                gc_report = maintenance.gc(b, now=future, orphan_grace=0)
                assert not gc_report["errors"], gc_report
                report = maintenance.verify(b)
                assert report["ok"], report
                with b._locked_conn() as conn:
                    assert conn.execute("SELECT count(*) FROM trash").fetchone()[0] == 0
                    row_hashes = {row.hash.hex() for row in b._blob_rows(conn)}
                left = {h for h, _ in iter_blob_files(b.path)} - row_hashes
                assert not left, f"gc left orphan files {left}"
                in_flight_hashes.clear()
                assert _state(b) == expected
                _check_readable(b)

            # 3. The store still accepts writes.
            probe = os.urandom(200_000)
            assert b.put(f"probe/{rnd}", probe).status == "created"
            assert b.get(f"probe/{rnd}") == probe
            b.delete(f"probe/{rnd}")

    print("crash rounds:", rounds, totals)
    assert totals["acked"] > rounds, "the child hardly ever got to acknowledge a write"


def _apply(expected: dict, buckets: set, entry: list) -> None:
    kind = entry[2]
    if kind in ("put", "delete", "batch"):
        for key, etag in entry[3]:
            expected[key] = etag
    elif kind == "mb":
        buckets.add(entry[3])
    elif kind == "rb":
        buckets.discard(entry[3])


# -- in-process failures in the middle of a write ------------------------------------


def _failing_stream(mib: int, exc: BaseException):
    rng = random.Random(mib)
    for _ in range(mib):
        yield rng.randbytes(1 << 20)
    raise exc


@pytest.mark.parametrize("mib", [1, 6])
@pytest.mark.parametrize("exc", [RuntimeError("source failed"), KeyboardInterrupt()])
@pytest.mark.parametrize("in_batch", [False, True])
def test_source_failing_mid_stream_leaves_nothing(bucket, mib, exc, in_batch):
    """A data source that fails (or is interrupted) part-way through a put leaves
    the previous version, no temp file, no orphan blob file and no new row."""
    old = os.urandom(300_000)
    bucket.put("k", old)
    rows_before = len(list(bucket._blob_rows(bucket._connection())))
    with pytest.raises(type(exc)):
        if in_batch:
            with bucket.batch() as tx:
                tx.put("other", b"would be applied with the batch")
                tx.put("k", _failing_stream(mib, exc))
        else:
            bucket.put("k", _failing_stream(mib, exc))
    assert bucket.get("k") == old
    assert not bucket.exists("other")
    assert list(iter_tmp_files(bucket.path)) == []
    assert {h for h, _ in iter_blob_files(bucket.path)} == {sha(old)}
    assert len(list(bucket._blob_rows(bucket._connection()))) == rows_before
    bucket.put("k", b"still writable")
    assert bucket.get("k") == b"still writable"
