"""mybucket sync: incremental, verified, safe to interrupt, local and over the protocol."""

from __future__ import annotations

import json
import os
import random
import signal
import subprocess
import sys
import time

import pytest

from mybucket import DamagedError, InvalidArgument, open_store, sync
from mybucket.maintenance import verify_store
from mybucket.remote import connect

from conftest import popen_python, run_python
from remote_helpers import child_env, install_fake_ssh, serve_argv


def fill(store, seed: int = 1, n: int = 120) -> dict:
    """A bucket mix: small JSON, Unicode keys, metadata, a large streamed object."""
    rng = random.Random(seed)
    b = store.bucket("archive", create=True)
    expected = {}
    with b.batch() as tx:
        for i in range(n):
            key = f"days/{i:04d}.json" if i % 7 else f"photos/trip {i}/café {i}.jpg"
            data = json.dumps({"i": i, "v": rng.random()}).encode()
            tx.put(key, data, content_type="application/json", metadata={"n": str(i)})
            expected[key] = data
    big = os.urandom(2 << 20) + bytes(6 << 20)
    b.put("media/big.bin", big)
    expected["media/big.bin"] = big
    store.bucket("second", create=True).put("only", b"one object")
    return expected


def assert_same(src, dst, buckets=("archive", "second")):
    for name in buckets:
        a, b = src.bucket(name), dst.bucket(name)
        left = {o.key: (o.etag, o.content_type, o.metadata) for o in a.list().objects}
        right = {o.key: (o.etag, o.content_type, o.metadata) for o in b.list().objects}
        assert left == right, name
        for key in left:
            assert a.get(key) == b.get(key)


def test_local_to_local_is_incremental(tmp_path):
    src = open_store(tmp_path / "src")
    fill(src)
    dst = open_store(tmp_path / "dst")
    first = sync(src, dst)
    assert first["ok"] and first["copied"] == 122 and first["updated"] == 0
    assert_same(src, dst)
    again = sync(src, dst)
    assert again["copied"] == again["updated"] == 0 and again["unchanged"] == 122 and again["bytes_copied"] == 0
    # Changes: new content, new metadata only, a delete, a new key.
    b = src.bucket("archive")
    b.put("days/0001.json", b"{}", content_type="application/json", metadata={"n": "1"})
    b.put("days/0002.json", b.get("days/0002.json"), content_type="application/json", metadata={"n": "changed"})
    b.delete("days/0003.json")
    b.put("days/new.json", b"[]")
    third = sync(src, dst)
    assert (third["copied"], third["updated"], third["deleted"]) == (1, 2, 0)
    assert third["buckets"][0]["extra_in_destination"] == 1 and dst.bucket("archive").exists("days/0003.json")
    fourth = sync(src, dst, delete=True)
    assert fourth["deleted"] == 1 and not dst.bucket("archive").exists("days/0003.json")
    assert dst.bucket("archive").list_versions("days/0003.json")[0].reason == "deleted"  # in the trash
    assert_same(src, dst)
    assert verify_store(dst)["ok"]


def test_new_destination_bucket_gets_the_source_settings(tmp_path):
    src = open_store(tmp_path / "src")
    src.create_bucket("keep", trash_days=30).put("k", b"v")
    dst = open_store(tmp_path / "dst")
    report = sync(src, dst)
    assert report["buckets"][0]["created_bucket"] and dst.bucket("keep").trash_days == 30


def test_dry_run_changes_nothing(tmp_path):
    src = open_store(tmp_path / "src")
    fill(src, n=20)
    dst = open_store(tmp_path / "dst")
    report = sync(src, dst, dry_run=True)
    assert report["dry_run"] and report["copied"] == 22 and dst.list_buckets() == []
    assert "Would copy" in report["summary"]


def test_prefix_and_bucket_selection(tmp_path):
    src = open_store(tmp_path / "src")
    fill(src, n=30)
    dst = open_store(tmp_path / "dst")
    report = sync(src, dst, buckets=["archive"], prefix="photos/")
    assert dst.list_buckets() == ["archive"]
    assert all(k.startswith("photos/") for k in dst.bucket("archive").list().keys)
    assert report["copied"] == len(dst.bucket("archive").list().keys) > 0


def test_same_store_is_refused(tmp_path):
    src = open_store(tmp_path / "src")
    with pytest.raises(InvalidArgument):
        sync(src, open_store(tmp_path / "src"))


def test_damaged_source_object_is_reported_and_never_copied(tmp_path):
    src = open_store(tmp_path / "src")
    b = src.bucket("archive", create=True)
    info = b.put("photo.jpg", os.urandom(300_000))
    b.put("fine.txt", b"fine")
    blob = b.path / "blobs" / info.etag[:2] / info.etag[2:4] / info.etag
    raw = bytearray(blob.read_bytes())
    raw[5] ^= 0xFF
    blob.write_bytes(bytes(raw))
    dst = open_store(tmp_path / "dst")
    report = sync(src, dst)
    assert not report["ok"] and report["buckets"][0]["damaged"][0]["key"] == "photo.jpg"
    assert dst.bucket("archive").list().keys == ["fine.txt"]
    # A large damaged object is streamed: the destination refuses it at the end.
    big = b.put("big.bin", os.urandom(6 << 20))
    blob = b.path / "blobs" / big.etag[:2] / big.etag[2:4] / big.etag
    raw = bytearray(blob.read_bytes())
    raw[-10] ^= 0xFF
    blob.write_bytes(bytes(raw))
    report = sync(src, dst)
    assert {d["key"] for d in report["buckets"][0]["damaged"]} == {"photo.jpg", "big.bin"}
    assert not dst.bucket("archive").exists("big.bin")
    assert verify_store(dst)["ok"]


def test_destination_changed_meanwhile_is_a_conflict_not_an_overwrite(tmp_path, monkeypatch):
    src = open_store(tmp_path / "src")
    src.bucket("archive", create=True).put("k", b"source v2")
    src.bucket("archive").put("other", b"o")
    dst = open_store(tmp_path / "dst")
    dst.bucket("archive", create=True).put("k", b"old")
    from mybucket import replication as sync_mod

    real = sync_mod._BucketSync.flush

    def flush_after_someone_writes(self):
        if self.pending:
            dst.bucket("archive").put("k", b"written by someone else")
        real(self)

    monkeypatch.setattr(sync_mod._BucketSync, "flush", flush_after_someone_writes)
    report = sync(src, dst)
    assert not report["ok"] and report["buckets"][0]["conflicts"][0]["key"] == "k"
    assert dst.bucket("archive").get("k") == b"written by someone else"
    assert dst.bucket("archive").get("other") == b"o"  # the rest of the batch still applied


def test_sync_over_the_protocol_both_directions(tmp_path):
    src = open_store(tmp_path / "src")
    fill(src, n=60)
    remote = connect(serve_argv(tmp_path / "remote", "--write-all"))
    try:
        report = sync(src, remote)
        assert report["ok"] and report["copied"] == 62
        assert sync(src, remote)["copied"] == 0
        back = open_store(tmp_path / "back")
        report = sync(remote, back)
        assert report["ok"] and report["copied"] == 62
        assert_same(src, back)
    finally:
        remote.close()
    with open_store(tmp_path / "remote", readonly=True) as r:
        assert verify_store(r)["ok"]


def test_sync_into_a_prefix_scoped_remote(tmp_path):
    src = open_store(tmp_path / "src")
    fill(src, n=30)
    with open_store(tmp_path / "remote") as r:
        r.create_bucket("archive")
    remote = connect(serve_argv(tmp_path / "remote", "--write", "archive/photos/"))
    try:
        report = sync(src, remote, buckets=["archive"], prefix="photos/")
        assert report["ok"] and report["copied"] > 0
        from mybucket import AccessDenied

        with pytest.raises(AccessDenied):
            sync(src, remote, buckets=["archive"])  # the whole bucket is outside the profile
    finally:
        remote.close()


# -- interruptions --------------------------------------------------------------------------------

CRASH_SYNC = """
import os
from mybucket import open_store, sync
from mybucket import replication as sync_mod
from mybucket import bucket as bucket_mod
from mybucket.remote import connect
count = {{"n": 0}}
def at(point):
    if point == {where!r}:
        count["n"] += 1
        if count["n"] == {after}:
            os._exit(9)  # no cleanup at all, like a power cut for this process
sync_mod._hook = lambda name: at("sync")
bucket_mod._hook = at
src = open_store({src!r}, readonly=True)
dst = connect({remote!r}) if {remote!r} else open_store({dst!r})
sync(src, dst)
print("finished")
"""


@pytest.mark.parametrize(
    "where, after, remote",
    [("sync", 1, False), ("sync", 3, False), ("before_commit", 2, False), ("after_install", 1, False), ("sync", 2, True)],
)
def test_interrupted_sync_leaves_a_consistent_destination_and_resumes(tmp_path, where, after, remote):
    src = open_store(tmp_path / "src")
    fill(src, n=1200)  # several batches, plus a streamed object
    src.close()
    dst_path = tmp_path / "dst"
    argv = serve_argv(dst_path, "--write-all") if remote else None
    proc = run_python(
        CRASH_SYNC.format(after=after, where=where, src=str(tmp_path / "src"), dst=str(dst_path), remote=argv), check=False, timeout=300
    )
    assert proc.returncode == 9, proc.stdout + proc.stderr
    src = open_store(tmp_path / "src", readonly=True)
    dst = open_store(dst_path)
    assert verify_store(dst)["ok"]
    # Whatever arrived is exactly the source's version of it: never a partial or wrong object.
    for name in dst.list_buckets():
        for info in dst.bucket(name).list().objects:
            assert info.etag == src.bucket(name).head(info.key).etag
            assert dst.bucket(name).get(info.key) == src.bucket(name).get(info.key)
    report = sync(src, dst)
    assert report["ok"]
    assert_same(src, dst)
    assert sync(src, dst)["copied"] == 0


def test_sigkill_at_random_moments_during_sync(tmp_path):
    """Kill -9 at random moments, with new work each round; the destination only ever holds
    versions the source really had, and verifies clean every time."""
    import hashlib

    src = open_store(tmp_path / "src")
    seen: dict[str, set[str]] = {}
    for key, data in fill(src, n=1500).items():
        seen.setdefault(key, set()).add(hashlib.sha256(data).hexdigest())
    rng = random.Random(7)
    code = f"""
from mybucket import open_store, sync
sync(open_store({str(tmp_path / 'src')!r}, readonly=True), open_store({str(tmp_path / 'dst')!r}))
print("done", flush=True)
"""
    interrupted = 0
    for _round in range(6):
        b = src.bucket("archive")
        keys = rng.sample(sorted(seen), 400)
        with b.batch() as tx:
            for key in keys:
                data = os.urandom(rng.choice([50, 5000, 60_000]))
                tx.put(key, data)
                seen[key].add(hashlib.sha256(data).hexdigest())
        for i in range(3):
            data = os.urandom(5 << 20)
            b.put(f"media/round-{_round}-{i}.bin", data)
            seen[f"media/round-{_round}-{i}.bin"] = {hashlib.sha256(data).hexdigest()}
        proc = popen_python(code, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(rng.uniform(0.25, 0.9))
        proc.send_signal(signal.SIGKILL)
        out, _ = proc.communicate()
        interrupted += "done" not in out
        with open_store(tmp_path / "dst") as dst:
            assert verify_store(dst)["ok"]
            for name in dst.list_buckets():
                for info in dst.bucket(name).list().objects:
                    if name == "archive":
                        assert info.etag in seen[info.key], info.key
    src.close()
    assert interrupted >= 3  # the kills really landed in the middle of a sync
    with open_store(tmp_path / "src", readonly=True) as s, open_store(tmp_path / "dst") as d:
        assert sync(s, d)["ok"]
        assert_same(s, d)


# -- the command ----------------------------------------------------------------------------------


def test_sync_command(tmp_path, monkeypatch):
    src = open_store(tmp_path / "src")
    fill(src, n=20)
    src.close()
    bindir = tmp_path / "bin"
    install_fake_ssh(bindir)
    env = child_env()
    env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}"

    def run(*args, code=0):
        proc = subprocess.run([sys.executable, "-m", "mybucket", *args], capture_output=True, env=env, timeout=300)
        assert proc.returncode == code, (proc.stdout, proc.stderr)
        return proc

    url = f"ssh://backup-host{tmp_path / 'backup'}"
    doc = json.loads(run("--json", "sync", str(tmp_path / "src"), url).stdout)
    assert doc["ok"] and doc["copied"] == 22
    human = run("sync", str(tmp_path / "src"), url).stdout.decode()
    assert "already up to date" in human and "archive: 0 new" in human
    assert "Would copy 0 objects" in run("sync", "--dry-run", str(tmp_path / "src"), url).stdout.decode()
    with open_store(tmp_path / "src") as s:
        b = s.bucket("archive")
        info = b.put("damaged.bin", os.urandom(200_000))
        blob = b.path / "blobs" / info.etag[:2] / info.etag[2:4] / info.etag
        raw = bytearray(blob.read_bytes())
        raw[0] ^= 1
        blob.write_bytes(bytes(raw))
    doc = json.loads(run("--json", "sync", str(tmp_path / "src"), url, code=1).stdout)
    assert doc["error"]["code"] == "sync_incomplete" and doc["buckets"][0]["damaged"][0]["key"] == "damaged.bin"
    proc = run("sync", str(tmp_path / "src"), url, code=1)
    assert b"not copied damaged.bin" in proc.stdout


def test_damaged_error_type_is_kept_for_remote_sources(tmp_path):
    """A damaged object read through the protocol is skipped like a local one."""
    path = tmp_path / "src"
    with open_store(path) as s:
        info = s.bucket("archive", create=True).put("bad", os.urandom(200_000))
        s.bucket("archive").put("ok", b"ok")
    blob = path / "buckets/archive/blobs" / info.etag[:2] / info.etag[2:4] / info.etag
    raw = bytearray(blob.read_bytes())
    raw[7] ^= 1
    blob.write_bytes(bytes(raw))
    remote = connect(serve_argv(path, "--read-all"), readonly=True)
    try:
        with pytest.raises(DamagedError):
            remote.bucket("archive").get("bad")
        dst = open_store(tmp_path / "dst")
        report = sync(remote, dst)
        assert report["buckets"][0]["damaged"][0]["key"] == "bad" and dst.bucket("archive").list().keys == ["ok"]
    finally:
        remote.close()
