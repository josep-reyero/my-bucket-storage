"""Engine tests: storage format, dedup, conditional writes, batches, trash, listing,
read-only mode, concurrency and crash safety."""

from __future__ import annotations

import hashlib
import io
import os
import random
import shutil
import signal
import sqlite3
import subprocess
import time
import tracemalloc
from pathlib import Path

import pytest
from conftest import popen_python, run_python
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import mybucket
from mybucket import _zstd, open_store
from mybucket import bucket as bucket_mod
from mybucket.blobs import blob_path, iter_blob_files, iter_tmp_files, tmp_is_stale
from mybucket.codec import INLINE_MAX, SMALL_MAX, BlobRow
from mybucket.errors import (
    BucketExists,
    BucketNotEmpty,
    BucketNotFound,
    Busy,
    DamagedError,
    InvalidArgument,
    InvalidBucketName,
    InvalidKey,
    NotAStore,
    ObjectNotFound,
    PreconditionFailed,
    ReadOnlyError,
    StoreNotFound,
    UnsupportedFormat,
)
from mybucket.schema import UNREFERENCED_BLOBS_SQL

DAY = 86_400_000


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def text_bytes(n: int, seed: int = 0) -> bytes:
    """Compressible, non-trivial bytes of length n."""
    rng = random.Random(seed)
    words = [b"alpha", b"beta", b"gamma", b"delta", b"heart_rate", b"steps", b"sleep", b"2026-11-14"]
    out = bytearray()
    while len(out) < n:
        out += b'{"%s": %d, "%s": "%s"}\n' % (rng.choice(words), rng.randrange(10**6), rng.choice(words), rng.choice(words))
    return bytes(out[:n])


def blob_rows(b) -> list[tuple]:
    with b._locked_conn() as conn:
        return conn.execute("SELECT hash, size, stored_size, codec, dict_id, data IS NULL FROM blobs").fetchall()


def sql(b, query: str, *params):
    with b._locked_conn() as conn:
        return conn.execute(query, params).fetchall()


# -- store and buckets -------------------------------------------------------


def test_store_layout_and_bucket_lifecycle(store, store_path):
    assert (store_path / "mybucket-store.json").is_file()
    assert store.list_buckets() == []
    b = store.create_bucket("photos")
    assert store.list_buckets() == ["photos"]
    assert (store_path / "buckets/photos/bucket.sqlite").is_file()
    assert (store_path / "buckets/photos/blobs").is_dir()
    with pytest.raises(BucketExists) as err:
        store.create_bucket("photos")
    assert err.value.exit_code == 5
    assert store.bucket("photos") is b
    with pytest.raises(BucketNotFound) as err:
        store.bucket("nope")
    assert err.value.exit_code == 3
    with pytest.raises(InvalidBucketName):
        store.bucket("Bad_Name")
    with pytest.raises(InvalidBucketName):
        store.create_bucket("../escape")

    b.put("a", b"1")
    with pytest.raises(BucketNotEmpty):
        store.delete_bucket("photos")
    b2 = store.bucket("photos")
    b2.delete("a")
    with pytest.raises(BucketNotEmpty):  # the trash still holds a version
        store.delete_bucket("photos")
    store.delete_bucket("photos", force=True)
    assert store.list_buckets() == []
    assert not (store_path / "buckets/photos").exists()
    assert [p.name for p in (store_path / "buckets").iterdir()] == []


def test_bucket_create_flag_and_reopen(store_path, clock):
    with open_store(store_path, clock=clock) as s:
        s.bucket("logs", create=True).put("k", b"v")
    with open_store(store_path) as s:
        assert s.bucket("logs").get("k") == b"v"


def test_refuses_non_store_directories(tmp_path):
    (tmp_path / "other").mkdir()
    (tmp_path / "other" / "file.txt").write_text("hello")
    with pytest.raises(NotAStore):
        open_store(tmp_path / "other")
    (tmp_path / "afile").write_text("x")
    with pytest.raises(NotAStore):
        open_store(tmp_path / "afile")
    with pytest.raises(StoreNotFound):
        open_store(tmp_path / "missing", readonly=True)
    with pytest.raises(StoreNotFound):
        open_store(tmp_path / "missing2", create=False)
    (tmp_path / "empty").mkdir()
    open_store(tmp_path / "empty").close()  # an empty directory becomes a store
    assert (tmp_path / "empty" / "mybucket-store.json").exists()


def test_database_identity_and_pragmas(bucket):
    with bucket._locked_conn() as conn:
        assert conn.execute("PRAGMA application_id").fetchone()[0] == 0x6D79626B
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
        assert conn.execute("PRAGMA auto_vacuum").fetchone()[0] == 2  # INCREMENTAL
    assert oct(bucket.db_path.stat().st_mode & 0o777) == "0o600"


def test_newer_format_is_refused(store, store_path):
    store.create_bucket("future")
    store.close()
    conn = sqlite3.connect(store_path / "buckets/future/bucket.sqlite")
    conn.execute("UPDATE meta SET v = 3 WHERE k = 'format'")
    conn.commit()
    conn.close()
    with open_store(store_path) as s, pytest.raises(UnsupportedFormat):
        s.bucket("future")


# -- put / get / head / exists ---------------------------------------------


def test_put_get_roundtrip_small(bucket, clock):
    data = b'{"steps": 12345}'
    info = bucket.put("days/2026-11-14.json", data, content_type="application/json", metadata={"src": "tracker"})
    assert info.status == "created"
    assert info.etag == sha(data)
    assert info.size == len(data)
    assert info.content_type == "application/json"
    assert info.metadata == {"src": "tracker"}
    assert info.modified == clock.now
    assert info.version == 1
    assert bucket.get("days/2026-11-14.json") == data
    head = bucket.head("days/2026-11-14.json")
    assert head.etag == info.etag and head.metadata == {"src": "tracker"} and head.version == 1
    assert bucket.exists("days/2026-11-14.json")
    assert not bucket.exists("days/missing.json")
    with pytest.raises(ObjectNotFound) as err:
        bucket.get("days/missing.json")
    assert err.value.exit_code == 3
    with pytest.raises(ObjectNotFound):
        bucket.head("days/missing.json")


def test_empty_object(bucket):
    info = bucket.put("empty", b"")
    assert info.size == 0 and info.etag == sha(b"")
    assert bucket.get("empty") == b""
    assert blob_rows(bucket) == [(hashlib.sha256(b"").digest(), 0, 0, 0, None, 0)]
    with bucket.open("empty") as f:
        assert f.read() == b""


def test_invalid_inputs_are_rejected_before_writing(bucket):
    with pytest.raises(InvalidKey):
        bucket.put("../etc/passwd", b"x")
    with pytest.raises(InvalidKey):
        bucket.get(".state/token")
    with pytest.raises(InvalidArgument):
        bucket.put("k", "a str is ambiguous")
    with pytest.raises(InvalidArgument):
        bucket.put("k", 12345)
    with pytest.raises(InvalidArgument):
        bucket.put("k", iter(["text chunk"]))
    with pytest.raises(InvalidArgument):
        bucket.put("k", io.StringIO("text file"))
    with pytest.raises(InvalidArgument):
        bucket.put("k", b"x", if_none_match="etag")
    with pytest.raises(InvalidArgument):
        bucket.put("k", b"x", if_match="not-an-etag")
    assert bucket.list().objects == []
    assert list(iter_tmp_files(bucket.path)) == []


def test_put_accepts_bytes_like_files_and_iterators(bucket, tmp_path):
    data = text_bytes(50_000)
    path = tmp_path / "in.bin"
    path.write_bytes(data)
    with open(path, "rb") as f:
        assert bucket.put("from-file", f).etag == sha(data)
    assert bucket.put("from-iter", iter([data[:10], b"", data[10:]])).etag == sha(data)
    assert bucket.put("from-bytearray", bytearray(data)).etag == sha(data)
    assert bucket.put("from-memoryview", memoryview(data)).etag == sha(data)
    for k in ("from-file", "from-iter", "from-bytearray", "from-memoryview"):
        assert bucket.get(k) == data
    assert len(blob_rows(bucket)) == 1  # all four share one blob


def test_bytearray_is_copied(bucket):
    buf = bytearray(b"original")
    with bucket.batch() as tx:
        tx.put("k", buf)
        buf[:] = b"mutated!"
    assert bucket.get("k") == b"original"


# -- compression and placement ----------------------------------------------


def test_small_compressible_is_zstd_inline(bucket):
    data = text_bytes(20_000)
    info = bucket.put("t.json", data)
    [(h, size, stored, codec, dict_id, in_file)] = blob_rows(bucket)
    assert codec == 1 and dict_id is None and not in_file
    assert stored < size * 0.5 and stored == info.stored_size
    # the inline bytes are a plain zstd frame
    [(raw,)] = sql(bucket, "SELECT data FROM blobs")
    assert _zstd.Decompressor().decompress(raw) == data


def test_incompressible_is_stored_raw(bucket):
    data = os.urandom(50_000)
    bucket.put("r.bin", data)
    [(_, size, stored, codec, _, in_file)] = blob_rows(bucket)
    assert codec == 0 and stored == size == 50_000 and not in_file
    [(raw,)] = sql(bucket, "SELECT data FROM blobs")
    assert raw == data


def test_large_raw_goes_to_a_file(bucket):
    data = os.urandom(INLINE_MAX + 1)
    info = bucket.put("big.bin", data)
    [(h, _, stored, codec, _, in_file)] = blob_rows(bucket)
    assert codec == 0 and in_file and stored == len(data)
    path = blob_path(bucket.path, info.etag)
    assert path.read_bytes() == data
    assert path.parent.name == info.etag[2:4] and path.parent.parent.name == info.etag[0:2]
    assert bucket.get("big.bin") == data
    assert list(iter_tmp_files(bucket.path)) == []


def test_inline_threshold_is_on_stored_size(bucket):
    exactly = os.urandom(INLINE_MAX)
    bucket.put("exact", exactly)
    assert sql(bucket, "SELECT data IS NULL FROM blobs") == [(0,)]
    compressible = text_bytes(3 * INLINE_MAX)  # bigger than 128 KiB raw, small compressed
    bucket.put("packed", compressible)
    rows = {r[0]: r for r in blob_rows(bucket)}
    row = rows[hashlib.sha256(compressible).digest()]
    assert row[3] == 1 and row[5] == 0  # zstd, inline


def test_streamed_large_object(bucket):
    """> 4 MiB through an iterator: level-9 zstd frames (codec 2) into a blob file, verified on read."""
    chunk = text_bytes(1 << 20, seed=1)
    total = 6
    info = bucket.put("stream.ndjson", (chunk for _ in range(total)), content_type="application/x-ndjson")
    expected = chunk * total
    assert info.size == len(expected) and info.etag == sha(expected)
    [(_, size, stored, codec, _, in_file)] = blob_rows(bucket)
    assert codec == 2 and size == len(expected)
    assert stored < size / 3
    with bucket.open("stream.ndjson") as f:
        assert f.info.etag == info.etag
        got = b"".join(iter(lambda: f.read(100_000), b""))
    assert got == expected
    # The blob file decompresses with zstd alone.
    if in_file:
        packed = blob_path(bucket.path, info.etag).read_bytes()
        assert _decompress_frames(packed) == expected


def test_streamed_incompressible_becomes_raw_file(bucket):
    rng = random.Random(5)
    parts = [rng.randbytes(1 << 20) for _ in range(5)]
    data = b"".join(parts)
    info = bucket.put("noise.bin", iter(parts))
    [(_, size, stored, codec, _, in_file)] = blob_rows(bucket)
    assert codec == 0 and in_file and stored == size == len(data)
    assert blob_path(bucket.path, info.etag).read_bytes() == data
    assert list(iter_tmp_files(bucket.path)) == []


def test_streamed_highly_compressible_becomes_inline(bucket):
    info = bucket.put("zeros", (b"\0" * (1 << 20) for _ in range(8)))
    [(_, size, stored, codec, _, in_file)] = blob_rows(bucket)
    assert size == 8 << 20 and codec == 2 and not in_file and stored < 4096
    assert bucket.get("zeros") == b"\0" * (8 << 20)
    assert info.stored_size == stored


def test_bytes_over_4mib_use_the_stream_path(bucket):
    data = text_bytes(SMALL_MAX + 10, seed=3)
    info = bucket.put("over", data)
    assert info.etag == sha(data)
    assert bucket.get("over") == data


def test_streaming_put_and_get_use_bounded_memory(bucket):
    chunk = text_bytes(1 << 20, seed=9)
    n = 48  # 48 MiB object
    tracemalloc.start()
    try:
        bucket.put("huge", (chunk for _ in range(n)))
        _, peak_put = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        h = hashlib.sha256()
        with bucket.open("huge") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                h.update(block)
        _, peak_get = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert h.hexdigest() == sha(chunk * n)
    assert peak_put < 24 << 20, peak_put
    assert peak_get < 8 << 20, peak_get


# -- dedup and reference tracking -------------------------------------------


def test_identical_put_is_unchanged(bucket, clock):
    first = bucket.put("k", b"same", content_type="text/plain", metadata={"a": "1"})
    clock.advance(seconds=10)
    again = bucket.put("k", b"same", content_type="text/plain", metadata={"a": "1"})
    assert again.status == "unchanged" and again.unchanged
    assert again.version == first.version and again.modified == first.modified
    assert len(bucket.list_versions("k")) == 1  # nothing went to the trash
    assert bucket.stats().trash_versions == 0


def test_metadata_or_content_type_change_is_a_new_version(bucket):
    v1 = bucket.put("k", b"same", content_type="text/plain")
    v2 = bucket.put("k", b"same", content_type="text/plain", metadata={"x": "y"})
    v3 = bucket.put("k", b"same", content_type="text/csv", metadata={"x": "y"})
    v4 = bucket.put("k", b"same")
    assert [v.status for v in (v1, v2, v3, v4)] == ["created", "updated", "updated", "updated"]
    assert v1.version < v2.version < v3.version < v4.version
    assert len(blob_rows(bucket)) == 1


def test_dedup_across_keys_and_references(bucket):
    data = text_bytes(300_000, seed=4)
    bucket.put("a", data)
    bucket.put("b", data)
    bucket.put("c/d", data)
    assert len(blob_rows(bucket)) == 1
    stats = bucket.stats()
    assert stats.objects == 3 and stats.blobs == 1 and stats.logical_bytes == 3 * len(data)
    bucket.delete("a")
    bucket.put("b", b"other")
    assert bucket.get("c/d") == data
    assert sql(bucket, UNREFERENCED_BLOBS_SQL) == []  # trash still references
    # A referenced blob row can never be deleted: foreign keys refuse it.
    with bucket._locked_conn() as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM blobs WHERE hash = ?", (hashlib.sha256(data).digest(),))


def test_unreferenced_blobs_are_exactly_the_orphans(bucket):
    bucket.configure(trash_days=0)
    keep = bucket.put("keep", b"keep me").etag
    gone = bucket.put("gone", b"drop me").etag
    shared = bucket.put("s1", b"shared").etag
    bucket.put("s2", b"shared")
    bucket.delete("gone")
    bucket.delete("s1")
    unreferenced = {h.hex() for _, h, _ in sql(bucket, UNREFERENCED_BLOBS_SQL)}
    assert unreferenced == {gone}
    assert keep not in unreferenced and shared not in unreferenced


def test_dedup_reuses_blob_even_when_first_copy_is_in_trash(bucket):
    data = os.urandom(200_000)
    bucket.put("a", data)
    bucket.delete("a")
    info = bucket.put("b", data)
    assert len(blob_rows(bucket)) == 1
    assert bucket.get("b") == data and info.etag == sha(data)
    assert len(list(iter_blob_files(bucket.path))) == 1


def test_put_of_known_content_skips_compression(bucket, monkeypatch):
    data = text_bytes(100_000)
    bucket.put("a", data)
    calls = []
    real = _zstd.compress
    monkeypatch.setattr(_zstd, "compress", lambda d, level, dictionary=None: calls.append(level) or real(d, level))
    bucket.put("b", data)
    assert calls == []
    bucket.put("c", data + b"!")
    assert calls == [19]


def test_blob_missing_row_after_precheck_is_recreated(bucket, monkeypatch):
    """If gc removes a blob between the pre-check and the write, the writer recreates it."""
    data = text_bytes(10_000)
    # Pretend the pre-check found the content stored (so nothing is prepared before the lock).
    monkeypatch.setattr(bucket, "_prepare_outside_lock", lambda staged: None)
    info = bucket.put("k", data)
    assert bucket.get("k") == data and info.stored_size > 0


# -- conditional writes ----------------------------------------------------


def test_if_none_match_star(bucket):
    bucket.put("k", b"1", if_none_match="*")
    with pytest.raises(PreconditionFailed) as err:
        bucket.put("k", b"2", if_none_match="*")
    assert err.value.exit_code == 5 and "already exists" in err.value.message
    with pytest.raises(PreconditionFailed):
        bucket.put("k", b"1", if_none_match="*")  # even identical content
    assert bucket.get("k") == b"1"


def test_if_match(bucket):
    v1 = bucket.put("k", b"1")
    with pytest.raises(PreconditionFailed):
        bucket.put("k", b"2", if_match=sha(b"other"))
    with pytest.raises(PreconditionFailed):
        bucket.put("missing", b"2", if_match=v1.etag)
    v2 = bucket.put("k", b"2", if_match=f'"{v1.etag.upper()}"')
    assert v2.status == "updated"
    bucket.put("k", b"3", if_match="*")
    with pytest.raises(PreconditionFailed):
        bucket.put("nope", b"3", if_match="*")
    assert bucket.get("k") == b"3"


def test_conditional_delete(bucket):
    v1 = bucket.put("k", b"1")
    with pytest.raises(PreconditionFailed):
        bucket.delete("k", if_match=sha(b"x"))
    assert bucket.exists("k")
    deleted = bucket.delete("k", if_match=v1.etag)
    assert deleted.etag == v1.etag
    assert not bucket.exists("k")
    with pytest.raises(ObjectNotFound):
        bucket.delete("k")


def test_conditions_are_checked_inside_the_transaction(bucket, monkeypatch):
    """A change committed between the caller's read and the write is detected."""
    v1 = bucket.put("k", b"1")
    other = open_store(bucket.store.path).bucket(bucket.name)
    real = bucket._prepare_outside_lock

    def sneak(staged):
        real(staged)
        other.put("k", b"changed by someone else")

    monkeypatch.setattr(bucket, "_prepare_outside_lock", sneak)
    with pytest.raises(PreconditionFailed):
        bucket.put("k", b"2", if_match=v1.etag)
    assert bucket.get("k") == b"changed by someone else"
    other.close()


# -- trash and versions -----------------------------------------------------


def test_overwrite_and_delete_keep_versions(bucket, clock):
    v1 = bucket.put("k", b"one")
    clock.advance(seconds=1)
    v2 = bucket.put("k", b"two")
    clock.advance(seconds=1)
    deleted_at = clock.now
    bucket.delete("k")
    versions = bucket.list_versions("k")
    assert [v.version for v in versions] == [v2.version, v1.version]
    assert [v.reason for v in versions] == ["deleted", "overwritten"]
    assert not any(v.current for v in versions)
    assert versions[0].trashed_at == deleted_at
    assert versions[0].expires_at == deleted_at + 7 * DAY
    assert versions[1].expires_at == v2.modified + 7 * DAY
    assert versions[1].etag == sha(b"one")
    # put again: current version first, then the trash
    v3 = bucket.put("k", b"three")
    versions = bucket.list_versions("k")
    assert versions[0].current and versions[0].version == v3.version
    assert [v.version for v in versions] == [v3.version, v2.version, v1.version]
    with pytest.raises(ObjectNotFound):
        bucket.list_versions("never-existed")


def test_trash_days_is_configurable(store, clock):
    b = store.create_bucket("short", trash_days=1)
    assert b.trash_days == 1
    b.put("k", b"1")
    b.put("k", b"2")
    assert bucket_trash(b)[0][1] == clock.now + DAY
    b.configure(trash_days=30)
    assert b.trash_days == 30
    b.put("k", b"3")
    assert sorted(r[1] for r in bucket_trash(b)) == [clock.now + DAY, clock.now + 30 * DAY]
    b.configure(trash_days=0)
    b.put("k", b"4")
    assert len(bucket_trash(b)) == 2  # not kept
    with pytest.raises(InvalidArgument):
        b.configure(trash_days=-1)
    with pytest.raises(InvalidArgument):
        store.create_bucket("bad-trash", trash_days=1.5)


def bucket_trash(b):
    return sql(b, "SELECT version, expires_at FROM trash ORDER BY version")


def test_internal_set_current_supports_restore(bucket):
    """What a restore needs: put a trashed version back as a new current version."""
    v1 = bucket.put("k", b"one")
    bucket.put("k", b"two")
    with bucket._write_txn("restore") as txn:
        old = bucket._trash_entry(txn.conn, "k", v1.version)
        current = bucket._current(txn.conn, "k")
        new_version = bucket._set_current(txn, "k", old.blob_id, old.content_type, old.meta, current)
    assert bucket.get("k") == b"one"
    assert bucket.head("k").version == new_version > v1.version
    reasons = [(v.version, v.reason) for v in bucket.list_versions("k")]
    assert reasons[0] == (new_version, None)
    assert {r for _, r in reasons[1:]} == {"overwritten"}


# -- listing -----------------------------------------------------------------


def _fill(bucket, keys):
    with bucket.batch() as tx:
        for k in keys:
            tx.put(k, k.encode())


def test_list_prefix_and_delimiter(bucket):
    keys = ["a.txt", "photos/2025/x.jpg", "photos/2026/y.jpg", "photos/2026/z.jpg", "photos/top.jpg", "zeta"]
    _fill(bucket, keys)
    assert bucket.list().keys == keys
    assert bucket.list(prefix="photos/").keys == keys[1:5]
    res = bucket.list(delimiter="/")
    assert res.keys == ["a.txt", "zeta"] and res.prefixes == ["photos/"]
    assert [str(e) if isinstance(e, str) else e.key for e in res] == ["a.txt", "photos/", "zeta"]
    res = bucket.list(prefix="photos/", delimiter="/")
    assert res.keys == ["photos/top.jpg"] and res.prefixes == ["photos/2025/", "photos/2026/"]
    res = bucket.list(prefix="photos/20", delimiter="/")
    assert res.keys == [] and res.prefixes == ["photos/2025/", "photos/2026/"]
    assert bucket.list(prefix="nothing/").keys == []
    assert bucket.list(prefix="photos/2026/y.jpg").keys == ["photos/2026/y.jpg"]
    info = bucket.list(prefix="a").objects[0]
    assert info.etag == sha(b"a.txt") and info.size == 5


def test_list_pagination(bucket):
    keys = [f"k{i:03d}" for i in range(25)]
    _fill(bucket, keys)
    seen, after = [], None
    while True:
        page = bucket.list(limit=10, start_after=after)
        seen += page.keys
        if not page.truncated:
            assert page.next_start_after is None
            break
        assert len(page) == 10
        after = page.next_start_after
    assert seen == keys
    page = bucket.list(limit=25)
    assert not page.truncated and len(page.keys) == 25
    assert bucket.list(start_after="k020").keys == keys[21:]
    with pytest.raises(InvalidArgument):
        bucket.list(limit=0)


def test_list_pagination_with_common_prefixes(bucket):
    keys = ["a/1", "a/2", "b", "c/1", "c/2", "c/3", "d", "e/x/y"]
    _fill(bucket, keys)
    entries, after = [], None
    while True:
        page = bucket.list(delimiter="/", limit=2, start_after=after)
        entries += sorted(page.keys + page.prefixes)
        if not page.truncated:
            break
        after = page.next_start_after
    assert entries == ["a/", "b", "c/", "d", "e/"]


def test_list_does_not_see_deleted_or_trash(bucket):
    _fill(bucket, ["x", "y"])
    bucket.delete("x")
    assert bucket.list().keys == ["y"]


_list_keys = st.lists(
    st.lists(st.sampled_from(["a", "b", "ab", "a-b", "z"]), min_size=1, max_size=4).map("/".join),
    min_size=0,
    max_size=25,
    unique=True,
)


def _reference_list(keys, prefix, delimiter):
    out = []
    for k in sorted(keys):
        if not k.startswith(prefix):
            continue
        if delimiter:
            i = k.find(delimiter, len(prefix))
            if i >= 0:
                cp = k[: i + len(delimiter)]
                if not out or out[-1] != ("p", cp):
                    out.append(("p", cp))
                continue
        out.append(("k", k))
    return out


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    keys=_list_keys,
    prefix=st.sampled_from(["", "a", "a/", "a-", "b/a", "z/"]),
    delimiter=st.sampled_from([None, "/", "-", "b/"]),
    limit=st.sampled_from([None, 1, 2, 3, 7]),
)
def test_list_matches_reference_model(tmp_path_factory, keys, prefix, delimiter, limit):
    s = open_store(tmp_path_factory.mktemp("lst") / "s")
    b = s.bucket("model", create=True)
    if keys:
        _fill(b, keys)
    got, after = [], None
    for _ in range(100):
        page = b.list(prefix=prefix, delimiter=delimiter, limit=limit, start_after=after)
        if limit is not None:
            assert len(page) <= limit
        entries = [("k", o.key) for o in page.objects] + [("p", p) for p in page.prefixes]
        got += sorted(entries, key=lambda e: e[1])
        if not page.truncated:
            break
        after = page.next_start_after
    s.close()
    assert got == _reference_list(keys, prefix, delimiter)


# -- batch -------------------------------------------------------------------


def test_batch_commits_everything_together(bucket):
    bucket.put("old", b"x")
    with bucket.batch() as tx:
        tx.put("day/a.json", b"a")
        tx.put("day/b.json", os.urandom(200_000))
        tx.delete("old")
        assert not bucket.exists("day/a.json")  # invisible until the block ends
    assert [r.key for r in tx.results] == ["day/a.json", "day/b.json", "old"]
    assert tx.results[0].status == "created"
    assert bucket.exists("day/a.json") and bucket.exists("day/b.json") and not bucket.exists("old")
    assert list(iter_tmp_files(bucket.path)) == []


def test_batch_failed_precondition_applies_nothing(bucket):
    bucket.put("exists", b"x")
    with pytest.raises(PreconditionFailed) as err:
        with bucket.batch() as tx:
            tx.put("new1", b"1")
            tx.put("big", os.urandom(300_000))
            tx.put("exists", b"y", if_none_match="*")
    assert "No operation in the batch was applied." in err.value.message
    assert bucket.list().keys == ["exists"]
    assert bucket.get("exists") == b"x"
    assert list(iter_blob_files(bucket.path)) == []  # installed file was removed again
    assert list(iter_tmp_files(bucket.path)) == []
    assert len(blob_rows(bucket)) == 1


def test_batch_exception_in_block_applies_nothing(bucket):
    with pytest.raises(RuntimeError):
        with bucket.batch() as tx:
            tx.put("a", os.urandom(300_000))
            raise RuntimeError("caller bug")
    assert bucket.list().keys == []
    assert list(iter_tmp_files(bucket.path)) == []
    assert tx.results is None


def test_batch_delete_missing_rolls_back(bucket):
    with pytest.raises(ObjectNotFound):
        with bucket.batch() as tx:
            tx.put("a", b"1")
            tx.delete("missing")
    assert not bucket.exists("a")


def test_batch_sequential_semantics_on_same_key(bucket):
    with bucket.batch() as tx:
        tx.put("k", b"1")
        tx.put("k", b"2")
        tx.put("k", b"2")
    assert [r.status for r in tx.results] == ["created", "updated", "unchanged"]
    assert bucket.get("k") == b"2"
    assert len(bucket.list_versions("k")) == 2


def test_batch_misuse(bucket):
    b = bucket.batch()
    with pytest.raises(mybucket.StoreError):
        b.put("k", b"x")
    with b:
        pass
    assert b.results == []
    with pytest.raises(mybucket.StoreError):
        with b:
            pass


# -- damage detection ------------------------------------------------------


def test_damaged_inline_blob_is_never_returned(bucket):
    bucket.put("k", text_bytes(5000))
    with bucket._locked_conn() as conn:
        (data,) = conn.execute("SELECT data FROM blobs").fetchone()
        broken = bytearray(data)
        broken[len(broken) // 2] ^= 0xFF
        conn.execute("UPDATE blobs SET data = ?", (bytes(broken),))
    with pytest.raises(DamagedError) as err:
        bucket.get("k")
    assert err.value.exit_code == 1 and "damaged" in err.value.message


def test_damaged_raw_inline_blob_is_detected(bucket):
    bucket.put("k", os.urandom(1000))
    with bucket._locked_conn() as conn:
        conn.execute("UPDATE blobs SET data = zeroblob(1000)")
    with pytest.raises(DamagedError):
        bucket.get("k")


def test_damaged_and_missing_blob_files(bucket):
    data = os.urandom(300_000)
    info = bucket.put("k", data)
    path = blob_path(bucket.path, info.etag)
    original = path.read_bytes()
    path.chmod(0o600)
    path.write_bytes(original[:-10])
    with pytest.raises(DamagedError):
        bucket.get("k")
    with bucket.open("k") as f, pytest.raises(DamagedError):
        f.read()
    path.write_bytes(original + b"extra")
    with pytest.raises(DamagedError):
        bucket.get("k")
    path.unlink()
    with pytest.raises(DamagedError):
        bucket.get("k")
    with bucket._locked_conn() as conn:
        row = BlobRow(*conn.execute("SELECT id, hash, size, stored_size, codec, dict_id, data FROM blobs").fetchone())
    assert bucket._check_blob(row) == "its blob file is missing"
    path.write_bytes(original)
    assert bucket._check_blob(row) is None
    assert bucket.get("k") == data


def test_damaged_zstd_file_is_detected(bucket):
    data = text_bytes(SMALL_MAX + 1, seed=7)  # streamed, zstd, large enough for a file
    info = bucket.put("k", data)
    path = blob_path(bucket.path, info.etag)
    assert path.exists()
    raw = bytearray(path.read_bytes())
    raw[len(raw) // 2] ^= 0x55
    path.write_bytes(bytes(raw))
    with pytest.raises(DamagedError):
        bucket.get("k")


def test_missing_dictionary_is_reported_as_damage(bucket):
    bucket.put("k", b"hello" * 100)
    with bucket._locked_conn() as conn:
        conn.execute("UPDATE blobs SET dict_id = 7")
    with pytest.raises(DamagedError) as err:
        bucket.get("k")
    assert "dictionary it was compressed with (7) is missing" in err.value.message


# -- read-only mode ---------------------------------------------------------


def test_read_only_store(store_path, clock):
    with open_store(store_path, clock=clock) as s:
        s.bucket("data", create=True).put("k", b"v")
    ro = open_store(store_path, readonly=True)
    b = ro.bucket("data")
    assert b.get("k") == b"v"
    assert b.list().keys == ["k"]
    for action in (
        lambda: b.put("k2", b"x"),
        lambda: b.delete("k"),
        lambda: b.batch(),
        lambda: b.configure(trash_days=1),
        lambda: ro.create_bucket("other"),
        lambda: ro.delete_bucket("data", force=True),
    ):
        with pytest.raises(ReadOnlyError) as err:
            action()
        assert err.value.exit_code == 4
    with pytest.raises(BucketNotFound):
        ro.bucket("missing", create=True)
    # Second layer: SQLite itself refuses writes on this connection.
    with b._locked_conn() as conn, pytest.raises(sqlite3.OperationalError):
        conn.execute("DELETE FROM objects")
    # A reader sees commits made by a writer after it opened.
    with open_store(store_path) as w:
        w.bucket("data").put("k", b"new")
    assert b.get("k") == b"new"
    ro.close()


# -- concurrency ---------------------------------------------------------------


def test_busy_bucket_raises_temporary_error(store_path):
    s = open_store(store_path, timeout=0.2)
    b = s.bucket("busy", create=True)
    blocker = sqlite3.connect(b.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        t = time.monotonic()
        with pytest.raises(Busy) as err:
            b.put("k", b"x")
        assert err.value.exit_code == 75
        assert time.monotonic() - t >= 0.15
        assert b.list().keys == []  # reads still work while another process writes
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()
    b.put("k", b"x")
    assert list(iter_tmp_files(b.path)) == []
    s.close()


WRITER = """
import sys
from mybucket import open_store, PreconditionFailed, Busy
store = open_store(sys.argv[1] if len(sys.argv) > 1 else {path!r}, timeout=30)
b = store.bucket("shared")
for i in range({n}):
    b.put("w{w}/item-%d" % i, b"payload %d from {w}" % i, if_none_match="*")
    while True:  # optimistic increment of a shared counter
        info = b.head("counter")
        value = int(b.get("counter"))
        try:
            b.put("counter", str(value + 1).encode(), if_match=info.etag)
            break
        except PreconditionFailed:
            continue
print("done")
"""


def test_concurrent_writer_processes(store, store_path):
    b = store.create_bucket("shared")
    b.put("counter", b"0")
    workers, n = 4, 15
    procs = [popen_python(WRITER.format(path=str(store_path), n=n, w=w), stdout=subprocess.PIPE, stderr=subprocess.PIPE) for w in range(workers)]
    # A reader works the whole time.
    deadline = time.time() + 120
    while any(p.poll() is None for p in procs) and time.time() < deadline:
        int(b.get("counter"))
        b.list(prefix="w0/")
    for p in procs:
        out, err = p.communicate(timeout=120)
        assert p.returncode == 0, err
        assert out.strip() == "done"
    assert int(b.get("counter")) == workers * n
    for w in range(workers):
        assert len(b.list(prefix=f"w{w}/").keys) == n
    assert len(b.list_versions("counter")) == workers * n + 1


# -- crash safety ----------------------------------------------------------


CRASH_AT = """
import os, sys
from mybucket import open_store
from mybucket import bucket as bucket_mod
store = open_store({path!r})
b = store.bucket("crash")
def hook(point):
    if point == {point!r}:
        os._exit(9)
bucket_mod._hook = hook
b.put("victim", {data!r})
print("not reached")
"""


@pytest.mark.parametrize("point", ["staged", "after_install", "before_commit"])
@pytest.mark.parametrize("size", ["small", "large"])
def test_crash_before_commit_keeps_previous_version(store, store_path, point, size):
    b = store.create_bucket("crash")
    old = os.urandom(200_000)
    b.put("victim", old)
    b.put("other", b"untouched")
    new = os.urandom(300_000) if size == "large" else b"tiny new content"
    proc = run_python(CRASH_AT.format(path=str(store_path), point=point, data=new), check=False)
    if size == "small" and point == "after_install":
        assert proc.returncode == 0  # small content is inline: no file is installed
        assert b.get("victim") == new
        return
    assert proc.returncode == 9, proc.stderr
    assert b.get("victim") == old
    assert b.get("other") == b"untouched"
    assert b.head("victim").etag == sha(old)
    rows = {h.hex() for h, *_ in blob_rows(b)}
    assert sha(new) not in rows
    files = {h for h, _ in iter_blob_files(b.path)}
    if point in ("after_install", "before_commit") and size == "large":
        assert sha(new) in files  # an orphan: a file with no row, harmless
    assert files - rows <= {sha(new)}
    # Temp files left by the dead writer are recognisably stale (no flock held).
    for tmp in iter_tmp_files(b.path):
        assert tmp_is_stale(tmp, min_age=0)
    # A later write of the same content works and is readable.
    b.put("victim", new)
    assert b.get("victim") == new


def test_exception_after_install_removes_the_file(bucket, monkeypatch):
    def boom(point):
        if point == "before_commit":
            raise RuntimeError("simulated failure")

    monkeypatch.setattr(bucket_mod, "_hook", boom)
    data = os.urandom(300_000)
    with pytest.raises(RuntimeError):
        bucket.put("k", data)
    monkeypatch.setattr(bucket_mod, "_hook", None)
    assert not bucket.exists("k")
    assert list(iter_blob_files(bucket.path)) == []
    assert blob_rows(bucket) == []
    bucket.put("k", data)
    assert bucket.get("k") == data


def test_tmp_files_in_use_are_never_stale(bucket):
    from mybucket.blobs import TempFile

    tmp = TempFile(bucket.tmp_dir)
    tmp.write(b"in progress")
    os.utime(tmp.path, (0, 0))  # looks ancient
    assert not tmp_is_stale(tmp.path, min_age=0)  # but the writer holds its flock
    tmp.close()
    assert tmp_is_stale(tmp.path, min_age=0)
    tmp.discard()


KILL_LOOP = """
import os, sys
from mybucket import open_store
store = open_store({path!r})
b = store.bucket("kill")
i = 0
while True:
    data = os.urandom(150_000 if i % 3 == 0 else 2_000) + str(i).encode()
    if i % 5 == 4:
        with b.batch() as tx:
            tx.put("batch/%d/a" % i, data)
            tx.put("batch/%d/b" % i, data[::-1])
        print("batch", i, flush=True)
    else:
        info = b.put("obj/%d" % i, data)
        print("put", i, info.etag, flush=True)
    i += 1
"""


def test_kill_minus_nine_never_loses_or_corrupts(store, store_path):
    b = store.create_bucket("kill")
    committed: dict[str, str] = {}
    batches: list[int] = []
    rng = random.Random(42)
    for _round in range(4):
        proc = popen_python(KILL_LOOP.format(path=str(store_path)), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(0.4 + rng.random() * 0.6)
        proc.send_signal(signal.SIGKILL)
        out, _ = proc.communicate(timeout=30)
        for line in out.splitlines():
            parts = line.split()
            if parts and parts[0] == "put":
                committed[f"obj/{parts[1]}"] = parts[2]
            elif parts and parts[0] == "batch":
                batches.append(int(parts[1]))
        # Everything reported as committed is there and intact.
        for key, etag in committed.items():
            assert b.head(key).etag == etag
            assert sha(b.get(key)) == etag
        for i in batches:
            a, z = b.get(f"batch/{i}/a"), b.get(f"batch/{i}/b")
            assert a == z[::-1]
        # Nothing readable is corrupt, and batches are all-or-nothing.
        listing = b.list()
        for info in listing.objects:
            assert sha(b.get(info.key)) == info.etag
        batch_keys = {k for k in listing.keys if k.startswith("batch/")}
        for k in batch_keys:
            prefix = k.rsplit("/", 1)[0]
            assert {f"{prefix}/a", f"{prefix}/b"} <= batch_keys
    assert committed, "the writer never got to commit anything"
    with b._locked_conn() as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    # Every committed file blob is complete.
    with b._locked_conn() as conn:
        rows = [BlobRow(*r) for r in conn.execute("SELECT id, hash, size, stored_size, codec, dict_id, data FROM blobs")]
    assert all(b._check_blob(r) is None for r in rows)


# -- reader retry when gc removes a file after the reader's snapshot ----------


def test_reader_retries_when_blob_file_vanishes(bucket, monkeypatch):
    data = os.urandom(300_000)
    bucket.put("k", data)
    real = bucket_mod.BlobReader
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise FileNotFoundError("simulated gc race")
        return real(*args, **kwargs)

    monkeypatch.setattr(bucket_mod, "BlobReader", flaky)
    assert bucket.get("k") == data
    assert calls["n"] == 2


# -- stats and errors ------------------------------------------------------


def test_stats(bucket):
    bucket.put("a", text_bytes(10_000))
    bucket.put("b", text_bytes(10_000))
    bucket.put("big", os.urandom(200_000))
    bucket.put("a", b"replaced")
    s = bucket.stats()
    assert s.objects == 3
    assert s.trash_versions == 1
    assert s.blobs == 3
    assert s.file_blobs == 1 and s.inline_blobs == 2
    assert s.logical_bytes == 10_000 + 200_000 + len(b"replaced")
    assert s.file_bytes == 200_000
    assert s.stored_bytes < s.blob_bytes
    assert s.database_bytes > 0
    assert s.to_dict()["bucket"] == "test-bucket"


def test_error_shape():
    err = PreconditionFailed("Object 'k' has changed.")
    d = err.to_dict()
    assert set(d["error"]) == {"code", "message", "hint"}
    assert d["error"]["code"] == "precondition_failed" and d["error"]["hint"]
    assert Busy("x").exit_code == 75 and ObjectNotFound("x").exit_code == 3


# -- FORMAT.md: data readable with sqlite3 and zstd alone ----------------------


def _decompress_frames(stored: bytes) -> bytes:
    """Every zstd frame in ``stored``, skipping skippable frames (what ``zstd -d`` does)."""
    out = []
    while stored:
        magic = int.from_bytes(stored[:4], "little")
        if magic & 0xFFFFFFF0 == 0x184D2A50:  # a skippable frame: magic, size, body
            stored = stored[8 + int.from_bytes(stored[4:8], "little") :]
            continue
        d = _zstd.Decompressor()
        out.append(d.decompress(stored))
        stored = d.unused_data
    return b"".join(out)


def test_format_readable_with_plain_sqlite_and_zstd(bucket):
    small = text_bytes(4000)
    large = text_bytes(SMALL_MAX + 100, seed=11)
    bucket.put("notes/small.json", small, content_type="application/json", metadata={"a": "b"})
    bucket.put("notes/large.ndjson", large)
    bucket.close()
    conn = sqlite3.connect(f"file:{bucket.db_path}?mode=ro", uri=True)
    q = (
        "SELECT lower(hex(b.hash)), b.size, b.codec, b.dict_id, b.data, o.content_type, o.meta "
        "FROM objects o JOIN blobs b ON b.id = o.blob_id WHERE o.key = ?"
    )
    for key, expected in (("notes/small.json", small), ("notes/large.ndjson", large)):
        hexd, size, codec, dict_id, data, ctype, meta = conn.execute(q, (key,)).fetchone()
        assert dict_id is None
        stored = data if data is not None else (bucket.path / "blobs" / hexd[:2] / hexd[2:4] / hexd).read_bytes()
        content = _decompress_frames(stored) if codec in (1, 2) else stored
        assert content == expected and len(content) == size and sha(content) == hexd
    conn.close()


@pytest.mark.skipif(not (shutil.which("sqlite3") and shutil.which("zstd")), reason="needs sqlite3 and zstd CLIs")
def test_format_worked_example_with_cli_tools(bucket, tmp_path):
    data = text_bytes(3000)
    bucket.put("tracker/day.json", data)
    big = text_bytes(SMALL_MAX + 100, seed=12)
    bucket.put("tracker/big.ndjson", big)
    bucket.close()
    db = str(bucket.db_path)
    out = tmp_path / "day.json.zst"
    subprocess.run(
        ["sqlite3", db, f"SELECT writefile('{out}', b.data) FROM objects o JOIN blobs b ON b.id = o.blob_id "
         "WHERE o.key = 'tracker/day.json';"],
        check=True, capture_output=True,
    )
    subprocess.run(["zstd", "-q", "-d", str(out), "-o", str(tmp_path / "day.json")], check=True)
    assert (tmp_path / "day.json").read_bytes() == data
    hexd = subprocess.run(
        ["sqlite3", db, "SELECT lower(hex(b.hash)) FROM objects o JOIN blobs b ON b.id = o.blob_id "
         "WHERE o.key = 'tracker/big.ndjson' AND b.data IS NULL;"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    path = Path(db).parent / "blobs" / hexd[:2] / hexd[2:4] / hexd
    restored = subprocess.run(["zstd", "-q", "-d", "-c", str(path)], check=True, capture_output=True).stdout
    assert restored == big
