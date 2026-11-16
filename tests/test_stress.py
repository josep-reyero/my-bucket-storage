"""Stress tests: a bucket compared against a plain in-memory model through long
random operation sequences, plus multi-process and multi-thread concurrency.

Scale knobs (environment variables, all optional):

    MYBUCKET_STRESS_EXAMPLES   hypothesis examples for the state machine (default 25)
    MYBUCKET_SOAK_OPS          operations in the seeded soak run (default 700)
    MYBUCKET_SOAK_SEED         seed of the soak run (default 7)
    MYBUCKET_CONCURRENCY_OPS   operations per writer process (default 60)
"""

from __future__ import annotations

import functools
import hashlib
import io
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from conftest import SRC, FakeClock
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from mybucket import keys as keys_mod
from mybucket import open_store

try:
    from mybucket import maintenance
except ImportError:  # built by another part of the project; tests using it skip
    maintenance = None
from mybucket.codec import CODEC_FRAMES, CODEC_RAW, CODEC_ZSTD, INLINE_MAX, SMALL_MAX
from mybucket.errors import (
    Busy,
    MyBucketError,
    ObjectNotFound,
    PreconditionFailed,
    ReadOnlyError,
)

DAY_MS = 86_400_000
TRASH_DAYS = 7


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


# -- content pool -------------------------------------------------------------

_HEX_TABLE = bytes(b"0123456789abcdef"[i % 16] for i in range(256))


def _random(n: int, seed: int) -> bytes:
    return random.Random(seed).randbytes(n)


def _hexish(n: int, seed: int) -> bytes:
    """Bytes that zstd shrinks to about half: stored size is easy to place either side of 128 KiB."""
    return _random(n, seed).translate(_HEX_TABLE)


def _repeat(n: int, seed: int) -> bytes:
    unit = _random(256, seed)
    return (unit * (n // 256 + 1))[:n]


def _json_lines(n: int, seed: int) -> bytes:
    rng = random.Random(seed)
    out = bytearray()
    t = 1_700_000_000
    while len(out) < n:
        t += 60
        out += b'{"t":%d,"bpm":%d,"stress":%d}\n' % (t, rng.randrange(50, 160), rng.randrange(0, 100))
    return bytes(out[:n])


#: name -> (builder, args). Built lazily and cached: the big ones cost a little.
_CONTENT_SPECS = {
    "empty": (bytes, ()),
    "one": (lambda: b"x", ()),
    "text": (lambda: b"hello world\n", ()),
    "text2": (lambda: b"hello world!\n", ()),
    "json": (_json_lines, (1500, 1)),
    "json2": (_json_lines, (1500, 2)),
    "rand1k": (_random, (1000, 3)),
    "rand_inline_max_minus_1": (_random, (INLINE_MAX - 1, 4)),
    "rand_inline_max": (_random, (INLINE_MAX, 5)),
    "rand_inline_max_plus_1": (_random, (INLINE_MAX + 1, 6)),
    "json_300k": (_json_lines, (300_000, 7)),  # zstd, stored inline
    "hex_200k": (_hexish, (200_000, 8)),  # zstd, about 100 KB stored: inline
    "hex_300k": (_hexish, (300_000, 9)),  # zstd, about 150 KB stored: file
}
_BIG_SPECS = {
    "rand_small_max": (_random, (SMALL_MAX, 10)),  # level 19 in memory, raw file
    "rand_small_max_plus_1": (_random, (SMALL_MAX + 1, 11)),  # stream path, raw file
    "json_small_max": (_json_lines, (SMALL_MAX, 12)),  # level 19, zstd file (about 500 KB)
    "json_small_max_plus_1": (_json_lines, (SMALL_MAX + 1, 13)),  # stream path, zstd file
    "repeat_small_max": (_repeat, (SMALL_MAX, 15)),  # level 19, inline
    "repeat_small_max_plus_1": (_repeat, (SMALL_MAX + 1, 16)),  # stream path, inline
    "hex_small_max_plus_1": (_hexish, (SMALL_MAX + 1, 14)),  # stream path, zstd file
}
SMALL_CONTENTS = list(_CONTENT_SPECS)
ALL_CONTENTS = SMALL_CONTENTS + list(_BIG_SPECS)


@functools.cache
def content(name: str) -> bytes:
    builder, args = _CONTENT_SPECS.get(name) or _BIG_SPECS[name]
    return builder(*args)


@functools.cache
def content_etag(name: str) -> str:
    return sha(content(name))


# Keys chosen so that many are prefixes of each other, sort next to each other,
# or share common prefixes under several delimiters.
KEYS = [
    "a",
    "a/b",
    "a/b/c",
    "a/b/c/d.json",
    "a/b-c",
    "a/b~",
    "a-b",
    "a.b",
    "a~",
    "ab",
    "A",
    "b",
    "b/a",
    "z/y/x",
    "2026/01/01/hr.json",
    "2026/01/02/hr.json",
    "/".join(["k" * 127] * 3 + ["k" * 128]),  # exactly 512 bytes
]
assert len(KEYS[-1]) == 512 and keys_mod.is_valid_key(KEYS[-1])
PREFIXES = ["", "a", "a/", "a/b", "a/b/", "a-", "A", "b", "2026/", "2026/01/0", "k", "nothing/"]
DELIMITERS = [None, "/", "-", "/b", "b"]
LIMITS = [None, 1, 2, 3, 7]
CTYPES = [None, "application/json", "application/octet-stream"]
METAS = [None, {}, {"src": "tracker"}, {"src": "tracker", "day": "2026-01-01"}, {"day": "2026-01-01", "src": "tracker"}]
CONDS = ["none", "match", "match_quoted", "match_upper", "match_wrong", "match_star", "none_match"]
DELETE_CONDS = ["none", "match", "match_wrong", "match_star"]
MODES = ["bytes", "bytearray", "memoryview", "file", "iter"]


# -- the model ------------------------------------------------------------------


@dataclass(frozen=True)
class MObj:
    cname: str
    etag: str
    size: int
    ctype: str | None
    meta: str | None
    version: int
    modified: int


@dataclass(frozen=True)
class MTrash:
    version: int
    etag: str
    size: int
    reason: str
    modified: int
    trashed_at: int


@dataclass
class MState:
    objects: dict[str, MObj] = field(default_factory=dict)
    trash: dict[str, list[MTrash]] = field(default_factory=dict)
    last_version: int = 0
    hashes: dict[str, int] = field(default_factory=dict)  # every committed content: etag -> size

    def copy(self) -> MState:
        return MState(
            dict(self.objects),
            {k: list(v) for k, v in self.trash.items()},
            self.last_version,
            dict(self.hashes),
        )


def _trash(state: MState, key: str, old: MObj, reason: str, now: int) -> None:
    state.trash.setdefault(key, []).append(MTrash(old.version, old.etag, old.size, reason, old.modified, now))


def model_put(state: MState, key, cname, ctype, meta, if_match, if_none_match, now):
    """Apply a put to the model. Returns (error class or None, status, resulting MObj)."""
    etag = content_etag(cname)
    cur = state.objects.get(key)
    if if_none_match is not None and cur is not None:
        return PreconditionFailed, None, None
    if if_match is not None and (cur is None or (if_match != "*" and if_match != cur.etag)):
        return PreconditionFailed, None, None
    if cur is not None and cur.etag == etag and cur.ctype == ctype and cur.meta == meta:
        return None, "unchanged", cur
    state.last_version += 1
    new = MObj(cname, etag, len(content(cname)), ctype, meta, state.last_version, now)
    if cur is not None:
        _trash(state, key, cur, "overwritten", now)
    state.objects[key] = new
    state.hashes[etag] = new.size
    return None, ("created" if cur is None else "updated"), new


def model_delete(state: MState, key, if_match, now):
    cur = state.objects.get(key)
    if cur is None:
        return ObjectNotFound, None
    if if_match is not None and if_match != "*" and if_match != cur.etag:
        return PreconditionFailed, None
    _trash(state, key, cur, "deleted", now)
    del state.objects[key]
    return None, cur


def reference_list(keys, prefix: str, delimiter: str | None) -> list[tuple[str, str]]:
    """S3 semantics: [("o", key) | ("p", common_prefix)] in key order."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for k in sorted(keys):
        if not k.startswith(prefix):
            continue
        if delimiter is not None:
            i = k.find(delimiter, len(prefix))
            if i >= 0:
                common = k[: i + len(delimiter)]
                if common not in seen:
                    seen.add(common)
                    out.append(("p", common))
                continue
        out.append(("o", k))
    return out


# -- input/output helpers ---------------------------------------------------------


def make_input(data: bytes, mode: str, rng: random.Random):
    if mode == "bytes":
        return data
    if mode == "bytearray":
        return bytearray(data)
    if mode == "memoryview":
        return memoryview(data)
    if mode == "file":
        return io.BytesIO(data)

    def chunks():
        pos = 0
        while pos < len(data):
            n = rng.choice([0, 1, 7, 4096, 65536, 300_000, 1 << 20])
            yield data[pos : pos + n]
            pos += n
        yield b""

    return chunks()


def read_stream(stream, rng: random.Random) -> bytes:
    parts = []
    while True:
        chunk = stream.read(rng.choice([1, 13, 4096, 100_000, -1]))
        if not chunk:
            return b"".join(parts)
        parts.append(chunk)


def collect_listing(bucket, prefix, delimiter, limit, start_after=None):
    """Page through list() and return the entries in order, checking each page."""
    entries: list[tuple[str, str]] = []
    pages = 0
    while True:
        page = bucket.list(prefix=prefix, delimiter=delimiter, limit=limit, start_after=start_after)
        pages += 1
        assert pages < 1000, "list() pagination does not terminate"
        if limit is not None:
            assert len(page) <= limit
        if page.truncated:
            assert page.next_start_after is not None
            assert limit is not None and len(page) == limit
        else:
            assert page.next_start_after is None
        for item in page:
            entries.append(("p", item) if isinstance(item, str) else ("o", item.key))
        if not page.truncated:
            return entries
        start_after = page.next_start_after


# -- the harness: one bucket plus its model, used by hypothesis and the soak run ---


class Harness:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.clock = FakeClock()
        self.store = open_store(root, clock=self.clock)
        self.bucket = self.store.create_bucket("model", trash_days=TRASH_DAYS)
        self.state = MState()
        self.rng = random.Random(0)
        self.ops = 0

    def close(self) -> None:
        self.store.close()

    def tick(self) -> int:
        self.clock.advance(seconds=1)
        self.ops += 1
        return self.clock.now

    # -- conditions -----------------------------------------------------------

    def _if_match(self, key: str, cond: str) -> str | None:
        cur = self.state.objects.get(key)
        etag = cur.etag if cur is not None else sha(b"not there %d" % self.ops)
        return {
            "match": etag,
            "match_quoted": f'"{etag}"',
            "match_upper": etag.upper(),
            "match_wrong": sha(b"wrong %d" % self.ops),
            "match_star": "*",
        }.get(cond)

    @staticmethod
    def _model_if_match(arg: str | None) -> str | None:
        return None if arg is None else keys_mod.normalize_etag(arg)

    # -- operations -------------------------------------------------------------

    def put(self, key, cname, ctype, meta, cond, mode) -> None:
        now = self.tick()
        if_match = self._if_match(key, cond)
        if_none_match = "*" if cond == "none_match" else None
        meta_json = keys_mod.validate_metadata(meta)
        err, status, expected = model_put(
            self.state, key, cname, ctype, meta_json, self._model_if_match(if_match), if_none_match, now
        )
        data = content(cname)
        try:
            info = self.bucket.put(
                key,
                make_input(data, mode, self.rng),
                content_type=ctype,
                metadata=meta,
                if_match=if_match,
                if_none_match=if_none_match,
            )
        except MyBucketError as exc:
            assert err is not None and type(exc) is err, f"put {key!r} {cond}: unexpected {exc!r}"
            return
        assert err is None, f"put {key!r} {cond}: expected {err.__name__}, got {info}"
        self._check_info(info, key, expected)
        assert info.status == status

    def delete(self, key, cond) -> None:
        now = self.tick()
        if_match = self._if_match(key, cond)
        err, expected = model_delete(self.state, key, self._model_if_match(if_match), now)
        try:
            info = self.bucket.delete(key, if_match=if_match)
        except MyBucketError as exc:
            assert err is not None and type(exc) is err, f"delete {key!r} {cond}: unexpected {exc!r}"
            return
        assert err is None, f"delete {key!r}: expected {err.__name__}"
        self._check_info(info, key, expected)

    def batch(self, ops: list[tuple], abort: bool = False) -> None:
        now = self.tick()
        draft = self.state.copy()
        expected: list = []
        err = None
        calls = []
        for op in ops:
            if op[0] == "put":
                _, key, cname, ctype, meta, cond = op
                if_match = self._if_match_in(draft, key, cond)
                if_none_match = "*" if cond == "none_match" else None
                if err is None:
                    e, status, obj = model_put(
                        draft,
                        key,
                        cname,
                        ctype,
                        keys_mod.validate_metadata(meta),
                        self._model_if_match(if_match),
                        if_none_match,
                        now,
                    )
                    err = e
                    expected.append((key, status, obj))
                calls.append(("put", key, cname, ctype, meta, if_match, if_none_match))
            else:
                _, key, cond = op
                if_match = self._if_match_in(draft, key, cond)
                if err is None:
                    e, obj = model_delete(draft, key, self._model_if_match(if_match), now)
                    err = e
                    expected.append((key, None, obj))
                calls.append(("delete", key, if_match))
        if abort:
            err = RuntimeError
        tx = self.bucket.batch()
        try:
            with tx:
                for call in calls:
                    if call[0] == "put":
                        _, key, cname, ctype, meta, if_match, if_none_match = call
                        tx.put(
                            key,
                            make_input(content(cname), self.rng.choice(MODES), self.rng),
                            content_type=ctype,
                            metadata=meta,
                            if_match=if_match,
                            if_none_match=if_none_match,
                        )
                    else:
                        tx.delete(call[1], if_match=call[2])
                if abort:
                    raise RuntimeError("caller gave up")
        except (MyBucketError, RuntimeError) as exc:
            assert err is not None and type(exc) is err, f"batch {ops}: unexpected {exc!r}"
            if isinstance(exc, MyBucketError):
                assert exc.message.endswith("No operation in the batch was applied.")
            return
        assert err is None, f"batch {ops}: expected {err.__name__}"
        self.state = draft
        assert tx.results is not None and len(tx.results) == len(expected)
        for result, (key, status, obj) in zip(tx.results, expected, strict=True):
            self._check_info(result, key, obj)
            if status is not None:
                assert result.status == status

    def _if_match_in(self, state: MState, key: str, cond: str) -> str | None:
        saved = self.state
        self.state = state
        try:
            return self._if_match(key, cond)
        finally:
            self.state = saved

    # -- checks -------------------------------------------------------------------

    def _check_info(self, info, key: str, obj: MObj) -> None:
        assert info.key == key
        assert info.etag == obj.etag
        assert info.size == obj.size
        assert info.content_type == obj.ctype
        assert info.metadata == keys_mod.parse_metadata(obj.meta)
        assert info.version == obj.version
        assert info.modified == obj.modified

    def check_key(self, key: str) -> None:
        obj = self.state.objects.get(key)
        assert self.bucket.exists(key) is (obj is not None)
        if obj is None:
            with pytest.raises(ObjectNotFound):
                self.bucket.head(key)
            with pytest.raises(ObjectNotFound):
                self.bucket.get(key)
            with pytest.raises(ObjectNotFound):
                self.bucket.open(key)
            return
        head = self.bucket.head(key)
        self._check_info(head, key, obj)
        if self.rng.random() < 0.5:
            data = self.bucket.get(key)
        else:
            with self.bucket.open(key) as stream:
                self._check_info(stream.info, key, obj)
                data = read_stream(stream, self.rng)
        assert data == content(obj.cname), f"content of {key!r} differs from the model"

    def check_versions(self, key: str) -> None:
        cur = self.state.objects.get(key)
        trash = self.state.trash.get(key, [])
        if cur is None and not trash:
            with pytest.raises(ObjectNotFound):
                self.bucket.list_versions(key)
            return
        expected = [(t.version, t.etag, t.size, False, t.reason, t.trashed_at, t.modified) for t in trash]
        if cur is not None:
            expected.append((cur.version, cur.etag, cur.size, True, None, None, cur.modified))
        expected.sort(reverse=True)
        got = self.bucket.list_versions(key)
        assert [(v.version, v.etag, v.size, v.current, v.reason, v.trashed_at, v.modified) for v in got] == expected
        for v in got:
            if not v.current:
                assert v.expires_at == v.trashed_at + TRASH_DAYS * DAY_MS

    def check_list(self, prefix, delimiter, limit) -> None:
        got = collect_listing(self.bucket, prefix, delimiter, limit)
        assert got == reference_list(self.state.objects, prefix, delimiter), (prefix, delimiter, limit)

    def check_list_start_after(self, start_after, limit) -> None:
        got = collect_listing(self.bucket, "", None, limit, start_after=start_after)
        assert got == [("o", k) for k in sorted(self.state.objects) if k > start_after]

    def check_stats(self) -> None:
        stats = self.bucket.stats()
        objects = self.state.objects
        assert stats.objects == len(objects)
        assert stats.logical_bytes == sum(o.size for o in objects.values())
        trash = [t for entries in self.state.trash.values() for t in entries]
        assert stats.trash_versions == len(trash)
        assert stats.trash_logical_bytes == sum(t.size for t in trash)
        # Dedup: each distinct content is stored exactly once (nothing is collected here).
        assert stats.blobs == len(self.state.hashes)
        assert stats.blob_bytes == sum(self.state.hashes.values())
        assert stats.inline_blobs + stats.file_blobs == stats.blobs

    def check_all(self) -> None:
        for key in sorted(set(KEYS) | set(self.state.objects)):
            self.check_key(key)
        self.check_list("", None, None)
        self.check_stats()

    def check_integrity(self) -> None:
        with self.bucket._locked_conn() as conn:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
            rows = list(self.bucket._blob_rows(conn))
        for row in rows:
            assert self.bucket._check_blob(row) is None

    def reopen(self) -> None:
        self.store.close()
        self.store = open_store(self.root, clock=self.clock)
        self.bucket = self.store.bucket("model")

    def check_read_only_view(self, key: str) -> None:
        with open_store(self.root, readonly=True) as ro:
            rb = ro.bucket("model")
            assert rb.list().keys == sorted(self.state.objects)
            obj = self.state.objects.get(key)
            if obj is not None:
                assert rb.get(key) == content(obj.cname)
            with pytest.raises(ReadOnlyError):
                rb.put(key, b"nope")
            with pytest.raises(ReadOnlyError):
                rb.delete(key)


# -- hypothesis state machine ---------------------------------------------------------

keys_st = st.sampled_from(KEYS)
small_content_st = st.sampled_from(SMALL_CONTENTS)
put_op_st = st.tuples(
    st.just("put"), keys_st, small_content_st, st.sampled_from(CTYPES), st.sampled_from(METAS), st.sampled_from(CONDS)
)
delete_op_st = st.tuples(st.just("delete"), keys_st, st.sampled_from(DELETE_CONDS))


class BucketMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.tmp = Path(tempfile.mkdtemp(prefix="mybucket-model-"))
        self.h = Harness(self.tmp / "store")

    def teardown(self) -> None:
        try:
            self.h.check_all()
            self.h.check_integrity()
        finally:
            self.h.close()
            shutil.rmtree(self.tmp, ignore_errors=True)

    @rule(
        key=keys_st,
        cname=small_content_st,
        ctype=st.sampled_from(CTYPES),
        meta=st.sampled_from(METAS),
        cond=st.sampled_from(CONDS),
        mode=st.sampled_from(MODES),
    )
    def put(self, key, cname, ctype, meta, cond, mode):
        self.h.put(key, cname, ctype, meta, cond, mode)

    @rule(key=keys_st, cname=small_content_st)
    def put_again_unchanged_or_fresh(self, key, cname):
        # Re-putting what a key already holds must be a no-op ("unchanged").
        cur = self.h.state.objects.get(key)
        if cur is not None:
            self.h.put(key, cur.cname, cur.ctype, keys_mod.parse_metadata(cur.meta) or None, "none", "iter")
        else:
            self.h.put(key, cname, None, None, "none_match", "bytes")

    @rule(key=keys_st, cond=st.sampled_from(DELETE_CONDS))
    def delete(self, key, cond):
        self.h.delete(key, cond)

    @rule(ops=st.lists(st.one_of(put_op_st, delete_op_st), min_size=1, max_size=5), abort=st.booleans())
    def batch(self, ops, abort):
        self.h.batch(ops, abort=abort and len(ops) % 2 == 0)

    @rule(key=keys_st)
    def read(self, key):
        self.h.check_key(key)

    @rule(key=keys_st)
    def versions(self, key):
        self.h.check_versions(key)

    @rule(prefix=st.sampled_from(PREFIXES), delimiter=st.sampled_from(DELIMITERS), limit=st.sampled_from(LIMITS))
    def listing(self, prefix, delimiter, limit):
        self.h.check_list(prefix, delimiter, limit)

    @rule(start_after=st.sampled_from(KEYS + PREFIXES[1:]), limit=st.sampled_from(LIMITS))
    def listing_start_after(self, start_after, limit):
        self.h.check_list_start_after(start_after, limit)

    @rule()
    def reopen(self):
        self.h.reopen()

    @rule(key=keys_st)
    def read_only_view(self, key):
        self.h.check_read_only_view(key)

    @invariant()
    def stats_match(self):
        self.h.check_stats()


BucketMachine.TestCase.settings = settings(
    max_examples=_env_int("MYBUCKET_STRESS_EXAMPLES", 25),
    stateful_step_count=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
TestBucketModel = BucketMachine.TestCase


# -- seeded soak run (includes the 4 MiB thresholds) ------------------------------------


def test_seeded_soak_against_model(tmp_path):
    """A long seeded random sequence on one bucket, with objects either side of
    the 128 KiB and 4 MiB thresholds, empty and incompressible ones."""
    seed = _env_int("MYBUCKET_SOAK_SEED", 7)
    n_ops = _env_int("MYBUCKET_SOAK_OPS", 700)
    rng = random.Random(seed)
    h = Harness(tmp_path / "store")
    h.rng = random.Random(seed + 1)
    try:
        # Weighted so big objects show up regularly but not constantly.
        def pick_content() -> str:
            return rng.choice(ALL_CONTENTS) if rng.random() < 0.25 else rng.choice(SMALL_CONTENTS)

        def random_put_op():
            return ("put", rng.choice(KEYS), pick_content(), rng.choice(CTYPES), rng.choice(METAS), rng.choice(CONDS))

        for i in range(n_ops):
            r = rng.random()
            key = rng.choice(KEYS)
            if r < 0.35:
                h.put(key, pick_content(), rng.choice(CTYPES), rng.choice(METAS), rng.choice(CONDS), rng.choice(MODES))
            elif r < 0.42:
                cur = h.state.objects.get(key)
                if cur is not None:
                    meta = keys_mod.parse_metadata(cur.meta) or None
                    h.put(key, cur.cname, cur.ctype, meta, rng.choice(["none", "match"]), rng.choice(MODES))
            elif r < 0.52:
                h.delete(key, rng.choice(DELETE_CONDS))
            elif r < 0.60:
                ops = [
                    random_put_op() if rng.random() < 0.7 else ("delete", rng.choice(KEYS), rng.choice(DELETE_CONDS))
                    for _ in range(rng.randint(1, 6))
                ]
                h.batch(ops, abort=rng.random() < 0.1)
            elif r < 0.75:
                h.check_key(key)
            elif r < 0.82:
                h.check_versions(key)
            elif r < 0.92:
                h.check_list(rng.choice(PREFIXES), rng.choice(DELIMITERS), rng.choice(LIMITS))
            elif r < 0.95:
                h.check_list_start_after(rng.choice(KEYS + PREFIXES[1:]), rng.choice(LIMITS))
            elif r < 0.97:
                h.check_stats()
            elif r < 0.985:
                h.reopen()
            else:
                h.check_read_only_view(key)
            if i % 100 == 99:
                h.check_all()
        h.check_all()
        h.check_integrity()
        for key in KEYS:
            h.check_versions(key)
        # The run must have reached the interesting sizes at least once.
        assert any(content_etag(c) in h.state.hashes for c in _BIG_SPECS), "no 4 MiB-class object was committed"
    finally:
        h.close()


# -- placement at the thresholds ----------------------------------------------------------

#: content name -> (codec, stored in a file?)
PLACEMENT = {
    "empty": (CODEC_RAW, False),
    "rand1k": (CODEC_RAW, False),
    "rand_inline_max_minus_1": (CODEC_RAW, False),
    "rand_inline_max": (CODEC_RAW, False),
    "rand_inline_max_plus_1": (CODEC_RAW, True),
    "json": (CODEC_ZSTD, False),
    "json_300k": (CODEC_ZSTD, False),
    "hex_200k": (CODEC_ZSTD, False),
    "hex_300k": (CODEC_ZSTD, True),
    "rand_small_max": (CODEC_RAW, True),
    "rand_small_max_plus_1": (CODEC_RAW, True),
    "json_small_max": (CODEC_ZSTD, True),
    "json_small_max_plus_1": (CODEC_FRAMES, True),
    "repeat_small_max": (CODEC_ZSTD, False),
    "repeat_small_max_plus_1": (CODEC_FRAMES, False),
    "hex_small_max_plus_1": (CODEC_FRAMES, True),
}


@pytest.mark.parametrize("cname", list(PLACEMENT))
@pytest.mark.parametrize("mode", ["bytes", "iter"])
def test_placement_at_thresholds(bucket, cname, mode):
    data = content(cname)
    info = bucket.put("k", make_input(data, mode, random.Random(1)))
    codec, in_file = PLACEMENT[cname]
    with bucket._locked_conn() as conn:
        row = bucket._blob_row(conn, bucket._current(conn, "k").blob_id)
    assert row.codec == codec, f"{cname}: codec {row.codec}"
    assert (row.data is None) is in_file, f"{cname}: stored_size {row.stored_size}"
    assert row.stored_size <= INLINE_MAX or in_file
    if codec == CODEC_ZSTD:
        assert row.stored_size <= 0.95 * len(data)
    assert info.stored_size == row.stored_size
    assert bucket.get("k") == data
    with bucket.open("k") as stream:
        assert read_stream(stream, random.Random(2)) == data
    # The same content again, through another input mode, is a no-op.
    again = bucket.put("k", make_input(data, "file", random.Random(3)))
    assert again.status == "unchanged"


# -- concurrency: processes -------------------------------------------------------------------


def _popen(code: str, *args: str) -> subprocess.Popen:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(code), *args],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


CHILD_COMMON = r"""
import hashlib, json, os, random, sys, time, traceback
from mybucket import open_store
from mybucket.errors import Busy, ObjectNotFound, PreconditionFailed

def payload(w, seq, size, kind):
    head = json.dumps({"w": w, "seq": seq, "size": size, "kind": kind}).encode() + b"\n"
    rng = random.Random(w * 1_000_003 + seq)
    if kind == "random":
        body = rng.randbytes(size)
    else:
        unit = rng.randbytes(64)
        body = (unit * (size // 64 + 1))[:size]
    return head + body

def check_payload(data):
    head, _, _ = data.partition(b"\n")
    m = json.loads(head)
    if data != payload(m["w"], m["seq"], m["size"], m["kind"]):
        raise AssertionError("payload does not match its own header: mixed or corrupt content")
    return m

BUSY = 0
def retry(fn, *args, **kwargs):
    global BUSY
    for attempt in range(200):
        try:
            return fn(*args, **kwargs)
        except Busy:
            BUSY += 1
            time.sleep(0.005 * (1 + attempt % 10))
    raise AssertionError("still busy after 200 attempts")
"""

WRITER = (
    CHILD_COMMON
    + r"""
path, w, n_ops, n_claims = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
rng = random.Random(1000 + w)
store = open_store(path, timeout=5)
b = store.bucket("shared")
increments = 0
won = []
own = {}
claims = list(range(n_claims))
rng.shuffle(claims)
seq = 0

def next_payload():
    global seq
    seq += 1
    r = rng.random()
    if r < 0.6:
        size = rng.choice([0, 10, 1000, 20_000])
    elif r < 0.95:
        size = rng.choice([140_000, 300_000])
    else:
        size = 5 * 1024 * 1024
    return payload(w, seq, size, rng.choice(["random", "repeat"]))

def increment():
    global increments
    while True:
        try:
            with b.open("counter") as s:
                value = int(s.read())
                etag = s.info.etag
            b.put("counter", str(value + 1).encode(), if_match=etag)
            increments += 1
            return
        except PreconditionFailed:
            continue

try:
    for i in range(n_ops):
        r = rng.random()
        if r < 0.30:
            retry(increment)
        elif r < 0.50:
            k = "shared/k%d" % rng.randrange(6)
            retry(b.put, k, next_payload())
        elif r < 0.62:
            k = "w%d/k%d" % (w, rng.randrange(5))
            data = next_payload()
            retry(b.put, k, data)
            own[k] = hashlib.sha256(data).hexdigest()
        elif r < 0.70:
            ks = ["w%d/b%d" % (w, j) for j in range(3)]
            datas = [next_payload() for _ in ks]
            def do_batch():
                with b.batch() as tx:
                    for k, d in zip(ks, datas):
                        tx.put(k, d)
            retry(do_batch)
            for k, d in zip(ks, datas):
                own[k] = hashlib.sha256(d).hexdigest()
        elif r < 0.76:
            k = "shared/k%d" % rng.randrange(6)
            try:
                retry(b.delete, k)
            except ObjectNotFound:
                pass
        elif r < 0.82:
            k = "w%d/k%d" % (w, rng.randrange(5))
            if own.get(k):
                retry(b.delete, k, if_match=own[k])
                own[k] = None
        elif claims and r < 0.92:
            j = claims.pop()
            try:
                retry(b.put, "claims/%d" % j, payload(w, 10**6 + j, 100, "random"), if_none_match="*")
                won.append(j)
            except PreconditionFailed:
                pass
        else:
            for info in retry(b.list, prefix="shared/").objects:
                try:
                    check_payload(retry(b.get, info.key))
                except ObjectNotFound:
                    pass
    for j in claims:
        try:
            retry(b.put, "claims/%d" % j, payload(w, 10**6 + j, 100, "random"), if_none_match="*")
            won.append(j)
        except PreconditionFailed:
            pass
except Exception:
    traceback.print_exc()
    sys.exit(1)
print(json.dumps({"increments": increments, "won": won, "own": own, "busy": BUSY}))
"""
)

READER = (
    CHILD_COMMON
    + r"""
path, readonly, stop_file = sys.argv[1], sys.argv[2] == "ro", sys.argv[3]
rng = random.Random(7 if readonly else 8)
store = open_store(path, readonly=readonly, timeout=5)
b = store.bucket("shared")
reads = 0
last_counter = -1
try:
    while not os.path.exists(stop_file):
        # The counter only ever grows: a reader must never see it go back.
        value = int(retry(b.get, "counter"))
        if value < last_counter:
            raise AssertionError("counter went back from %d to %d" % (last_counter, value))
        last_counter = value
        listing = retry(b.list)
        for info in listing.objects:
            if info.key == "counter" or rng.random() < 0.5:
                continue
            try:
                if rng.random() < 0.5:
                    data = retry(b.get, info.key)
                else:
                    with retry(b.open, info.key) as s:
                        parts = []
                        while True:
                            chunk = s.read(rng.choice([1000, 65536, 1 << 20]))
                            if not chunk:
                                break
                            parts.append(chunk)
                        data = b"".join(parts)
                reads += 1
                if info.key.startswith(("shared/", "claims/", "w")):
                    check_payload(data)
            except ObjectNotFound:
                pass  # deleted between list and get
        versions = retry(b.list_versions, "counter")
        nums = [v.version for v in versions]
        if nums != sorted(nums, reverse=True):
            raise AssertionError("list_versions is not newest first")
except Exception:
    traceback.print_exc()
    sys.exit(1)
print(json.dumps({"reads": reads, "last_counter": last_counter, "busy": BUSY}))
"""
)


def test_concurrent_processes_mixed_workload(store, store_path, tmp_path):
    """Four writers and two readers (one read-only) on one bucket.

    Checks: no lost update on an if_match counter, each if_none_match claim is
    won exactly once, every writer's own keys end as it last wrote them, readers
    never see mixed or corrupt content or a counter going back, and the only
    error anyone saw (and retried) is the documented temporary Busy.
    """
    b = store.create_bucket("shared")
    b.put("counter", b"0")
    n_ops = _env_int("MYBUCKET_CONCURRENCY_OPS", 60)
    n_claims, n_writers = 12, 4
    stop = tmp_path / "stop"
    writers = [_popen(WRITER, str(store_path), str(w), str(n_ops), str(n_claims)) for w in range(n_writers)]
    readers = [_popen(READER, str(store_path), mode, str(stop)) for mode in ("ro", "rw")]
    snapshots = []
    try:
        # While everyone works: snapshots (when built) must be consistent copies.
        if maintenance is not None:
            for i in range(3):
                time.sleep(0.3)
                dest = tmp_path / f"snap-{i}"
                report = maintenance.snapshot(store, dest)
                assert report["ok"], report
                snapshots.append(dest)
        results = []
        for p in writers:
            out, err = p.communicate(timeout=300)
            assert p.returncode == 0, f"writer failed:\n{err}"
            results.append(json.loads(out.strip().splitlines()[-1]))
    finally:
        stop.touch()
    reader_results = []
    for p in readers:
        out, err = p.communicate(timeout=120)
        assert p.returncode == 0, f"reader failed:\n{err}"
        reader_results.append(json.loads(out.strip().splitlines()[-1]))

    total = sum(r["increments"] for r in results)
    assert int(b.get("counter")) == total, "an if_match increment was lost"
    assert len(b.list_versions("counter")) == total + 1

    winners: dict[int, int] = {}
    for w, r in enumerate(results):
        for j in r["won"]:
            assert j not in winners, f"claim {j} was won twice"
            winners[j] = w
    assert sorted(winners) == list(range(n_claims)), "a claim was never won"
    for j, w in winners.items():
        head = json.loads(b.get(f"claims/{j}").partition(b"\n")[0])
        assert head["w"] == w

    for r in results:
        for key, etag in r["own"].items():
            if etag is None:
                assert not b.exists(key)
            else:
                assert b.head(key).etag == etag
                assert sha(b.get(key)) == etag

    assert all(r["reads"] > 0 for r in reader_results), reader_results
    with b._locked_conn() as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        rows = list(b._blob_rows(conn))
    assert all(b._check_blob(row) is None for row in rows)
    for dest in snapshots:
        with open_store(dest, readonly=True) as snap:
            sb = snap.bucket("shared")
            report = maintenance.verify(sb)
            assert report["ok"], report
            assert 0 <= int(sb.get("counter")) <= total
            for info in sb.list().objects:
                data = sb.get(info.key)
                assert sha(data) == info.etag
    busy = sum(r["busy"] for r in results + reader_results)
    print(f"increments={total} busy_retries={busy} reads={[r['reads'] for r in reader_results]}")


def test_concurrent_writers_short_timeout_only_raise_busy(store, store_path):
    """With a tiny busy timeout, contended writers must fail only with Busy
    (exit code 75, plain message), never with a raw sqlite3 error."""
    b = store.create_bucket("shared")
    b.put("counter", b"0")
    code = r"""
import json, sys
from mybucket import open_store
from mybucket.errors import Busy, PreconditionFailed
b = open_store(sys.argv[1], timeout=0.001).bucket("shared")
busy = ok = 0
for i in range(150):
    try:
        b.put("w%s/%d" % (sys.argv[2], i % 7), b"x" * (i * 37 % 5000))
        ok += 1
    except Busy as exc:
        assert exc.exit_code == 75 and exc.code == "busy", exc
        assert exc.to_dict()["error"]["hint"]
        busy += 1
print(json.dumps({"ok": ok, "busy": busy}))
"""
    procs = [_popen(code, str(store_path), str(w)) for w in range(4)]
    stats = []
    for p in procs:
        out, err = p.communicate(timeout=300)
        assert p.returncode == 0, err
        stats.append(json.loads(out.strip().splitlines()[-1]))
    assert sum(s["ok"] for s in stats) > 0
    for w in range(4):
        for info in b.list(prefix=f"w{w}/").objects:
            assert sha(b.get(info.key)) == info.etag
    print("short-timeout writers:", stats)


# -- concurrency: threads ---------------------------------------------------------------------


def test_threads_sharing_and_not_sharing_a_bucket(store_path):
    """Threads sharing one Bucket object and threads with their own Store both
    serialise correctly: no lost if_match increments."""
    s = open_store(store_path)
    b = s.create_bucket("threads")
    b.put("counter", b"0")
    errors: list[BaseException] = []
    counts = [0] * 8

    def worker(i: int) -> None:
        own_store = open_store(store_path) if i % 2 else None
        bk = own_store.bucket("threads") if own_store else b
        try:
            done = 0
            while done < 25:
                try:
                    with bk.open("counter") as stream:
                        value, etag = int(stream.read()), stream.info.etag
                    bk.put("counter", str(value + 1).encode(), if_match=etag)
                    done += 1
                except (PreconditionFailed, Busy):
                    continue
                bk.put(f"t{i}/x", os.urandom(i * 1000 + 1))
            counts[i] = done
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)
        finally:
            if own_store:
                own_store.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=300)
    assert not errors, errors
    assert int(b.get("counter")) == sum(counts) == 200
    s.close()


# -- garbage collection racing writers (runs once gc() exists) ------------------------------------


GC_WRITER = r"""
import hashlib, json, os, random, sys
from mybucket import open_store
from mybucket.errors import Busy
path, n = sys.argv[1], int(sys.argv[2])
b = open_store(path, timeout=30).bucket("gcrace")
# Each key flips between two contents of its own. With trash_days=0 every
# overwrite turns the previous content into garbage right before the writer
# needs it again: exactly the window gc must not get wrong.
sizes = [200_000, 300_000, 5_000, 6 * 1024 * 1024]
contents = {
    "k%d" % j: [random.Random(10 * j + v).randbytes(sizes[j]) for v in range(2)]
    for j in range(len(sizes))
}
final = {}
for i in range(n):
    key = "k%d" % (i % len(sizes))
    data = contents[key][(i // len(sizes)) % 2]
    while True:
        try:
            if i % 7 == 6 and key in final:
                b.delete(key)
                final.pop(key)
            else:
                b.put(key, data)
                final[key] = hashlib.sha256(data).hexdigest()
            break
        except Busy:
            continue
print(json.dumps(final))
"""


def test_gc_never_removes_a_blob_a_writer_references(store, store_path):
    """Writers keep deleting content (trash_days=0, so it becomes garbage at once)
    and putting the same content back, while gc() runs in a loop."""
    if maintenance is None:
        pytest.skip("mybucket.maintenance is not built yet")
    b = store.create_bucket("gcrace", trash_days=0)

    def gc():
        # orphan_grace=0: the most aggressive setting; safety must not depend on the grace period.
        return maintenance.gc(b, orphan_grace=0)

    proc = _popen(GC_WRITER, str(store_path), "300")
    runs = removed = 0
    while proc.poll() is None:
        try:
            removed += gc()["blobs_removed"]
            runs += 1
            report = maintenance.verify(b)
            assert report["ok"], report
        except Busy:
            pass
        for info in b.list().objects:
            try:
                # open() reads .info and content from one snapshot (list then get would not).
                with b.open(info.key) as stream:
                    assert sha(stream.read()) == stream.info.etag
            except ObjectNotFound:
                pass
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == 0, err
    final = json.loads(out.strip().splitlines()[-1])
    gc()
    assert sorted(final) == b.list().keys
    for key, etag in final.items():
        assert sha(b.get(key)) == etag
    assert runs > 0 and removed > 0, (runs, removed)
    with b._locked_conn() as conn:
        rows = list(b._blob_rows(conn))
    assert all(b._check_blob(row) is None for row in rows)
    assert maintenance.verify(b)["ok"]
    print(f"gc runs={runs} blobs removed={removed}")


def test_streaming_put_memory_does_not_grow_with_size(bucket):
    """Peak Python memory of a streamed put is the same for 8 MiB and 64 MiB."""
    import tracemalloc

    def stream(mib: int):
        base = random.Random(mib).randbytes(1 << 20)
        for i in range(mib):
            yield base[:-8] + i.to_bytes(8, "big")

    peaks = {}
    for mib in (8, 64):
        tracemalloc.start()
        try:
            info = bucket.put(f"big/{mib}", stream(mib))
            peaks[mib] = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        assert info.size == mib << 20
    assert peaks[64] < peaks[8] + 2 * (1 << 20), peaks
    assert peaks[64] < 24 * (1 << 20), peaks
