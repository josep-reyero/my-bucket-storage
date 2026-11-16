"""Regression tests for the security audit fixes.

Each test builds a small malformed store (or a stub peer) in a temp folder,
using harmless stand-ins only (links to a second temp folder, placeholder
files), and checks the safe behaviour: a refusal, a skip or a clean error.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from pathlib import Path

import pytest

from mybucket import (
    DamagedError,
    InvalidArgument,
    ProtocolError,
    Refused,
    StoreError,
    UnsafeStore,
    open_store,
    sync,
)
from mybucket.access import Profile
from mybucket.blobs import blob_path
from mybucket.maintenance import verify

from remote_helpers import HELLO, j, session, upload

BIG = 300_000  # stored in a blob file (above the 128 KiB inline limit), incompressible


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_store(tmp_path: Path, data: bytes | None = None):
    data = os.urandom(BIG) if data is None else data
    path = tmp_path / "store"
    with open_store(path) as s:
        info = s.bucket("box", create=True).put("big.bin", data)
        s.bucket("box").put("small.txt", b"small")
    return path, path / "buckets" / "box", info


def db(bdir: Path) -> sqlite3.Connection:
    return sqlite3.connect(bdir / "bucket.sqlite")


# -- A4: a blob file is checked before any byte is handed out ---------------------------------


def test_blob_file_of_the_wrong_size_is_refused_at_open(tmp_path):
    path, bdir, info = make_store(tmp_path)
    blob_path(bdir, info.etag).write_bytes(b"placeholder")
    with open_store(path) as s:
        with pytest.raises(DamagedError, match="the row records"):
            s.bucket("box").open("big.bin")
        assert not verify(s.bucket("box"))["ok"]


def test_unframed_stream_is_checked_before_its_first_byte(tmp_path):
    path, bdir, info = make_store(tmp_path)
    blob = blob_path(bdir, info.etag)
    raw = bytearray(blob.read_bytes())
    raw[-1] ^= 1  # damage at the very end: only a full check finds it
    blob.write_bytes(bytes(raw))
    with open_store(path) as s:
        with s.bucket("box").open("big.bin") as stream, pytest.raises(DamagedError):
            stream.read(10)


# -- A3: a prefix-scoped connection learns nothing from compressed sizes ----------------------


def _dict_bucket(tmp_path: Path) -> Path:
    path = tmp_path / "store"
    with open_store(path) as s:
        b = s.bucket("box", create=True)
        for i in range(200):
            doc = {"device": "generic-watch-2", "kind": "record", "i": i, "ok": i % 3 == 0, "note": "steady"}
            b.put(f"shared/{i:04d}.json", json.dumps(doc).encode())
        assert b.train_dictionary()["kept"]
    return path


def _put_frames(key: str, data: bytes) -> list[bytes]:
    return [j({"op": "put", "bucket": "box", "key": key}), upload(data)]


def test_prefix_scoped_writer_gets_no_stored_size_and_no_dictionary(tmp_path):
    path = _dict_bucket(tmp_path)
    doc = json.dumps({"device": "generic-watch-2", "kind": "record", "i": 9999, "ok": True, "note": "steady"}).encode()
    scoped = Profile.parse(write=["box/mine/"])
    status, out = session(path, scoped, HELLO, *_put_frames("mine/a.json", doc), j({"op": "head", "bucket": "box", "key": "mine/a.json"}),
                          j({"op": "list", "bucket": "box", "prefix": "mine/"}))
    answers = [f for f in out if isinstance(f, dict)]
    assert status == 0 and all(a["ok"] for a in answers), answers
    put, head, listing = answers[1]["result"], answers[2]["result"], answers[3]["result"]
    assert put["stored_size"] is None and head["stored_size"] is None
    assert [o["stored_size"] for o in listing["objects"]] == [None]
    whole = Profile.parse(write=["box"])
    status, out = session(path, whole, HELLO, *_put_frames("other/b.json", doc + b" "))
    assert [f for f in out if isinstance(f, dict)][1]["result"]["stored_size"] > 0
    conn = db(path / "buckets" / "box")
    rows = dict(conn.execute("SELECT o.key, b.dict_id FROM objects o JOIN blobs b ON b.id = o.blob_id WHERE o.key IN ('mine/a.json', 'other/b.json')"))
    conn.close()
    assert rows["mine/a.json"] is None and rows["other/b.json"] is not None


# -- A5: a bucket the profile may only read is never upgraded ---------------------------------


def test_read_grant_does_not_upgrade_a_format_1_bucket(tmp_path):
    import shutil

    fixture = Path(__file__).parent / "data" / "v1-store"
    expected = json.loads((Path(__file__).parent / "data" / "v1-store-expected.json").read_text())
    shutil.copytree(fixture, tmp_path / "store")
    key = sorted(expected["archive"])[0]
    profile = Profile.parse(read=["archive"], write=["second"])
    status, out = session(tmp_path / "store", profile, HELLO, j({"op": "head", "bucket": "archive", "key": key}))
    assert status == 0 and [f for f in out if isinstance(f, dict)][1]["ok"]
    conn = db(tmp_path / "store" / "buckets" / "archive")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    conn.close()


# -- A6: memory ------------------------------------------------------------------------------


def test_get_refuses_more_than_its_limit_unless_asked(tmp_path):
    path, _bdir, _info = make_store(tmp_path)
    with open_store(path) as s:
        b = s.bucket("box")
        with pytest.raises(Refused, match="more than its limit"):
            b.get("big.bin", max_size=1000)
        assert len(b.get("big.bin", 0, 1000, max_size=1000)) == 1000
        assert len(b.get("big.bin", max_size=None)) == BIG
        with pytest.raises(InvalidArgument):
            b.get("big.bin", max_size=-1)


def test_zstandard_fallback_refuses_a_frame_without_a_declared_size():
    zstandard = pytest.importorskip("zstandard")
    from test_zstd_shim import _load_fallback

    fallback = _load_fallback()
    frame = zstandard.ZstdCompressor(write_content_size=False).compress(b"placeholder" * 100)
    with pytest.raises(fallback.ZstdError):
        fallback.Decompressor().decompress(frame, 2000)


# -- A7: databases written elsewhere, odd values, output --------------------------------------


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TRIGGER note AFTER INSERT ON meta BEGIN SELECT 1; END",
        "CREATE VIEW summary AS SELECT key FROM objects",
        "CREATE TABLE other (a)",
    ],
)
def test_unexpected_schema_is_refused(tmp_path, statement):
    path, bdir, _info = make_store(tmp_path)
    conn = db(bdir)
    conn.execute(statement)
    conn.commit()
    conn.close()
    with open_store(path) as s, pytest.raises(UnsafeStore, match="not written by mybucket"):
        s.bucket("box")


def test_extension_tables_named_x_are_allowed(tmp_path):
    path, bdir, _info = make_store(tmp_path)
    conn = db(bdir)
    conn.execute("CREATE TABLE x_notes (a)")
    conn.commit()
    conn.close()
    with open_store(path) as s:
        assert s.bucket("box").get("small.txt") == b"small"


def test_text_hash_is_a_clean_damage_error(tmp_path):
    path, bdir, _info = make_store(tmp_path)
    conn = db(bdir)
    conn.execute("UPDATE blobs SET hash = 'thirty-two characters of text!!!' WHERE size = 5")
    conn.commit()
    conn.close()
    with open_store(path) as s:
        b = s.bucket("box")
        with pytest.raises(DamagedError):
            b.get("small.txt")
        assert not verify(b)["ok"]


def test_unusable_dictionary_is_a_clean_damage_error(tmp_path):
    path = _dict_bucket(tmp_path)
    junk = b"placeholder, not a dictionary" * 10
    conn = db(path / "buckets" / "box")
    conn.execute("UPDATE dicts SET data = ?, hash = ?", (junk, hashlib.sha256(junk).digest()))
    conn.commit()
    conn.close()
    with open_store(path) as s:
        b = s.bucket("box")
        with pytest.raises(DamagedError, match="cannot be used"):
            b.get("shared/0001.json")
        assert b.put("shared/new.json", b'{"kind": "record"}').status == "created"  # stored without it


def test_version_numbers_beyond_the_range_are_clean_errors(tmp_path):
    path, bdir, _info = make_store(tmp_path)
    conn = db(bdir)
    conn.execute("UPDATE meta SET v = 9223372036854775807 WHERE k = 'last_version'")
    conn.commit()
    conn.close()
    with open_store(path) as s:
        b = s.bucket("box")
        with pytest.raises(StoreError, match="used up its version numbers"):
            b.put("new.txt", b"x")
        with pytest.raises(InvalidArgument):
            b.restore("small.txt", 1 << 70)


@pytest.mark.parametrize("port", ["２２", "²", "٢٢"])
def test_non_ascii_port_is_refused(port):
    from mybucket.remote import parse_ssh_url

    with pytest.raises(InvalidArgument, match="port"):
        parse_ssh_url(f"ssh://example:{port}/srv/store")


def test_cli_get_writes_owner_only_files(tmp_path):
    from mybucket.cli import main

    path, _bdir, _info = make_store(tmp_path)
    dest = tmp_path / "small-copy.txt"
    assert main(["--store", str(path), "get", "box", "small.txt", str(dest)]) == 0
    assert stat.S_IMODE(dest.stat().st_mode) == 0o600


def test_cli_ls_shows_control_characters_escaped(tmp_path, capsys):
    from mybucket.cli import main

    path, bdir, _info = make_store(tmp_path)
    conn = db(bdir)
    conn.execute("UPDATE objects SET key = 'bell' || char(7) || '.txt' WHERE key = 'small.txt'")
    conn.commit()
    conn.close()
    assert main(["--store", str(path), "ls", "box"]) == 0
    printed = capsys.readouterr().out
    assert "\x07" not in printed and "bell\\x07.txt" in printed


def test_serving_side_never_prints_a_traceback(tmp_path, monkeypatch, capsys):
    from mybucket import server

    path, _bdir, _info = make_store(tmp_path)
    monkeypatch.setenv("MYBUCKET_DEBUG", "1")

    def broken(s, msg):
        raise RuntimeError("a bug")

    monkeypatch.setitem(server._HANDLERS, "buckets", broken)
    status, out = session(path, Profile.parse(read_all=True), HELLO, j({"op": "buckets"}))
    assert [f for f in out if isinstance(f, dict)][1]["error"]["code"] == "internal"
    assert "Traceback" not in capsys.readouterr().err


# -- sync --delete -----------------------------------------------------------------------------


class _StubStore:
    """Just enough of a RemoteStore for RemoteBucket.list(): canned list pages."""

    url = "stub"
    readonly = True

    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages

    def _call(self, op, **fields):
        return self.pages.pop(0)


def _page(keys, truncated=False, cursor=None, complete=None) -> dict:
    objects = [
        {"key": k, "etag": "0" * 64, "size": 1, "content_type": None, "metadata": {}, "modified": 0, "version": 1, "stored_size": 1}
        for k in keys
    ]
    page = {"objects": objects, "prefixes": [], "truncated": truncated, "next_start_after": cursor}
    if complete is not None:
        page["complete"] = complete
    return page


def test_remote_listing_cut_short_without_a_cursor_is_an_error():
    from mybucket.remote import RemoteBucket

    bucket = RemoteBucket(_StubStore([_page(["a"], truncated=True, cursor=None)]), "box", {})
    with pytest.raises(ProtocolError, match="without saying where it continues"):
        bucket.list()
    assert RemoteBucket(_StubStore([_page(["a"], complete=True)]), "box", {}).list().complete
    assert not RemoteBucket(_StubStore([_page(["a"])]), "box", {}).list().complete  # no explicit marker


class _IncompleteSource:
    """A source store whose listing never confirms that it is complete."""

    is_remote = True
    url = "stub-source"

    def list_buckets(self):
        return ["box"]

    def bucket(self, name):
        from mybucket.remote import RemoteBucket

        return RemoteBucket(_StubStore([_page([])]), name, {})


def test_sync_delete_waits_for_a_complete_source_listing(tmp_path):
    dst = open_store(tmp_path / "dst")
    b = dst.bucket("box", create=True)
    for i in range(3):
        b.put(f"keep/{i}", b"x")
    report = sync(_IncompleteSource(), dst, delete=True)
    assert not report["ok"] and report["delete_refused"] and report["deleted"] == 0
    assert len(b.list().objects) == 3
    dst.close()


def test_sync_delete_limit(tmp_path):
    src = open_store(tmp_path / "src")
    src.bucket("box", create=True).put("kept", b"x")
    dst = open_store(tmp_path / "dst")
    d = dst.bucket("box", create=True)
    with d.batch() as tx:
        for i in range(30):
            tx.put(f"old/{i:02d}", b"x")
    report = sync(src, dst, delete=True)  # 30 of 31 objects: above half of the destination
    assert report["delete_refused"] and report["deleted"] == 0 and len(d.list().objects) == 31
    assert sync(src, dst, delete=True, max_delete=5)["deleted"] == 0
    report = sync(src, dst, delete=True, force=True)
    assert report["ok"] and report["deleted"] == 30 and d.list().keys == ["kept"]
    src.close()
    dst.close()


def test_sync_command_refusal_exits_5(tmp_path):
    from mybucket.cli import main

    with open_store(tmp_path / "src") as s:
        s.bucket("box", create=True).put("kept", b"x")
    with open_store(tmp_path / "dst") as s:
        with s.bucket("box", create=True).batch() as tx:
            for i in range(30):
                tx.put(f"old/{i:02d}", b"x")
    assert main(["sync", "--delete", str(tmp_path / "src"), str(tmp_path / "dst")]) == 5
    assert main(["sync", "--delete", "--force", str(tmp_path / "src"), str(tmp_path / "dst")]) == 0
