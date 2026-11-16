"""The serving side treats the client as hostile: framing, validation and access profiles.

These tests talk to ``mybucket.server.serve`` with hand-made frames, never
through the client library, so nothing on the client side can hide a server
bug. After every hostile session the store is checked to be intact.
"""

from __future__ import annotations

import hashlib
import struct
import subprocess

import pytest

from mybucket import open_store
from mybucket.access import Profile
from mybucket.maintenance import verify_store

from remote_helpers import HELLO, HELLO_RO, KIND_DATA, KIND_JSON, child_env, d, frame, j, serve_argv, session, upload

ALL = Profile.parse(write_all=True)
READ_ALL = Profile.parse(read_all=True)


@pytest.fixture
def served(tmp_path):
    """A store with two buckets; returns its path. Checked to be intact after the test."""
    path = tmp_path / "store"
    with open_store(path) as s:
        pub = s.create_bucket("pub")
        pub.put("public/a.txt", b"public a")
        pub.put("public/sub/b.txt", b"public b")
        pub.put("private/secret.txt", b"secret")
        pub.put("publicity.txt", b"not under public/")
        pub.put("café/menu.txt", b"menu")
        pub.put("étude.txt", b"normalised key")  # stored as NFC: "étude.txt"
        s.create_bucket("other").put("x", b"other bucket")
    yield path
    with open_store(path, readonly=True) as s:
        report = verify_store(s)
        assert report["ok"], report


def results(frames):
    return [f for f in frames if isinstance(f, dict)]


def last_error(frames) -> dict:
    errors = [f["error"] for f in results(frames) if f.get("ok") is False]
    assert errors, frames
    return errors[-1]


def content(path, bucket, key):
    with open_store(path, readonly=True) as s:
        return s.bucket(bucket).get(key)


# -- framing ---------------------------------------------------------------------------------


def test_first_message_must_be_hello(served):
    status, out = session(served, ALL, j({"op": "buckets"}))
    assert status == 1 and last_error(out)["code"] == "protocol_error"


def test_wrong_protocol_version(served):
    status, out = session(served, ALL, j({"op": "hello", "protocol": 99}))
    assert status == 1 and "protocol 99" in last_error(out)["message"]


@pytest.mark.parametrize(
    "raw",
    [
        struct.pack(">BI", KIND_JSON, 64 * 1024 + 1),  # over the JSON limit: refused before reading it
        struct.pack(">BI", KIND_JSON, 0xFFFFFFFF),  # a 4 GiB "message"
        struct.pack(">BI", KIND_DATA, (1 << 20) + 1),  # over the data chunk limit
        struct.pack(">BI", 0x00, 3) + b"abc",  # unknown frame type
        struct.pack(">BI", KIND_JSON, 100) + b"{",  # cut off in the middle
        b"\x4a\x00",  # cut off inside the header
        frame(KIND_JSON, b"\xff\xfe not utf-8"),
        frame(KIND_JSON, b"[1, 2, 3]"),  # not an object
        frame(KIND_JSON, b'{"op": NaN}'),
        frame(KIND_JSON, b'{"op": Infinity}'),
        frame(KIND_JSON, b"[" * 5000 + b"]" * 5000),
        frame(KIND_JSON, b'{"a":' * 40 + b"1" + b"}" * 40),  # deeper than allowed
        frame(KIND_JSON, b'{"n": ' + b"9" * 5000 + b"}"),  # an integer too long to parse
        d(b"data where a request was expected"),
    ],
)
def test_broken_framing_ends_the_connection(served, raw):
    status, out = session(served, ALL, HELLO, raw, j({"op": "buckets"}))
    assert status == 1
    assert last_error(out)["code"] == "protocol_error"
    # The request after the broken frame is never served.
    assert not any(isinstance(f.get("result"), list) for f in results(out))


def test_clean_eof_ends_quietly(served):
    status, out = session(served, ALL, HELLO)
    assert status == 0 and out[0]["ok"] is True


def test_unknown_operations_are_refused_and_maintenance_is_not_offered(served):
    ops = ["gc", "verify", "snapshot", "export_dir", "import_dir", "delete_bucket", "train_dictionary", "configure", "__init__", "hello"]
    status, out = session(served, ALL, HELLO, *[j({"op": op, "bucket": "pub", "dest": "/tmp/x"}) for op in ops], j({"op": 7}))
    errors = [f["error"] for f in results(out)[1:]]
    assert len(errors) == len(ops) + 1 and all(e["code"] == "invalid_argument" for e in errors)
    assert status == 0


@pytest.mark.parametrize(
    "request_",
    [
        {"op": "head", "bucket": "pub", "key": ["public/a.txt"]},
        {"op": "head", "bucket": ["pub"], "key": "public/a.txt"},
        {"op": "head", "bucket": "pub", "key": 5},
        {"op": "list", "bucket": "pub", "limit": True},  # a bool is not a number
        {"op": "list", "bucket": "pub", "limit": "10"},
        {"op": "list", "bucket": "pub", "limit": 1.5},
        {"op": "list", "bucket": "pub", "limit": -1},
        {"op": "open", "bucket": "pub", "key": "public/a.txt", "start": -5},
        {"op": "open", "bucket": "pub", "key": "public/a.txt", "start": 10, "end": 2},
        {"op": "read", "handle": "1"},
        {"op": "read", "handle": 12345},
        {"op": "restore", "bucket": "pub", "key": "public/a.txt"},
        {"op": "restore", "bucket": "pub", "key": "public/a.txt", "version": True},
        {"op": "delete", "bucket": "pub", "key": "public/a.txt", "missing_ok": "yes"},
        {"op": "create_bucket", "bucket": "new-one", "trash_days": "7"},
        {"op": "create_bucket", "bucket": "new-one", "trash_days": 10**9},
    ],
)
def test_fields_are_type_checked(served, request_):
    status, out = session(served, ALL, HELLO, j(request_), j({"op": "buckets"}))
    assert status == 0
    error = results(out)[1]
    assert error["ok"] is False and error["error"]["code"] in ("invalid_argument", "invalid_key", "invalid_bucket_name")
    assert results(out)[2]["ok"] is True  # and the connection keeps working


# -- nothing outside the store can be named -----------------------------------------------


@pytest.mark.parametrize("bucket", ["..", "../other", "/etc", "pub/../other", "", ".", "PUB", "pub\x00", "a" * 64, "-pub"])
def test_bucket_names_cannot_leave_the_store(served, bucket):
    status, out = session(served, ALL, HELLO, j({"op": "bucket", "bucket": bucket}), j({"op": "create_bucket", "bucket": bucket}))
    for r in results(out)[1:]:
        assert r["ok"] is False and r["error"]["code"] in ("invalid_bucket_name", "invalid_argument")
    assert sorted(p.name for p in (served / "buckets").iterdir()) == ["other", "pub"]


@pytest.mark.parametrize(
    "key",
    ["../other/x", "/etc/passwd", "public/../private/secret.txt", "public/./a.txt", "public//a.txt", "a\x00b", "a\\..\\b", ".hidden", "x\u202ey"],
)
def test_hostile_keys_are_refused(served, key):
    status, out = session(
        served,
        ALL,
        HELLO,
        j({"op": "head", "bucket": "pub", "key": key}),
        j({"op": "put", "bucket": "pub", "key": key}),
        upload(b"payload"),
        j({"op": "buckets"}),
    )
    errors = results(out)[1:3]
    assert all(e["ok"] is False and e["error"]["code"] == "invalid_key" for e in errors), errors
    assert results(out)[3]["ok"] is True  # the refused upload was drained: framing still in step


def test_error_messages_do_not_show_the_store_location(served):
    status, out = session(served, ALL, HELLO, j({"op": "bucket", "bucket": "missing"}))
    error = last_error(out)
    assert error["code"] == "bucket_not_found"
    assert str(served) not in error["message"] + error["hint"] and "<store>" in error["message"]


# -- access profiles are enforced on the serving side -------------------------------------


def test_read_only_profile_refuses_every_write(served):
    data = b"should never be stored"
    writes = [
        j({"op": "put", "bucket": "pub", "key": "public/new.txt"}) + upload(data),
        j({"op": "delete", "bucket": "pub", "key": "public/a.txt"}),
        j({"op": "restore", "bucket": "pub", "key": "public/a.txt", "version": 1}),
        j({"op": "create_bucket", "bucket": "new-one"}),
        j({"op": "batch_begin", "bucket": "pub"}),
        j({"op": "batch_put", "bucket": "pub", "key": "public/new.txt"}) + upload(data),
        j({"op": "batch_delete", "bucket": "pub", "key": "public/a.txt"}),
        j({"op": "batch_commit"}),
    ]
    status, out = session(served, READ_ALL, HELLO, *writes, j({"op": "head", "bucket": "pub", "key": "public/a.txt"}))
    answers = results(out)[1:]
    assert all(a["ok"] is False for a in answers[:-1]), answers
    assert {a["error"]["code"] for a in answers[:-1]} <= {"access_denied", "invalid_argument", "read_only"}
    assert answers[-1]["ok"] is True
    assert content(served, "pub", "public/a.txt") == b"public a"
    with open_store(served, readonly=True) as s:
        assert not s.bucket("pub").exists("public/new.txt") and s.list_buckets() == ["other", "pub"]


def test_client_cannot_widen_its_profile(served):
    # A read-only profile ignores hello's wishes; a write profile can be narrowed by the client.
    _, out = session(served, READ_ALL, j({"op": "hello", "protocol": 1, "readonly": False, "grants": [{"bucket": None, "write": True}]}))
    assert out[0]["result"]["readonly"] is True
    put = j({"op": "put", "bucket": "pub", "key": "public/x"}) + upload(b"x")
    _, out = session(served, ALL, HELLO_RO, put)
    assert out[0]["result"]["readonly"] is True and last_error(out)["code"] == "access_denied"
    assert not open_store(served, readonly=True).bucket("pub").exists("public/x")


PUBLIC_RO = Profile.parse(read=["pub/public/"])


@pytest.mark.parametrize(
    "request_",
    [
        {"op": "head", "bucket": "pub", "key": "private/secret.txt"},
        {"op": "exists", "bucket": "pub", "key": "private/secret.txt"},
        {"op": "exists", "bucket": "pub", "key": "does/not/exist"},  # same answer whether it exists or not
        {"op": "open", "bucket": "pub", "key": "private/secret.txt"},
        {"op": "open", "bucket": "pub", "key": "publicity.txt"},  # "public" without the "/" is not inside
        {"op": "versions", "bucket": "pub", "key": "private/secret.txt"},
        {"op": "list", "bucket": "pub"},
        {"op": "list", "bucket": "pub", "prefix": "pub"},
        {"op": "list", "bucket": "pub", "prefix": "private/"},
        {"op": "list", "bucket": "pub", "prefix": "", "delimiter": "/"},
        {"op": "stats", "bucket": "pub"},
        {"op": "bucket", "bucket": "other"},
        {"op": "head", "bucket": "other", "key": "x"},
        {"op": "bucket", "bucket": "missing"},  # the same refusal as for a bucket that exists
        {"op": "create_bucket", "bucket": "mine"},
    ],
)
def test_prefix_scoped_read_only_profile_cannot_escape(served, request_):
    status, out = session(served, PUBLIC_RO, HELLO, j(request_))
    error = last_error(out)
    assert error["code"] == "access_denied", error
    assert error["exit_code"] == 4
    assert set(error) == {"code", "message", "hint", "exit_code"}
    # Nothing about what lies outside the scope comes back: the refusal for a key or
    # bucket that exists reads exactly like the one for a name that does not.
    probe = dict(request_)
    for field, missing in (("key", "private/no-such-thing.txt"), ("bucket", "no-such-bucket")):
        if field in probe and probe[field] not in ("pub",):
            original = probe[field]
            probe[field] = missing
            _, out2 = session(served, PUBLIC_RO, HELLO, j(probe))
            other = last_error(out2)
            assert other["message"].replace(missing, original) == error["message"]
            assert other["hint"] == error["hint"]
            break


def test_prefix_scope_allows_what_it_covers(served):
    status, out = session(
        served,
        PUBLIC_RO,
        HELLO,
        j({"op": "buckets"}),
        j({"op": "head", "bucket": "pub", "key": "public/a.txt"}),
        j({"op": "list", "bucket": "pub", "prefix": "public/"}),
        j({"op": "list", "bucket": "pub", "prefix": "public/", "delimiter": "/", "start_after": "private/zzz"}),
        j({"op": "open", "bucket": "pub", "key": "public/sub/b.txt"}),
        j({"op": "read", "handle": 1, "max": 100}),
    )
    r = results(out)
    assert r[1]["result"] == ["pub"]  # "other" is not even listed
    assert r[2]["result"]["key"] == "public/a.txt"
    assert [o["key"] for o in r[3]["result"]["objects"]] == ["public/a.txt", "public/sub/b.txt"]
    assert r[4]["result"]["prefixes"] == ["public/sub/"]
    assert b"public b" in [f for f in out if isinstance(f, bytes)]
    assert r[-1]["result"]["eof"] is True and r[-1]["result"]["verified"] is True


def test_prefix_is_checked_on_the_normalised_key(served):
    # The scope "pub/e" covers keys starting with the letter e. "e" + COMBINING ACUTE is
    # stored as "é", which is outside it: checking the raw text would let it through.
    scope = Profile.parse(write=["pub/e"])
    status, out = session(
        served,
        scope,
        HELLO,
        j({"op": "head", "bucket": "pub", "key": "étude.txt"}),
        j({"op": "put", "bucket": "pub", "key": "évil.txt"}) + upload(b"evil"),
        j({"op": "put", "bucket": "pub", "key": "easy.txt"}) + upload(b"ok"),
    )
    r = results(out)
    assert r[1]["error"]["code"] == "access_denied" and r[2]["error"]["code"] == "access_denied"
    assert r[3]["ok"] is True
    with open_store(served, readonly=True) as s:
        assert not s.bucket("pub").exists("évil.txt") and s.bucket("pub").get("easy.txt") == b"ok"
    # And the other way round: a scope written decomposed matches the stored (NFC) keys.
    nfd_scope = Profile.parse(read=["pub/café/"])
    _, out = session(served, nfd_scope, HELLO, j({"op": "head", "bucket": "pub", "key": "café/menu.txt"}))
    assert results(out)[1]["ok"] is True


def test_prefix_scoped_writer_cannot_touch_other_keys(served):
    scope = Profile.parse(write=["pub/public/"])
    ops = [
        j({"op": "put", "bucket": "pub", "key": "private/secret.txt"}) + upload(b"overwritten"),
        j({"op": "delete", "bucket": "pub", "key": "private/secret.txt"}),
        j({"op": "restore", "bucket": "pub", "key": "private/secret.txt", "version": 1}),
        j({"op": "batch_begin", "bucket": "pub"}),
        j({"op": "batch_put", "bucket": "pub", "key": "public/ok.txt"}) + upload(b"fine"),
        j({"op": "batch_put", "bucket": "pub", "key": "private/secret.txt"}) + upload(b"overwritten"),
        j({"op": "batch_delete", "bucket": "pub", "key": "private/secret.txt"}),
        j({"op": "batch_commit"}),
        j({"op": "put", "bucket": "other", "key": "x"}) + upload(b"overwritten"),
    ]
    status, out = session(served, scope, HELLO, *ops)
    r = results(out)[1:]
    assert [a["ok"] for a in r] == [False, False, False, True, True, False, False, True, False], r
    assert content(served, "pub", "private/secret.txt") == b"secret"
    assert content(served, "pub", "public/ok.txt") == b"fine"  # the allowed part of the batch
    assert content(served, "other", "x") == b"other bucket"


def test_bucket_scoped_profile(served):
    scope = Profile.parse(read=["other"])
    status, out = session(served, scope, HELLO, j({"op": "buckets"}), j({"op": "stats", "bucket": "other"}), j({"op": "head", "bucket": "pub", "key": "public/a.txt"}))
    r = results(out)
    assert r[1]["result"] == ["other"] and r[2]["ok"] is True and r[3]["error"]["code"] == "access_denied"


# -- uploads are checked before anything is committed -----------------------------------


def test_upload_with_wrong_size_or_hash_writes_nothing(served):
    data = b"the real content"
    wrong = [
        j({"op": "put", "bucket": "pub", "key": "public/w1"}) + d(data) + j({"end": True, "size": len(data) + 1, "sha256": hashlib.sha256(data).hexdigest()}),
        j({"op": "put", "bucket": "pub", "key": "public/w2"}) + d(data) + j({"end": True, "size": len(data), "sha256": "0" * 64}),
        j({"op": "put", "bucket": "pub", "key": "public/w3"}) + d(data) + j({"end": True, "abort": True}),
        j({"op": "put", "bucket": "pub", "key": "public/w4"}) + d(data) + j({"end": True}),
        j({"op": "put", "bucket": "pub", "key": "public/w5", "expected_size": 3}) + upload(data),
    ]
    status, out = session(served, ALL, HELLO, *wrong)
    r = results(out)[1:]
    assert [a["error"]["code"] for a in r] == ["content_mismatch"] * 5
    with open_store(served, readonly=True) as s:
        assert not any(s.bucket("pub").exists(f"public/w{i}") for i in range(1, 6))


def test_upload_cut_off_writes_nothing_and_leaves_only_temp_garbage(served):
    status, out = session(served, ALL, HELLO, j({"op": "put", "bucket": "pub", "key": "public/cut"}), d(b"x" * 1000), d(b"y" * 10))
    assert status == 1
    with open_store(served) as s:
        assert not s.bucket("pub").exists("public/cut")


def test_request_in_place_of_end_marker_is_a_protocol_error(served):
    status, out = session(served, ALL, HELLO, j({"op": "put", "bucket": "pub", "key": "public/x"}), d(b"abc"), j({"op": "delete", "bucket": "pub", "key": "private/secret.txt"}))
    assert status == 1 and last_error(out)["code"] == "protocol_error"
    assert content(served, "pub", "private/secret.txt") == b"secret"
    with open_store(served, readonly=True) as s:
        assert not s.bucket("pub").exists("public/x")


def test_large_upload_streams_and_is_verified(served):
    data = bytes(range(256)) * (5 * 4096 + 7)  # a bit over 5 MiB: the streaming path
    status, out = session(served, ALL, HELLO, j({"op": "put", "bucket": "pub", "key": "public/big"}) + upload(data))
    assert results(out)[1]["ok"] is True
    assert content(served, "pub", "public/big") == data


# -- bounded resources --------------------------------------------------------------------


def test_stream_handles_are_limited(served):
    opens = [j({"op": "open", "bucket": "pub", "key": "public/a.txt"}) for _ in range(17)]
    status, out = session(served, ALL, HELLO, *opens, j({"op": "close", "handle": 1}), j({"op": "open", "bucket": "pub", "key": "public/a.txt"}))
    r = results(out)[1:]
    assert all(a["ok"] for a in r[:16]) and r[16]["error"]["code"] == "refused"
    assert r[18]["ok"] is True  # after closing one, a new stream may be opened


def test_read_window_is_clamped(served):
    with open_store(served) as s:
        s.bucket("pub").put("public/big", bytes(20 << 20))
    status, out = session(served, ALL, HELLO, j({"op": "open", "bucket": "pub", "key": "public/big"}), j({"op": "read", "handle": 1, "max": 1 << 40}))
    data = sum(len(f) for f in out if isinstance(f, bytes))
    assert data == 16 << 20 and results(out)[-1]["result"]["eof"] is False
    assert max(len(f) for f in out if isinstance(f, bytes)) <= 1 << 20


def test_batch_limits(served, monkeypatch):
    from mybucket import server

    monkeypatch.setattr(server, "MAX_BATCH_OPS", 3)
    monkeypatch.setattr(server, "MAX_BATCH_BYTES", 1000)
    ops = [j({"op": "batch_begin", "bucket": "pub"})]
    ops += [j({"op": "batch_put", "bucket": "pub", "key": f"public/n{i}"}) + upload(b"x" * 10) for i in range(4)]
    ops += [j({"op": "batch_commit"})]
    ops += [j({"op": "batch_begin", "bucket": "pub"}), j({"op": "batch_put", "bucket": "pub", "key": "public/huge"}) + upload(b"z" * 5000)]
    ops += [j({"op": "batch_begin", "bucket": "pub"}), j({"op": "batch_put", "bucket": "other", "key": "x"}) + upload(b"z")]
    ops += [j({"op": "batch_commit"}), j({"op": "batch_commit"})]
    status, out = session(served, ALL, HELLO, *ops)
    r = results(out)[1:]
    assert [a["ok"] for a in r[:6]] == [True, True, True, True, False, True]
    assert r[4]["error"]["code"] == "refused" and len(r[5]["result"]) == 3
    assert r[7]["error"]["code"] == "refused"  # over the byte limit: not added
    assert r[8]["error"]["code"] == "refused"  # a batch is already open
    assert r[9]["error"]["code"] == "invalid_argument"  # the open batch is for another bucket
    assert r[10]["ok"] is True and r[10]["result"] == []
    assert r[11]["error"]["code"] == "invalid_argument"  # nothing open any more
    with open_store(served, readonly=True) as s:
        assert s.bucket("pub").list(prefix="public/n").keys == ["public/n0", "public/n1", "public/n2"]
        assert not s.bucket("pub").exists("public/huge")


def test_open_batch_is_discarded_when_the_client_goes_away(served):
    status, out = session(served, ALL, HELLO, j({"op": "batch_begin", "bucket": "pub"}), j({"op": "batch_put", "bucket": "pub", "key": "public/never"}) + upload(b"x"))
    with open_store(served, readonly=True) as s:
        assert not s.bucket("pub").exists("public/never")


def test_unexpected_errors_are_reported_without_details(served, monkeypatch):
    from mybucket import server

    def boom(s, msg):
        raise KeyError("/some/secret/path")

    monkeypatch.setitem(server._HANDLERS, "buckets", boom)
    status, out = session(served, ALL, HELLO, j({"op": "buckets"}), j({"op": "bucket", "bucket": "pub"}))
    error = results(out)[1]["error"]
    assert error["code"] == "internal" and "secret" not in error["message"] + error["hint"]
    assert results(out)[2]["ok"] is True


# -- the real command -----------------------------------------------------------------------


def test_serve_requires_explicit_grants(served):
    proc = subprocess.run(serve_argv(served), input=HELLO, capture_output=True, env=child_env(), timeout=60)
    assert proc.returncode == 2 and b"No access was granted" in proc.stderr and proc.stdout == b""
    proc = subprocess.run(serve_argv(served, "--read", "../x"), input=HELLO, capture_output=True, env=child_env(), timeout=60)
    assert proc.returncode == 2 and b"not valid" in proc.stderr


def test_serve_subprocess_hostile_input(served):
    """The real command line: garbage after hello ends the session; nothing on stdout but frames."""
    garbage = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"
    proc = subprocess.run(serve_argv(served, "--write-all"), input=HELLO + garbage, capture_output=True, env=child_env(), timeout=60)
    assert proc.returncode == 1
    from remote_helpers import parse

    frames = parse(proc.stdout)
    assert frames[0]["ok"] is True and frames[-1]["error"]["code"] == "protocol_error"


def test_read_only_serve_opens_sqlite_read_only(served, monkeypatch):
    """Defence in depth: even if a profile check were missing, the engine itself refuses."""
    from mybucket.access import Profile as P

    monkeypatch.setattr(P, "require_key", lambda self, name, key, write: None)
    monkeypatch.setattr(P, "writable", lambda self, name: True)
    status, out = session(served, READ_ALL, HELLO, j({"op": "put", "bucket": "pub", "key": "public/a.txt"}) + upload(b"changed"))
    assert last_error(out)["code"] == "read_only"
    assert content(served, "pub", "public/a.txt") == b"public a"
