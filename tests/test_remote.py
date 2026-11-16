"""open_store("ssh://...") and the client library, against a real ``mybucket serve --stdio`` child.

ssh itself is replaced by a fake ``ssh`` on PATH (tests/remote_helpers.py)
that records its arguments and runs the remote words locally, so these
tests check the exact command line too, without any network.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys

import pytest

from mybucket import (
    AccessDenied,
    BucketNotFound,
    ContentMismatch,
    DamagedError,
    InvalidArgument,
    InvalidKey,
    NotAvailable,
    ObjectNotFound,
    PreconditionFailed,
    ReadOnlyError,
    StoreError,
    StoreNotFound,
    open_store,
)
from mybucket.remote import RemoteStore, connect, parse_ssh_url, ssh_argv

from remote_helpers import child_env, install_fake_ssh, serve_argv


@pytest.fixture
def fake_ssh(tmp_path, monkeypatch):
    bindir = tmp_path / "bin"
    log = install_fake_ssh(bindir)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PYTHONPATH", child_env()["PYTHONPATH"])
    monkeypatch.delenv("MYBUCKET_SSH", raising=False)
    monkeypatch.delenv("MYBUCKET_REMOTE_COMMAND", raising=False)
    return log


def calls(log):
    return [json.loads(line) for line in log.read_text().splitlines()]


@pytest.fixture
def remote(tmp_path, fake_ssh):
    path = tmp_path / "remote-store"
    store = open_store(f"ssh://example-host{path}")
    yield store
    store.close()


# -- the ssh command line --------------------------------------------------------------------


def test_ssh_command_is_an_exact_argument_list(tmp_path, fake_ssh):
    path = tmp_path / "s"
    with open_store(f"ssh://backup-user@example-host:2222{path}", timeout=3) as s:
        assert isinstance(s, RemoteStore) and not s.readonly
    with open_store(f"ssh://example-host{path}", readonly=True):
        pass
    first, second = calls(fake_ssh)
    assert first == [
        "-T", "-x", "-a", "-o", "ClearAllForwardings=yes", "-p", "2222", "--", "backup-user@example-host",
        "mybucket", "serve", "--stdio", "--store", str(path), "--write-all", "--timeout", "3",
    ]
    assert second[second.index("--") + 1 :] == [
        "example-host", "mybucket", "serve", "--stdio", "--store", str(path), "--read-all", "--timeout", "10",
    ]


def test_remote_command_can_be_replaced(tmp_path, fake_ssh, monkeypatch):
    monkeypatch.setenv("MYBUCKET_REMOTE_COMMAND", "env mybucket")
    with open_store(f"ssh://example-host{tmp_path / 's'}"):
        pass
    words = calls(fake_ssh)[0]
    assert words[words.index("example-host") + 1 : words.index("serve")] == ["env", "mybucket"]
    for bad in (["mybucket;", "rm"], ["$(id)"], ["-oProxyCommand=x"], ["a b"], ["`id`"]):
        with pytest.raises(InvalidArgument):
            open_store(f"ssh://example-host{tmp_path / 's'}", remote_command=bad)


@pytest.mark.parametrize(
    "url",
    [
        "ssh://-oProxyCommand=evil/srv/store",
        "ssh://host;id/srv/store",
        "ssh://ho st/srv/store",
        "ssh://host/srv/my store",
        "ssh://host/srv/$(id)",
        "ssh://host/srv/`id`",
        "ssh://host/srv/a;b",
        "ssh://host/srv/*",
        "ssh://host/srv/../../etc",
        "ssh://host/~user/x",
        "ssh://-l@host/srv",
        "ssh://us er@host/srv",
        "ssh://host:0/srv",
        "ssh://host:99999/srv",
        "ssh://host:22x/srv",
        "ssh://host",
        "ssh://host/",
        "ssh://[::1/srv",
        "ssh://[zz]/srv",
        "sftp://host/srv",
    ],
)
def test_hostile_urls_are_refused_before_anything_runs(url, fake_ssh):
    with pytest.raises(InvalidArgument):
        open_store(url)
    assert not fake_ssh.exists()


def test_url_forms():
    assert parse_ssh_url("ssh://h/abs/path") == (None, "h", None, "/abs/path")
    t = parse_ssh_url("ssh://u@h.example:2200/srv/store")
    assert (t.user, t.host, t.port, t.path) == ("u", "h.example", 2200, "/srv/store")
    t = parse_ssh_url("ssh://h/~/stores/main")
    assert t.path == "~/stores/main"
    t = parse_ssh_url("ssh://[::1]:22/srv")
    assert (t.host, t.port) == ("::1", 22)
    argv = ssh_argv(parse_ssh_url("ssh://h/~/x"), readonly=True, timeout=10)
    assert argv[argv.index("--store") + 1] == "~/x"


def test_ssh_failure_is_a_plain_error(tmp_path, fake_ssh, monkeypatch, capfd):
    monkeypatch.setenv("FAKE_SSH_FAIL", "1")
    with pytest.raises(StoreError) as err:
        open_store(f"ssh://example-host{tmp_path / 's'}")
    assert "closed (exit status 255)" in err.value.message and "ssh example-host" in err.value.hint
    assert "Connection refused" in capfd.readouterr().err  # ssh's own message is shown


def test_missing_remote_mybucket(tmp_path, fake_ssh, monkeypatch):
    monkeypatch.setenv("MYBUCKET_REMOTE_COMMAND", "no-such-mybucket-command")
    with pytest.raises(StoreError) as err:
        open_store(f"ssh://example-host{tmp_path / 's'}")
    assert "MYBUCKET_REMOTE_COMMAND" in err.value.hint


# -- the same API as a local store -------------------------------------------------------------


def test_round_trip_through_the_same_api(remote):
    b = remote.bucket("notes", create=True)
    info = b.put("2026/11/14/day.json", b'{"steps": 1}', content_type="application/json", metadata={"source": "watch"})
    assert info.status == "created" and info.etag == hashlib.sha256(b'{"steps": 1}').hexdigest()
    assert b.put("2026/11/14/day.json", b'{"steps": 1}', content_type="application/json", metadata={"source": "watch"}).unchanged
    assert b.get("2026/11/14/day.json") == b'{"steps": 1}'
    head = b.head("2026/11/14/day.json")
    assert head.metadata == {"source": "watch"} and head.content_type == "application/json"
    assert b.exists("2026/11/14/day.json") and not b.exists("nope")
    with pytest.raises(ObjectNotFound):
        b.get("nope")
    with pytest.raises(InvalidKey):
        b.head("../x")
    assert remote.list_buckets() == ["notes"]
    with pytest.raises(BucketNotFound):
        remote.bucket("missing")


def test_unicode_and_space_keys_remotely(remote):
    b = remote.bucket("photos", create=True)
    b.put("summer trip/IMG 1234.jpg", b"jpeg")
    b.put("café/menu.txt", b"menu")  # sent decomposed, stored NFC
    assert b.list().keys == ["café/menu.txt", "summer trip/IMG 1234.jpg"]
    assert b.get("café/menu.txt") == b"menu"
    assert b.list(prefix="summer trip/").keys == ["summer trip/IMG 1234.jpg"]


def test_streaming_both_ways_and_ranges(remote):
    b = remote.bucket("media", create=True)
    data = os.urandom(3 << 20) + bytes(range(256)) * 20000
    chunks = (data[i : i + 300_000] for i in range(0, len(data), 300_000))
    info = b.put("video.bin", chunks, expected_size=len(data))
    assert info.size == len(data)
    with b.open("video.bin") as stream:
        h = hashlib.sha256()
        while chunk := stream.read(65536):
            h.update(chunk)
        assert stream.verified and h.hexdigest() == info.etag
    for start, end in [(0, 1), (1000, 2_000_000), ((1 << 20) - 1, (1 << 20) + 1), (len(data) - 3, None), (len(data) + 10, None)]:
        assert b.get("video.bin", start, end) == data[start:end]
    with b.open("video.bin", 10, 20) as stream:
        assert stream.read() == data[10:20] and (stream.start, stream.end) == (10, 20)


def test_partial_read_then_close_does_not_block_the_connection(remote):
    b = remote.bucket("media", create=True)
    b.put("big", bytes(30 << 20))
    with b.open("big") as stream:
        assert stream.read(10) == bytes(10)
    assert b.get("big", 0, 5) == bytes(5)  # the connection is usable right away
    s1, s2 = b.open("big", 0, 3), b.open("big", 3, 6)
    assert s1.read() == bytes(3) and s2.read() == bytes(3)
    s1.close(), s2.close()


def test_conditional_writes_batches_versions_and_restore(remote):
    b = remote.bucket("data", create=True)
    first = b.put("counter", b"1")
    with pytest.raises(PreconditionFailed) as err:
        b.put("counter", b"2", if_match="0" * 64)
    assert err.value.exit_code == 5
    b.put("counter", b"2", if_match=first.etag)
    with pytest.raises(PreconditionFailed):
        b.put("counter", b"x", if_none_match="*")
    with pytest.raises(ContentMismatch):
        b.put("short", b"abc", expected_size=4)
    assert not b.exists("short")
    with b.batch() as tx:
        tx.put("a", b"A")
        tx.put("b", b"B")
        tx.delete("counter")
    assert [type(r).__name__ for r in tx.results] == ["PutInfo", "PutInfo", "ObjectInfo"]
    with pytest.raises(RuntimeError):
        with b.batch() as tx:
            tx.put("c", b"C")
            raise RuntimeError("caller gave up")
    assert not b.exists("c")
    with pytest.raises(PreconditionFailed) as err:
        with b.batch() as tx:
            tx.put("d", b"D")
            tx.put("a", b"again", if_none_match="*")
    assert "No operation in the batch was applied" in err.value.message and not b.exists("d")
    versions = b.list_versions("counter")
    assert [v.reason for v in versions] == ["deleted", "overwritten"]
    assert b.restore("counter", versions[0].version).status == "created"
    assert b.get("counter") == b"2"
    assert b.delete("nothing", missing_ok=True) is None
    page = b.list(limit=2)
    assert page.truncated and page.keys == ["a", "b"]
    assert b.list(start_after=page.next_start_after).keys == ["counter"]
    st = b.stats()
    assert st.objects == 3 and st.bucket == "data"


def test_listing_pages_through_everything(remote):
    b = remote.bucket("many", create=True)
    for part in range(3):  # a remote batch holds at most 1,000 operations
        with b.batch() as tx:
            for i in range(part * 500, min(1205, part * 500 + 500)):
                tx.put(f"k/{i:05d}", b"x")
    assert len(b.list().objects) == 1205
    assert len(b.list(limit=1001).objects) == 1001
    assert b.list(delimiter="/").prefixes == ["k/"]


def test_failing_source_writes_nothing_and_raises_its_own_error(remote):
    b = remote.bucket("data", create=True)

    def source():
        yield b"x" * (2 << 20)
        raise OSError("the disk went away")

    with pytest.raises(OSError, match="the disk went away"):
        b.put("broken", source())
    assert not b.exists("broken")
    assert b.put("after", b"fine").status == "created"  # the connection is still in step


def test_maintenance_is_not_available_remotely(remote):
    b = remote.bucket("data", create=True)
    for call in (remote.gc, remote.verify, lambda: remote.snapshot("/tmp/x"), lambda: remote.delete_bucket("data"), b.verify, b.gc, lambda: b.export_dir("/tmp/x"), lambda: b.import_dir("/tmp/x"), b.train_dictionary, lambda: b.configure(trash_days=1)):
        with pytest.raises(NotAvailable):
            call()


def test_read_only_remote(tmp_path, fake_ssh):
    path = tmp_path / "s"
    with open_store(path) as local:
        local.bucket("data", create=True).put("k", b"v")
    with open_store(f"ssh://example-host{path}", readonly=True) as s:
        b = s.bucket("data")
        assert b.readonly and b.get("k") == b"v"
        with pytest.raises(ReadOnlyError):
            b.put("k", b"w")
        with pytest.raises(ReadOnlyError):
            s.create_bucket("new-one")
    with pytest.raises(StoreNotFound) as err:
        open_store(f"ssh://example-host{tmp_path / 'missing'}", readonly=True)
    assert str(tmp_path) not in err.value.message  # the server does not show its paths


def test_profile_from_the_serving_side_wins(tmp_path):
    """A forced command decides; the client asking for write access does not change that."""
    path = tmp_path / "s"
    with open_store(path) as local:
        b = local.bucket("pub", create=True)
        b.put("public/a", b"a")
        b.put("private/b", b"b")
    s = connect(serve_argv(path, "--read", "pub/public/"), readonly=False)
    try:
        assert s.readonly and s.grants == [{"bucket": "pub", "prefix": "public/", "write": False}]
        b = s.bucket("pub")
        assert b.get("public/a") == b"a"
        with pytest.raises(AccessDenied) as err:
            b.get("private/b")
        assert err.value.exit_code == 4
        with pytest.raises(AccessDenied):
            b.list()
        assert b.list(prefix="public/").keys == ["public/a"]
    finally:
        s.close()


def test_server_detects_damage_and_the_client_reports_it(tmp_path):
    path = tmp_path / "s"
    with open_store(path) as local:
        info = local.bucket("data", create=True).put("photo", os.urandom(300_000))
    blob = path / "buckets/data/blobs" / info.etag[:2] / info.etag[2:4] / info.etag
    raw = bytearray(blob.read_bytes())
    raw[1000] ^= 1
    blob.write_bytes(bytes(raw))
    s = connect(serve_argv(path, "--read-all"), readonly=True)
    try:
        with pytest.raises(DamagedError):
            s.bucket("data").get("photo")
        assert s.bucket("data").exists("photo")  # still usable afterwards
    finally:
        s.close()


def test_killed_server_mid_upload_leaves_the_store_intact(tmp_path):
    path = tmp_path / "s"
    with open_store(path) as local:
        local.bucket("data", create=True).put("keep", b"old")
    s = connect(serve_argv(path, "--write-all"))
    b = s.bucket("data")
    sent = {"n": 0}

    def source():
        for _ in range(64):
            sent["n"] += 1
            if sent["n"] == 8:
                s._proc.kill()
                s._proc.wait()
            yield os.urandom(1 << 20)

    with pytest.raises(StoreError):
        b.put("keep", source())
    s.close()
    with open_store(path) as local:
        assert local.bucket("data").get("keep") == b"old"
        assert local.verify()["ok"]


def test_cli_against_a_remote_store(tmp_path, fake_ssh):
    url = f"ssh://example-host{tmp_path / 's'}"
    env = dict(os.environ)

    def run(*args, input=None, code=0):
        proc = subprocess.run([sys.executable, "-m", "mybucket", "--store", url, *args], input=input, capture_output=True, env=env, timeout=120)
        assert proc.returncode == code, (proc.stdout, proc.stderr)
        return proc

    run("mb", "pics")
    run("put", "pics", "a b/c.txt", input=b"hello world")
    assert run("cat", "pics", "a b/c.txt").stdout == b"hello world"
    assert run("cat", "pics", "a b/c.txt", "--start", "6").stdout == b"world"
    assert json.loads(run("--json", "head", "pics", "a b/c.txt").stdout)["size"] == 11
    assert "a b/c.txt" in run("ls", "pics").stdout.decode()
    out = tmp_path / "out.txt"
    run("get", "pics", "a b/c.txt", str(out), "--end", "5")
    assert out.read_bytes() == b"hello"
    doc = json.loads(run("--json", "gc", code=1).stdout)
    assert doc["error"]["code"] == "not_available"
    run("rm", "pics", "a b/c.txt")
    assert json.loads(run("--json", "versions", "pics", "a b/c.txt").stdout)["versions"][0]["reason"] == "deleted"
