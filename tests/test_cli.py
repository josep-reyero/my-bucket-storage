"""The mybucket command-line tool, run as a subprocess against a temporary store."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"


def _env(extra: dict | None = None) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in ("MYBUCKET_STORE", "MYBUCKET_DEBUG")}
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(extra or {})
    return env


class CLI:
    def __init__(self, store: Path) -> None:
        self.store = store

    def run(self, *args: str, input: bytes | None = None, store: bool = True, env: dict | None = None,
            timeout: float = 60) -> subprocess.CompletedProcess:
        argv = [sys.executable, "-m", "mybucket.cli"]
        if store:
            argv += ["--store", str(self.store)]
        return subprocess.run(
            argv + list(args),
            input=input if input is not None else b"",
            capture_output=True,
            env=_env(env),
            timeout=timeout,
        )

    def json(self, *args: str, code: int = 0, **kwargs) -> dict:
        proc = self.run("--json", *args, **kwargs)
        assert proc.returncode == code, (proc.returncode, proc.stdout, proc.stderr)
        text = proc.stdout.decode()
        assert text.count("\n") == 1, text  # exactly one JSON document, nothing else
        return json.loads(text)

    def ok(self, *args: str, **kwargs) -> subprocess.CompletedProcess:
        proc = self.run(*args, **kwargs)
        assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
        return proc


@pytest.fixture
def cli(tmp_path) -> CLI:
    return CLI(tmp_path / "store")


@pytest.fixture
def ready(cli) -> CLI:
    cli.ok("mb", "pics")
    return cli


def error_of(doc: dict) -> dict:
    assert set(doc) >= {"error"}
    err = doc["error"]
    assert set(err) == {"code", "message", "hint"}
    assert err["message"]
    return err


def has_maintenance(name: str) -> bool:
    try:
        from mybucket import maintenance
    except ImportError:
        return False
    return hasattr(maintenance, name)


# -- store selection, global flags, usage ------------------------------------------


def test_no_store_is_a_usage_error(cli):
    proc = cli.run("buckets", store=False)
    assert proc.returncode == 2
    assert b"MYBUCKET_STORE" in proc.stderr
    doc = json.loads(cli.run("--json", "buckets", store=False).stdout)
    assert error_of(doc)["code"] == "usage"


def test_store_from_environment(cli):
    proc = cli.run("mb", "env-bucket", store=False, env={"MYBUCKET_STORE": str(cli.store)})
    assert proc.returncode == 0, proc.stderr
    assert cli.json("buckets") == {"buckets": ["env-bucket"]}


def test_json_and_store_accepted_before_or_after_the_command(ready):
    a = ready.run("--json", "buckets")
    b = ready.run("buckets", "--json")
    c = subprocess.run(
        [sys.executable, "-m", "mybucket.cli", "buckets", "--json", "--store", str(ready.store)],
        capture_output=True, env=_env(),
    )
    assert a.stdout == b.stdout == c.stdout
    assert json.loads(a.stdout) == {"buckets": ["pics"]}


def test_read_commands_do_not_create_a_store(cli):
    for args in (("ls",), ("buckets",), ("stats",), ("head", "pics", "k")):
        doc = cli.json(*args, code=3)
        assert error_of(doc)["code"] == "store_not_found"
    assert not cli.store.exists()


def test_usage_errors_exit_2_with_plain_text_or_json(cli):
    proc = cli.run("frobnicate")
    assert proc.returncode == 2
    assert b"There is no command 'frobnicate'" in proc.stderr and b"usage:" in proc.stderr
    assert proc.stdout == b""
    for args in (("--json", "frobnicate"), ("put", "--json"), ("restore", "b", "k", "notanumber", "--json")):
        proc = cli.run(*args)
        assert proc.returncode == 2
        assert error_of(json.loads(proc.stdout))["code"] == "usage"
    # argparse wording is rewritten as plain sentences, with the command's own help as hint.
    err = error_of(cli.json("restore", "b", "k", "abc", code=2))
    assert err["message"] == "The version must be a whole number such as 3; got 'abc'."
    assert err["hint"] == "See: mybucket restore --help"
    err = error_of(cli.json("get", "b", code=2))
    assert err["message"] == "Missing KEY." and err["hint"] == "See: mybucket get --help"


def test_no_command(cli):
    assert cli.run().returncode == 2
    assert error_of(cli.json(code=2))["code"] == "usage"


def test_help_and_version(cli):
    proc = cli.ok("--help", store=False)
    assert b"export" in proc.stdout and b"Exit codes" in proc.stdout
    assert cli.ok("--version", store=False).stdout.startswith(b"mybucket ")
    assert b"--if-none-match" in cli.ok("put", "--help", store=False).stdout


def test_console_script_is_installed():
    script = Path(sys.executable).parent / "mybucket"
    if not script.exists():
        pytest.skip("the mybucket console script is not installed in this environment")
    proc = subprocess.run([str(script), "--version"], capture_output=True, env=_env())
    assert proc.returncode == 0 and proc.stdout.startswith(b"mybucket ")


# -- buckets ---------------------------------------------------------------------


def test_mb_buckets_rb(cli):
    assert cli.json("mb", "one") == {"bucket": "one", "created": True, "trash_days": 7}
    assert cli.json("mb", "two", "--trash-days", "0")["trash_days"] == 0
    assert cli.json("buckets") == {"buckets": ["one", "two"]}
    assert cli.ok("ls").stdout == b"one\ntwo\n"
    assert error_of(cli.json("mb", "one", code=5))["code"] == "bucket_exists"
    assert error_of(cli.json("mb", "Bad_Name", code=2))["code"] == "invalid_bucket_name"
    cli.ok("put", "one", "k", input=b"x")
    assert error_of(cli.json("rb", "one", code=5))["code"] == "bucket_not_empty"
    assert cli.json("rb", "one", "--force") == {"bucket": "one", "deleted": True}
    assert error_of(cli.json("rb", "nope", code=3))["code"] == "bucket_not_found"
    assert cli.json("buckets") == {"buckets": ["two"]}


# -- put / get / cat / head --------------------------------------------------------


def test_put_from_stdin_and_file(ready, tmp_path):
    doc = ready.json("put", "pics", "a/hello.txt", input=b"hello\n")
    assert doc["status"] == "created" and doc["size"] == 6
    assert doc["etag"] == hashlib.sha256(b"hello\n").hexdigest()
    assert doc["content_type"] == "text/plain"  # guessed from the key
    assert ready.json("put", "pics", "a/hello.txt", input=b"hello\n")["status"] == "unchanged"

    src = tmp_path / "photo.raw"
    src.write_bytes(b"\x00\x01" * 1000)
    doc = ready.json("put", "pics", "p/1", str(src), "-t", "image/x-raw", "-m", "camera=old", "-m", "n=1")
    assert doc["content_type"] == "image/x-raw" and doc["metadata"] == {"camera": "old", "n": "1"}
    assert ready.json("put", "pics", "p/2.json", str(src), "--no-guess-type")["content_type"] is None
    human = ready.ok("put", "pics", "p/1", str(src), "-t", "image/x-raw", "-m", "camera=new").stdout.decode()
    assert human.startswith("updated p/1 ")


def test_put_errors(ready, tmp_path):
    assert error_of(ready.json("put", "pics", "../escape", input=b"x", code=2))["code"] == "invalid_key"
    assert error_of(ready.json("put", "pics", "k", "-m", "novalue", input=b"x", code=2))["code"] == "usage"
    assert error_of(ready.json("put", "nope", "k", input=b"x", code=3))["code"] == "bucket_not_found"
    assert error_of(ready.json("put", "pics", "k", str(tmp_path / "missing"), code=3))["code"] == "file_not_found"
    assert error_of(ready.json("put", "pics", "k", str(tmp_path), code=2))["code"] == "usage"


def test_conditional_put_and_delete(ready):
    first = ready.json("put", "pics", "c", "--if-none-match", input=b"v1")
    err = error_of(ready.json("put", "pics", "c", "--if-none-match", input=b"v2", code=5))
    assert err["code"] == "precondition_failed"
    wrong = "0" * 64
    assert error_of(ready.json("put", "pics", "c", "--if-match", wrong, input=b"v2", code=5))["code"] == "precondition_failed"
    second = ready.json("put", "pics", "c", "--if-match", first["etag"], input=b"v2")
    assert second["status"] == "updated"
    assert error_of(ready.json("rm", "pics", "c", "--if-match", first["etag"], code=5))["code"] == "precondition_failed"
    assert ready.json("rm", "pics", "c", "--if-match", second["etag"])["deleted"][0]["key"] == "c"


def test_get_cat_head(ready, tmp_path):
    ready.ok("put", "pics", "dir/file.txt", input=b"content here")
    assert ready.ok("cat", "pics", "dir/file.txt").stdout == b"content here"
    assert ready.ok("get", "pics", "dir/file.txt").stdout == b"content here"
    assert ready.ok("get", "pics", "dir/file.txt", "-").stdout == b"content here"

    dest = tmp_path / "copy.txt"
    doc = ready.json("get", "pics", "dir/file.txt", str(dest))
    assert dest.read_bytes() == b"content here" and doc["path"] == str(dest) and doc["size"] == 12
    folder = tmp_path / "into"
    folder.mkdir()
    ready.ok("get", "pics", "dir/file.txt", str(folder))
    assert (folder / "file.txt").read_bytes() == b"content here"

    head = ready.json("head", "pics", "dir/file.txt")
    assert head["key"] == "dir/file.txt" and head["size"] == 12 and head["content_type"] == "text/plain"
    human = ready.ok("head", "pics", "dir/file.txt").stdout.decode()
    assert "etag:" in human and head["etag"] in human

    assert error_of(ready.json("get", "pics", "missing", str(tmp_path / "m"), code=3))["code"] == "object_not_found"
    assert error_of(ready.json("head", "pics", "missing", code=3))["code"] == "object_not_found"
    assert not (tmp_path / "m").exists()
    assert error_of(ready.json("get", "pics", "dir/file.txt", code=2))["code"] == "usage"  # stdout is for JSON
    assert error_of(ready.json("cat", "pics", "dir/file.txt", code=2))["code"] == "usage"
    proc = ready.run("cat", "pics", "missing")
    assert proc.returncode == 3 and proc.stdout == b"" and b"does not exist" in proc.stderr


def test_streaming_large_object_through_stdin_and_stdout(ready, tmp_path):
    block = os.urandom(64 * 1024)
    data = b"".join(block[: (i * 7919) % len(block)] + bytes([i % 256]) * 4096 for i in range(400))  # ~14 MB
    doc = ready.json("put", "pics", "big/stream.bin", input=data)
    assert doc["size"] == len(data) and doc["etag"] == hashlib.sha256(data).hexdigest()
    assert ready.ok("cat", "pics", "big/stream.bin").stdout == data
    dest = tmp_path / "big.bin"
    ready.ok("get", "pics", "big/stream.bin", str(dest))
    assert dest.read_bytes() == data


def _damage(store: Path, bucket: str, etag: str) -> None:
    path = store / "buckets" / bucket / "blobs" / etag[:2] / etag[2:4] / etag
    raw = bytearray(path.read_bytes())
    raw[len(raw) // 2] ^= 0xFF
    path.chmod(0o600)
    path.write_bytes(bytes(raw))


def test_get_never_leaves_a_damaged_file(ready, tmp_path):
    doc = ready.json("put", "pics", "rnd", input=os.urandom(300_000))
    _damage(ready.store, "pics", doc["etag"])
    dest = tmp_path / "out.bin"
    proc = ready.run("get", "pics", "rnd", str(dest))
    assert proc.returncode == 1 and b"damaged" in proc.stderr
    assert not dest.exists()
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []
    assert error_of(ready.json("cat", "pics", "rnd", code=2))  # (usage: --json with cat)
    assert ready.run("cat", "pics", "rnd").returncode == 1


def test_broken_pipe_is_quiet(ready):
    ready.ok("put", "pics", "big", input=b"x" * 5_000_000)
    proc = subprocess.Popen(
        [sys.executable, "-m", "mybucket.cli", "--store", str(ready.store), "cat", "pics", "big"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=_env(),
    )
    proc.stdout.read(10)
    proc.stdout.close()
    proc.wait(timeout=30)
    stderr = proc.stderr.read()
    proc.stderr.close()
    assert proc.returncode == 1
    assert b"Traceback" not in stderr


# -- ls ----------------------------------------------------------------------------


def test_ls_human_and_json(ready):
    for key in ("a/1", "a/2", "a/b/3", "c", "d/4"):
        ready.ok("put", "pics", key, input=key.encode())
    lines = ready.ok("ls", "pics").stdout.decode().splitlines()
    assert [line.split()[-1] for line in lines] == ["a/1", "a/2", "a/b/3", "c", "d/4"]

    doc = ready.json("ls", "pics", "-d", "/")
    assert [o["key"] for o in doc["objects"]] == ["c"]
    assert doc["prefixes"] == ["a/", "d/"] and doc["truncated"] is False
    human = ready.ok("ls", "pics", "--delimiter", "/").stdout.decode()
    assert "PRE" in human and "a/" in human

    assert [o["key"] for o in ready.json("ls", "pics", "a/")["objects"]] == ["a/1", "a/2", "a/b/3"]
    doc = ready.json("ls", "pics", "--prefix", "a/", "--delimiter", "/")
    assert [o["key"] for o in doc["objects"]] == ["a/1", "a/2"] and doc["prefixes"] == ["a/b/"]

    doc = ready.json("ls", "pics", "--limit", "2")
    assert [o["key"] for o in doc["objects"]] == ["a/1", "a/2"]
    assert doc["truncated"] is True and doc["next_start_after"] == "a/2"
    doc = ready.json("ls", "pics", "--limit", "2", "--start-after", "a/2")
    assert [o["key"] for o in doc["objects"]] == ["a/b/3", "c"]
    proc = ready.ok("ls", "pics", "--limit", "1")
    assert b"--start-after a/1" in proc.stderr

    assert ready.ok("ls", "pics", "zzz").stderr == b"(nothing found)\n"
    assert error_of(ready.json("ls", "pics", "--limit", "0", code=2))["code"] == "usage"
    assert error_of(ready.json("ls", "pics", "a", "--prefix", "b", code=2))["code"] == "usage"
    assert error_of(ready.json("ls", "nope", code=3))["code"] == "bucket_not_found"


def test_ls_pages_through_large_buckets(ready, tmp_path):
    src = tmp_path / "many"
    src.mkdir()
    for i in range(2100):
        (src / f"k{i:05d}").write_bytes(b"%d" % i)
    doc = ready.json("import", "pics", str(src), "--no-guess-type")
    assert doc["added"] == 2100
    keys = [o["key"] for o in ready.json("ls", "pics")["objects"]]
    assert keys == sorted(f"k{i:05d}" for i in range(2100))
    assert len(ready.ok("ls", "pics").stdout.splitlines()) == 2100
    doc = ready.json("ls", "pics", "--limit", "1500")
    assert len(doc["objects"]) == 1500 and doc["truncated"] and doc["next_start_after"] == "k01499"


# -- rm / versions / restore ----------------------------------------------------------


def test_rm_single_many_and_force(ready):
    for key in ("x", "y", "z"):
        ready.ok("put", "pics", key, input=key.encode())
    assert "kept in the trash for 7 days" in ready.ok("rm", "pics", "x").stdout.decode()
    err = error_of(ready.json("rm", "pics", "y", "missing", code=3))
    assert "No operation in the batch was applied" in err["message"]
    assert [o["key"] for o in ready.json("ls", "pics")["objects"]] == ["y", "z"]  # all or nothing
    doc = ready.json("rm", "pics", "y", "z", "missing", "-f")
    assert [d["key"] for d in doc["deleted"]] == ["y", "z"] and doc["missing"] == ["missing"]
    assert ready.json("ls", "pics")["objects"] == []
    assert error_of(ready.json("rm", "pics", "a", "b", "--if-match", "0" * 64, code=2))["code"] == "usage"


def test_versions(ready):
    ready.ok("put", "pics", "doc", input=b"one")
    ready.ok("put", "pics", "doc", input=b"two")
    ready.ok("rm", "pics", "doc")
    doc = ready.json("versions", "pics", "doc")
    assert doc["key"] == "doc"
    assert [(v["version"], v["current"], v["reason"]) for v in doc["versions"]] == [
        (2, False, "deleted"),
        (1, False, "overwritten"),
    ]
    human = ready.ok("versions", "pics", "doc").stdout.decode()
    assert "deleted, kept until" in human and "overwritten" in human
    assert error_of(ready.json("versions", "pics", "never", code=3))["code"] == "object_not_found"


@pytest.mark.skipif(not has_maintenance("restore"), reason="mybucket.maintenance.restore not available")
def test_restore(ready):
    ready.ok("put", "pics", "doc", input=b"one")
    ready.ok("put", "pics", "doc", input=b"two")
    doc = ready.json("restore", "pics", "doc", "1")
    assert doc["key"] == "doc" and doc["version"] == 3
    assert ready.ok("cat", "pics", "doc").stdout == b"one"
    assert "it is now version 4" in ready.ok("restore", "pics", "doc", "2").stdout.decode()
    assert error_of(ready.json("restore", "pics", "doc", "99", code=3))["code"] == "version_not_found"


# -- maintenance commands ---------------------------------------------------------------


@pytest.mark.skipif(not has_maintenance("gc"), reason="mybucket.maintenance.gc not available")
def test_gc(ready):
    ready.ok("put", "pics", "k", input=b"x")
    doc = ready.json("gc")
    assert isinstance(doc["buckets"], list) and doc["buckets"][0]["bucket"] == "pics"
    doc = ready.json("gc", "pics")
    assert [r["bucket"] for r in doc["buckets"]] == ["pics"]
    assert ready.ok("gc").stdout.strip()
    assert error_of(ready.json("gc", "nope", code=3))["code"] == "bucket_not_found"


@pytest.mark.skipif(not has_maintenance("verify"), reason="mybucket.maintenance.verify not available")
def test_verify_reports_damage_with_exit_1(ready):
    small = ready.json("put", "pics", "small", input=b"fine")
    big = ready.json("put", "pics", "big", input=os.urandom(300_000))
    doc = ready.json("verify")
    assert doc["ok"] is True and "error" not in doc
    assert "pics: ok" in ready.ok("verify").stdout.decode()

    _damage(ready.store, "pics", big["etag"])
    doc = ready.json("verify", code=1)
    assert doc["ok"] is False and error_of(doc)["code"] == "damaged"
    assert doc["buckets"][0]["bucket"] == "pics"
    proc = ready.run("verify", "pics")
    assert proc.returncode == 1
    assert b"DAMAGED" in proc.stdout and b"damage" in proc.stderr
    assert small["etag"] not in proc.stdout.decode()


@pytest.mark.skipif(not has_maintenance("snapshot"), reason="mybucket.maintenance.snapshot not available")
def test_snapshot_is_a_usable_store(ready, tmp_path):
    ready.ok("put", "pics", "a", input=b"alpha")
    ready.ok("put", "pics", "b", input=os.urandom(300_000))
    dest = tmp_path / "snap"
    doc = ready.json("snapshot", str(dest))
    assert doc.get("ok", True) is True
    snap = CLI(dest)
    assert snap.ok("cat", "pics", "a").stdout == b"alpha"
    assert [o["key"] for o in snap.json("ls", "pics")["objects"]] == ["a", "b"]
    assert ready.run("snapshot", str(dest)).returncode == 5  # never overwrites


# -- export / import / stats -------------------------------------------------------------


def test_export_import_round_trip(ready, tmp_path):
    ready.ok("put", "pics", "x/a.txt", "-m", "who=me", input=b"aaa")
    ready.ok("put", "pics", "x/y/b.bin", input=os.urandom(200_000))
    dest = tmp_path / "exported"
    doc = ready.json("export", "pics", str(dest))
    assert (doc["written"], doc["unchanged"], doc["bytes"]) == (2, 0, 200_003)
    assert (dest / "x/a.txt").read_bytes() == b"aaa"
    human = ready.ok("export", "pics", str(dest)).stdout.decode()
    assert "0 written" in human and "2 already there" in human

    ready.ok("mb", "copy")
    doc = ready.json("import", "copy", str(dest))
    assert (doc["added"], doc["unchanged"], doc["skipped"], doc["bytes"]) == (2, 0, 0, 200_003)
    assert ready.json("head", "copy", "x/a.txt")["metadata"] == {"who": "me"}
    human = ready.ok("import", "copy", str(dest), "--prefix", "again").stdout.decode()
    assert "2 added" in human
    assert [o["key"] for o in ready.json("ls", "copy")["objects"]] == [
        "again/x/a.txt", "again/x/y/b.bin", "x/a.txt", "x/y/b.bin",
    ]


def test_export_conflicts_via_cli(ready, tmp_path):
    ready.ok("put", "pics", "a", input=b"1")
    ready.ok("put", "pics", "a/b", input=b"2")
    dest = tmp_path / "e"
    assert error_of(ready.json("export", "pics", str(dest), code=5))["code"] == "export_conflict"
    proc = ready.ok("export", "pics", str(dest), "--skip-conflicts")
    assert b"skipped a:" in proc.stderr
    assert (dest / "a/b").read_bytes() == b"2"


def test_import_reports_skipped_entries(ready, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "ok.txt").write_bytes(b"ok")
    (src / "bad name.txt ").write_bytes(b"no")  # a trailing space cannot be a key
    # Skipped files make the import exit 1 (so scripts notice), with the list on stderr.
    proc = ready.run("import", "pics", str(src))
    assert proc.returncode == 1
    assert b"WARNING: skipped 1 entry" in proc.stdout and b"skipped bad name.txt :" in proc.stderr
    assert b"--allow-skips" in proc.stderr
    doc = ready.json("import", "pics", str(src), code=1)
    assert doc["skipped"] == 1 and doc["unchanged"] == 1 and error_of(doc)["code"] == "files_skipped"
    proc = ready.ok("import", "pics", str(src), "--allow-skips")
    assert b"WARNING: skipped 1 entry" in proc.stdout
    assert error_of(ready.json("import", "pics", str(tmp_path / "missing"), code=3))["code"] == "not_found"


def test_stats(ready):
    ready.ok("mb", "other")
    ready.ok("put", "pics", "a", input=b"x" * 1000)
    ready.ok("put", "other", "b", input=b"y" * 10)
    doc = ready.json("stats")
    assert [b["bucket"] for b in doc["buckets"]] == ["other", "pics"]
    assert doc["total"]["objects"] == 2 and doc["total"]["logical_bytes"] == 1010
    assert [b["bucket"] for b in ready.json("stats", "pics")["buckets"]] == ["pics"]
    human = ready.ok("stats").stdout.decode()
    assert "total:" in human and "pics:" in human


# -- concurrency and read-only -------------------------------------------------------------


def test_locked_bucket_exits_75(ready):
    db = ready.store / "buckets" / "pics" / "bucket.sqlite"
    holder = sqlite3.connect(db, timeout=0, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        start = time.monotonic()
        doc = ready.json("--timeout", "0.3", "put", "pics", "k", input=b"x", code=75)
        assert error_of(doc)["code"] == "busy"
        assert time.monotonic() - start < 20
        # readers are not blocked by the writer
        assert ready.json("ls", "pics")["objects"] == []
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    assert ready.json("put", "pics", "k", input=b"x")["status"] == "created"


def test_json_mode_never_writes_notes_to_stdout_or_stderr(ready):
    for args in (("buckets",), ("ls", "pics"), ("stats",), ("ls", "pics", "--limit", "1")):
        proc = ready.run("--json", *args)
        assert proc.returncode == 0
        json.loads(proc.stdout)
        assert proc.stderr == b""


def test_if_none_match_is_a_flag_and_keeps_the_file_argument(ready, tmp_path):
    src = tmp_path / "f.txt"
    src.write_bytes(b"from a file")
    doc = ready.json("put", "pics", "k", "--if-none-match", str(src))
    assert doc["size"] == len(b"from a file")
    assert ready.ok("cat", "pics", "k").stdout == b"from a file"


def test_ls_options_without_a_bucket_are_refused(ready):
    for args in (("--limit", "0"), ("--prefix", ""), ("-d", "/")):
        assert error_of(ready.json("ls", *args, code=2))["code"] == "usage"


# -- review fixes ---------------------------------------------------------------------------


def test_get_refuses_destinations_inside_the_store(ready, tmp_path):
    ready.ok("put", "pics", "a.txt", input=b"hello")
    db = ready.store / "buckets" / "pics" / "bucket.sqlite"
    err = error_of(ready.json("get", "pics", "a.txt", str(db), code=2))
    assert "inside the store" in err["message"]
    error_of(ready.json("get", "pics", "a.txt", str(ready.store / "buckets" / "pics" / "blobs"), code=2))
    assert ready.json("head", "pics", "a.txt")["size"] == 5  # the bucket still works


def test_get_into_a_folder_named_with_a_trailing_slash(ready, tmp_path):
    ready.ok("put", "pics", "dir/small.txt", input=b"small!")
    missing = tmp_path / "newdir"
    err = error_of(ready.json("get", "pics", "dir/small.txt", str(missing) + "/", code=3))
    assert err["code"] == "folder_not_found" and "does not exist" in err["message"] and "mkdir -p" in err["hint"]
    assert not missing.exists()
    missing.mkdir()
    ready.ok("get", "pics", "dir/small.txt", str(missing) + "/")
    assert (missing / "small.txt").read_bytes() == b"small!"


def test_get_into_a_missing_folder_names_the_folder_not_a_temp_file(ready, tmp_path):
    ready.ok("put", "pics", "small.txt", input=b"small!")
    dest = tmp_path / "nope" / "deeper" / "out.txt"
    proc = ready.run("get", "pics", "small.txt", str(dest))
    assert proc.returncode == 3
    text = proc.stderr.decode()
    assert ".part" not in text and "mybucket-" not in text
    assert f"the folder {dest.parent} does not exist" in text and "mkdir -p" in text


def test_put_expect_size_refuses_a_cut_off_pipe(ready):
    full = os.urandom(3 << 20)
    ready.ok("put", "pics", "db.dump", input=full)
    err = error_of(ready.json("put", "pics", "db.dump", "--expect-size", str(len(full)), input=full[:1_000_000], code=5))
    assert err["code"] == "content_mismatch"
    digest = hashlib.sha256(full).hexdigest()
    error_of(ready.json("put", "pics", "db.dump", "--expect-sha256", digest, input=full[:10], code=5))
    assert ready.json("head", "pics", "db.dump")["version"] == 1
    doc = ready.json("put", "pics", "db.dump", "--expect-size", str(len(full)), "--expect-sha256", digest, input=full)
    assert doc["status"] == "unchanged"


def test_rm_force_and_missing_keys(ready):
    ready.ok("put", "pics", "a", input=b"1")
    err = error_of(ready.json("rm", "pics", "missing", code=3))
    assert "-f" in err["hint"] and "missing_ok" not in err["hint"]
    doc = ready.json("rm", "pics", "a", "missing", "-f")
    assert [d["key"] for d in doc["deleted"]] == ["a"] and doc["missing"] == ["missing"]
    assert ready.json("rm", "pics", "missing", "-f")["missing"] == ["missing"]


def test_cli_hints_name_options_not_keyword_arguments(ready):
    ready.ok("put", "pics", "a", input=b"1")
    err = error_of(ready.json("rb", "pics", code=5))
    assert "--force" in err["hint"] and "force=True" not in err["hint"]
    assert "1 object and 0 trashed versions" in err["message"]
    err = error_of(ready.json("put", "pics", "a", "--if-none-match", input=b"2", code=5))
    assert "--if-match" in err["hint"]
    err = error_of(ready.json("put", "pics", "a", "--if-match", "0" * 64, input=b"2", code=5))
    assert "mybucket head" in err["hint"]


def test_verify_human_output_names_objects(ready):
    data = os.urandom(300_000)
    ready.ok("put", "pics", "big.bin", input=data)
    digest = hashlib.sha256(data).hexdigest()
    blob = ready.store / "buckets" / "pics" / "blobs" / digest[:2] / digest[2:4] / digest
    raw = bytearray(blob.read_bytes())
    raw[10] ^= 0xFF
    blob.write_bytes(bytes(raw))
    proc = ready.run("verify")
    assert proc.returncode == 1
    out = proc.stdout.decode()
    assert f"- big.bin (version 1, current): its stored file blobs/{digest[:2]}/{digest[2:4]}/{digest} is damaged" in out
    assert "{" not in out and "'versions'" not in out
    assert "Verify found damage in 1 bucket: pics." in proc.stderr.decode()
    # Putting the original again repairs it.
    proc = ready.ok("put", "pics", "big.bin", input=data)
    assert b"repaired" in proc.stdout
    ready.ok("verify")


def test_stats_and_plurals(ready, tmp_path):
    ready.ok("put", "pics", "a", input=b"x")
    human = ready.ok("stats").stdout.decode()
    assert "1 object," in human and "at least about 40 KiB" in human and "%" not in human
    out = tmp_path / "out"
    assert "Exported 1 object " in ready.ok("export", "pics", str(out)).stdout.decode()
    src = tmp_path / "src"
    src.mkdir()
    (src / "one.txt").write_bytes(b"1")
    assert "Imported 1 file " in ready.ok("import", "pics", str(src)).stdout.decode()
