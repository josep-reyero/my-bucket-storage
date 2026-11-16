"""export_dir / import_dir: plain files out, plain files in, and the safety rules around them."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from mybucket import open_store
from mybucket import transfer
from mybucket.errors import (
    DamagedError,
    InvalidArgument,
    InvalidKey,
    NotFound,
    ReadOnlyError,
)
from mybucket.transfer import MANIFEST_NAME, ExportConflict, export_dir, import_dir


def tree(root: Path) -> dict[str, bytes]:
    """Every regular file under root (relative posix path -> bytes), hidden files excluded."""
    out = {}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if any(part.startswith(".") for part in rel.split("/")):
            continue
        if path.is_file() and not path.is_symlink():
            out[rel] = path.read_bytes()
    return out


def hidden_temps(root: Path) -> list[Path]:
    return [p for p in root.rglob(".mybucket-tmp-*")]


def manifest(root: Path) -> tuple[dict, dict[str, dict]]:
    lines = (root / MANIFEST_NAME).read_text().splitlines()
    header = json.loads(lines[0])
    return header, {e["key"]: e for e in map(json.loads, lines[1:])}


@pytest.fixture
def filled(bucket):
    bucket.put("notes/a.txt", b"hello\n", content_type="text/plain", metadata={"by": "test"})
    bucket.put("notes/deep/b.json", b'{"x": 1}' * 500)
    bucket.put("big.bin", os.urandom(300_000))  # stored as a blob file
    bucket.put("top", b"top-level")
    return bucket


# -- export ----------------------------------------------------------------------


def test_export_writes_every_object_at_its_key_path(filled, tmp_path):
    dest = tmp_path / "out"
    result = export_dir(filled, dest)
    expected = {k: filled.get(k) for k in filled.list().keys}
    assert tree(dest) == expected
    assert result["written"] == 4 and result["unchanged"] == 0 and result["skipped"] == 0
    assert result["bytes"] == sum(len(v) for v in expected.values()) == result["written_bytes"]
    # modification times follow the objects
    info = filled.head("notes/a.txt")
    assert (dest / "notes/a.txt").stat().st_mtime_ns == info.modified * 1_000_000
    assert not hidden_temps(dest)


def test_export_manifest_records_what_files_cannot(filled, tmp_path):
    dest = tmp_path / "out"
    export_dir(filled, dest)
    header, entries = manifest(dest)
    assert header["format"] == "mybucket-export" and header["version"] == 1 and header["bucket"] == filled.name
    entry = entries["notes/a.txt"]
    assert entry["content_type"] == "text/plain"
    assert entry["metadata"] == {"by": "test"}
    assert entry["etag"] == hashlib.sha256(b"hello\n").hexdigest()
    assert set(entries) == set(filled.list().keys)


def test_export_again_reports_unchanged(filled, tmp_path):
    dest = tmp_path / "out"
    export_dir(filled, dest)
    again = export_dir(filled, dest)
    assert again["written"] == 0 and again["unchanged"] == 4 and again["written_bytes"] == 0


def test_export_with_prefix_keeps_full_key_paths(filled, tmp_path):
    dest = tmp_path / "out"
    result = export_dir(filled, dest, prefix="notes/")
    assert set(tree(dest)) == {"notes/a.txt", "notes/deep/b.json"}
    assert result["objects"] == 2


def test_export_streams_large_objects(bucket, tmp_path):
    data = (b"0123456789abcdef" * 65536 * 6) + os.urandom(1000)  # > 4 MiB, streamed path
    bucket.put("large/one.dat", iter([data[i : i + 1_000_000] for i in range(0, len(data), 1_000_000)]))
    export_dir(bucket, tmp_path / "out")
    assert (tmp_path / "out/large/one.dat").read_bytes() == data


def test_existing_different_file_is_a_conflict_unless_overwrite(filled, tmp_path):
    dest = tmp_path / "out"
    (dest / "notes").mkdir(parents=True)
    (dest / "notes/a.txt").write_bytes(b"mine, do not touch")
    with pytest.raises(ExportConflict) as err:
        export_dir(filled, dest)
    assert "notes/a.txt" in err.value.message
    assert err.value.exit_code == 5
    assert (dest / "notes/a.txt").read_bytes() == b"mine, do not touch"
    assert (dest / "top").read_bytes() == b"top-level"  # the rest was exported

    result = export_dir(filled, dest, overwrite=True)
    assert (dest / "notes/a.txt").read_bytes() == b"hello\n"
    assert result["written"] == 1 and result["unchanged"] == 3


def test_skip_conflicts_reports_instead_of_raising(filled, tmp_path):
    dest = tmp_path / "out"
    dest.mkdir()
    (dest / "top").mkdir()  # a folder where a file must go: never replaced
    result = export_dir(filled, dest, skip_conflicts=True, overwrite=True)
    assert result["skipped"] == 1
    assert result["conflicts"][0]["key"] == "top"
    assert "folder" in result["conflicts"][0]["reason"]
    assert (dest / "top").is_dir()


def test_key_that_is_also_a_folder_is_refused_before_writing(bucket, tmp_path):
    bucket.put("a", b"file a")
    bucket.put("a/b", b"file a/b")
    bucket.put("c", b"file c")
    dest = tmp_path / "out"
    with pytest.raises(ExportConflict) as err:
        export_dir(bucket, dest)
    assert "Nothing was exported" in err.value.message
    assert "'a'" in err.value.message
    assert tree(dest) == {}
    assert not (dest / MANIFEST_NAME).exists()

    result = export_dir(bucket, dest, skip_conflicts=True)
    assert tree(dest) == {"a/b": b"file a/b", "c": b"file c"}
    assert [c["key"] for c in result["conflicts"]] == ["a"]
    assert "both a file and a folder" in result["conflicts"][0]["reason"]


def test_case_conflicts_detected_on_case_insensitive_filesystems():
    keys = ["Docs/x", "docs/y", "A.txt", "a.txt", "fine"]
    assert transfer._key_conflicts(keys, case_insensitive=False) == {}
    found = transfer._key_conflicts(keys, case_insensitive=True)
    assert set(found) == {"Docs/x", "docs/y", "A.txt", "a.txt"}
    assert "letter case" in found["A.txt"]


def test_case_conflicts_end_to_end(bucket, tmp_path):
    bucket.put("Read.me", b"1")
    bucket.put("read.me", b"2")
    dest = tmp_path / "out"
    dest.mkdir()
    root_fd = os.open(dest, os.O_RDONLY)
    try:
        insensitive = transfer._case_insensitive(root_fd)
    finally:
        os.close(root_fd)
    if insensitive:
        with pytest.raises(ExportConflict):
            export_dir(bucket, dest)
        assert tree(dest) == {}
    else:
        export_dir(bucket, dest)
        assert tree(dest) == {"Read.me": b"1", "read.me": b"2"}


def test_symlinked_folder_in_destination_is_never_followed(bucket, tmp_path):
    bucket.put("a/x.txt", b"payload")
    outside = tmp_path / "outside"
    outside.mkdir()
    dest = tmp_path / "out"
    dest.mkdir()
    (dest / "a").symlink_to(outside)
    for overwrite in (False, True):
        with pytest.raises(ExportConflict):
            export_dir(bucket, dest, overwrite=overwrite)
        assert list(outside.iterdir()) == []
    result = export_dir(bucket, dest, skip_conflicts=True)
    assert "symbolic link" in result["conflicts"][0]["reason"]
    assert list(outside.iterdir()) == []


def test_symlinked_file_in_destination_is_replaced_not_written_through(bucket, tmp_path):
    bucket.put("x.txt", b"new content")
    target = tmp_path / "precious.txt"
    target.write_bytes(b"precious")
    dest = tmp_path / "out"
    dest.mkdir()
    (dest / "x.txt").symlink_to(target)
    with pytest.raises(ExportConflict):
        export_dir(bucket, dest)
    export_dir(bucket, dest, overwrite=True)
    assert not (dest / "x.txt").is_symlink()
    assert (dest / "x.txt").read_bytes() == b"new content"
    assert target.read_bytes() == b"precious"


def test_destination_must_be_outside_the_store(filled, store, tmp_path):
    with pytest.raises(InvalidArgument, match="inside the store"):
        export_dir(filled, store.path / "exported")
    with pytest.raises(InvalidArgument, match="contains the store"):
        export_dir(filled, store.path.parent)
    a_file = tmp_path / "file"
    a_file.write_bytes(b"x")
    with pytest.raises(InvalidArgument, match="is a file"):
        export_dir(filled, a_file)


def _damage_blob_file(bucket, key: str) -> None:
    etag = bucket.head(key).etag
    path = bucket.blobs_dir / etag[:2] / etag[2:4] / etag
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.chmod(0o600)
    path.write_bytes(bytes(data))


def test_damaged_object_is_not_written_and_the_rest_is(filled, tmp_path):
    _damage_blob_file(filled, "big.bin")
    dest = tmp_path / "out"
    with pytest.raises(DamagedError) as err:
        export_dir(filled, dest)
    assert "big.bin" in err.value.message
    assert "big.bin" not in tree(dest)
    assert set(tree(dest)) == {"notes/a.txt", "notes/deep/b.json", "top"}
    assert not hidden_temps(dest)
    _, entries = manifest(dest)
    assert "big.bin" not in entries


def test_object_deleted_during_export_counts_as_gone(filled, tmp_path, monkeypatch):
    real_open = filled.open

    def flaky_open(key):
        if key == "top":
            filled.delete("top")
        return real_open(key)

    monkeypatch.setattr(filled, "open", flaky_open)
    result = export_dir(filled, tmp_path / "out")
    assert result["gone"] == 1 and result["written"] == 3


def test_export_from_read_only_store(filled, store_path, tmp_path):
    ro = open_store(store_path, readonly=True)
    try:
        result = export_dir(ro.bucket(filled.name), tmp_path / "out")
    finally:
        ro.close()
    assert result["written"] == 4


# -- import ----------------------------------------------------------------------


def make_tree(root: Path, files: dict[str, bytes]) -> Path:
    for rel, data in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return root


def test_import_loads_every_file_and_is_idempotent(bucket, tmp_path):
    src = make_tree(tmp_path / "src", {"a.txt": b"A", "dir/b.json": b"{}", "dir/sub/c.bin": os.urandom(200_000)})
    first = import_dir(bucket, src)
    assert (first["files"], first["added"], first["updated"], first["unchanged"], first["skipped"]) == (3, 3, 0, 0, 0)
    assert first["bytes"] == first["new_bytes"] == 1 + 2 + 200_000
    assert bucket.get("dir/sub/c.bin") == (src / "dir/sub/c.bin").read_bytes()
    versions = {o.key: o.version for o in bucket.list().objects}

    second = import_dir(bucket, src)
    assert (second["added"], second["updated"], second["unchanged"]) == (0, 0, 3)
    assert second["new_bytes"] == 0
    assert {o.key: o.version for o in bucket.list().objects} == versions  # nothing rewritten

    (src / "a.txt").write_bytes(b"changed")
    third = import_dir(bucket, src)
    assert (third["added"], third["updated"], third["unchanged"]) == (0, 1, 2)
    assert bucket.get("a.txt") == b"changed"
    assert len(bucket.list_versions("a.txt")) == 2  # the old version went to the trash


def test_import_same_size_different_content_is_updated(bucket, tmp_path):
    src = make_tree(tmp_path / "src", {"k": b"aaaa"})
    import_dir(bucket, src)
    (src / "k").write_bytes(b"bbbb")
    assert import_dir(bucket, src)["updated"] == 1
    assert bucket.get("k") == b"bbbb"


def test_import_prefix(bucket, tmp_path):
    src = make_tree(tmp_path / "src", {"x/y.txt": b"1"})
    import_dir(bucket, src, prefix="backup/2026")
    import_dir(bucket, src, prefix="backup/2026/")  # same thing
    assert bucket.list().keys == ["backup/2026/x/y.txt"]
    assert bucket.list_versions("backup/2026/x/y.txt")[0].version == 1
    for bad in ("../up", "/abs", "a//b", ".hidden"):
        with pytest.raises(InvalidKey):
            import_dir(bucket, src, prefix=bad)


def test_import_skips_what_cannot_be_a_key(bucket, tmp_path):
    src = make_tree(
        tmp_path / "src",
        {
            "ok.txt": b"ok",
            ".DS_Store": b"junk",
            "has space.txt": b"x",
            ".git/config": b"secret-ish",
            "bad dir/inner.txt": b"x",
        },
    )
    result = import_dir(bucket, src)
    assert bucket.list().keys == ["ok.txt"]
    reasons = {e["path"]: e["reason"] for e in result["skipped_entries"]}
    assert set(reasons) == {".DS_Store", "has space.txt", ".git/", "bad dir/"}
    assert "everything in it was skipped" in reasons[".git/"]
    assert result["skipped"] == 4


def test_import_never_follows_symlinks(bucket, tmp_path):
    outside = make_tree(tmp_path / "outside", {"secret.txt": b"do not import", "d/s.txt": b"nor this"})
    src = make_tree(tmp_path / "src", {"real.txt": b"real"})
    (src / "link.txt").symlink_to(outside / "secret.txt")
    (src / "linkdir").symlink_to(outside / "d")
    result = import_dir(bucket, src)
    assert bucket.list().keys == ["real.txt"]
    reasons = {e["path"]: e["reason"] for e in result["skipped_entries"]}
    assert "symbolic link" in reasons["link.txt"] and "symbolic link" in reasons["linkdir"]


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs mkfifo")
def test_import_skips_special_files(bucket, tmp_path):
    src = make_tree(tmp_path / "src", {"a": b"a"})
    os.mkfifo(src / "pipe")
    result = import_dir(bucket, src)
    assert bucket.list().keys == ["a"]
    assert result["skipped_entries"] == [{"path": "pipe", "reason": "it is not a regular file"}]


def test_export_then_import_round_trips_content_type_and_metadata(filled, store, tmp_path):
    dest = tmp_path / "out"
    export_dir(filled, dest)
    other = store.create_bucket("copy-of-it")
    result = import_dir(other, dest)
    assert result["added"] == 4 and result["skipped"] == 0  # the manifest is not imported
    for obj in filled.list().objects:
        copy = other.head(obj.key)
        assert (copy.etag, copy.content_type, copy.metadata) == (obj.etag, obj.content_type, obj.metadata)
    assert import_dir(other, dest)["unchanged"] == 4


def test_import_guesses_types_unless_told_not_to(bucket, store, tmp_path):
    src = make_tree(tmp_path / "src", {"d.json": b"{}", "p.png": b"\x89PNG", "noext": b"?", "x.json.gz": b"z"})
    import_dir(bucket, src)
    assert bucket.head("d.json").content_type == "application/json"
    assert bucket.head("p.png").content_type == "image/png"
    assert bucket.head("noext").content_type is None
    assert bucket.head("x.json.gz").content_type is None
    plain = store.create_bucket("plain")
    import_dir(plain, src, guess_types=False)
    assert plain.head("d.json").content_type is None


def test_import_keeps_existing_content_type_and_metadata(bucket, tmp_path):
    bucket.put("doc.json", b"old", content_type="application/vnd.custom", metadata={"app": "x"})
    src = make_tree(tmp_path / "src", {"doc.json": b"old"})
    assert import_dir(bucket, src)["unchanged"] == 1
    (src / "doc.json").write_bytes(b"new")
    assert import_dir(bucket, src)["updated"] == 1
    info = bucket.head("doc.json")
    assert (info.content_type, info.metadata) == ("application/vnd.custom", {"app": "x"})


def test_import_commits_in_batches(bucket, tmp_path, monkeypatch):
    monkeypatch.setattr(transfer, "_BATCH_FILES", 7)
    files = {f"f/{i:03d}.txt": f"file {i}".encode() for i in range(30)}
    src = make_tree(tmp_path / "src", files)
    result = import_dir(bucket, src)
    assert result["added"] == 30
    assert {k: bucket.get(k) for k in bucket.list().keys} == files


def test_import_failure_in_a_batch_applies_nothing_of_that_batch(bucket, tmp_path, monkeypatch):
    src = make_tree(tmp_path / "src", {"a": b"a", "b": b"b"})
    real_flush = transfer._Importer.flush

    def broken_flush(self):
        if self.batch is not None:
            raise OSError(5, "simulated failure")
        return real_flush(self)

    monkeypatch.setattr(transfer._Importer, "flush", broken_flush)
    with pytest.raises(OSError):
        import_dir(bucket, src)
    assert bucket.list().keys == []
    assert list(bucket.tmp_dir.iterdir()) == []


def test_import_refuses_bad_sources(bucket, store, tmp_path):
    with pytest.raises(NotFound):
        import_dir(bucket, tmp_path / "missing")
    a_file = tmp_path / "file"
    a_file.write_bytes(b"x")
    with pytest.raises(InvalidArgument, match="not a folder"):
        import_dir(bucket, a_file)
    with pytest.raises(InvalidArgument, match="contains the store"):
        import_dir(bucket, store.path.parent)
    with pytest.raises(InvalidArgument, match="inside the store"):
        import_dir(bucket, store.path / "buckets")


def test_import_into_read_only_store(bucket, store_path, tmp_path):
    src = make_tree(tmp_path / "src", {"a": b"a"})
    ro = open_store(store_path, readonly=True)
    try:
        with pytest.raises(ReadOnlyError):
            import_dir(ro.bucket(bucket.name), src)
    finally:
        ro.close()


def test_hand_edited_manifest_with_bad_values_falls_back(bucket, tmp_path):
    src = make_tree(tmp_path / "src", {"a.txt": b"a"})
    header = {"format": "mybucket-export", "version": 1}
    entry = {"key": "a.txt", "content_type": "bad\ntype", "metadata": {"ok": "fine"}}
    (src / MANIFEST_NAME).write_text(json.dumps(header) + "\n" + json.dumps(entry) + "\n")
    import_dir(bucket, src)
    assert bucket.head("a.txt").content_type == "text/plain"


def test_unreadable_manifest_is_ignored(bucket, tmp_path):
    src = make_tree(tmp_path / "src", {"a.txt": b"a"})
    (src / MANIFEST_NAME).write_text("not json\n")
    result = import_dir(bucket, src)
    assert bucket.list().keys == ["a.txt"]
    assert result["skipped"] == 0  # export's own file name is never an object
    assert bucket.head("a.txt").content_type == "text/plain"


def test_export_import_database_untouched_by_reads(filled, tmp_path):
    """Exporting reads only: the bucket's version counter does not move."""
    conn = sqlite3.connect(filled.db_path)
    before = conn.execute("SELECT v FROM meta WHERE k = 'last_version'").fetchone()
    export_dir(filled, tmp_path / "out")
    assert conn.execute("SELECT v FROM meta WHERE k = 'last_version'").fetchone() == before
    conn.close()


# -- regressions found in review -------------------------------------------------------


def test_missing_blob_file_does_not_stop_the_export(filled, tmp_path):
    etag = filled.head("big.bin").etag
    (filled.blobs_dir / etag[:2] / etag[2:4] / etag).unlink()
    dest = tmp_path / "out"
    with pytest.raises(DamagedError) as err:
        export_dir(filled, dest)
    assert "big.bin" in err.value.message
    assert set(tree(dest)) == {"notes/a.txt", "notes/deep/b.json", "top"}
    assert "big.bin" not in manifest(dest)[1]


def test_import_with_prefix_still_uses_the_manifest(filled, store, tmp_path):
    dest = tmp_path / "out"
    export_dir(filled, dest)
    other = store.create_bucket("restored")
    import_dir(other, dest, prefix="restore")
    info = other.head("restore/notes/a.txt")
    assert (info.content_type, info.metadata) == ("text/plain", {"by": "test"})


def test_import_from_a_symlinked_source_folder(bucket, tmp_path):
    real = make_tree(tmp_path / "real", {"x.txt": b"x"})
    link = tmp_path / "link"
    link.symlink_to(real)
    result = import_dir(bucket, link)
    assert result["added"] == 1 and bucket.list().keys == ["x.txt"]


def test_importing_an_empty_export_skips_nothing(bucket, store, tmp_path):
    empty = store.create_bucket("empty")
    export_dir(empty, tmp_path / "out")
    result = import_dir(bucket, tmp_path / "out")
    assert (result["files"], result["skipped"]) == (0, 0)


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root ignores permissions")
def test_permission_problems_are_per_key_conflicts(bucket, tmp_path):
    bucket.put("locked/a.txt", b"a")
    bucket.put("open/b.txt", b"b")
    dest = tmp_path / "out"
    (dest / "locked").mkdir(parents=True)
    (dest / "locked").chmod(0o500)
    try:
        result = export_dir(bucket, dest, skip_conflicts=True)
    finally:
        (dest / "locked").chmod(0o700)
    assert (dest / "open/b.txt").read_bytes() == b"b"
    assert [c["key"] for c in result["conflicts"]] == ["locked/a.txt"]
    assert "cannot be written" in result["conflicts"][0]["reason"]
