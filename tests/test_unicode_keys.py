"""Keys with spaces and UTF-8: normalisation, order, prefixes, and export/import on real disks."""

from __future__ import annotations

import hashlib
import os
import unicodedata

import pytest

from mybucket import ExportConflict, InvalidKey, open_store
from mybucket import transfer as transfer_mod
from mybucket.keys import fold_path, validate_key

KEYS = [
    "summer trip/IMG 1234.jpg",
    "café/menu du jour.txt",
    "日本語/ファイル.txt",
    "emoji/👩‍👩‍👧 family.png",  # ZERO WIDTH JOINER sequences are allowed
    "Ελληνικά/αρχείο.md",
    "math/∑ x² ≤ ∞.txt",
    "symbols/100% #1 (final) [v2] {a} 'q' \"dq\" ? * : | <>.txt",
    "ß/straße.txt",
]


def test_engine_round_trip(bucket):
    for i, key in enumerate(KEYS):
        info = bucket.put(key, key.encode() * (i + 1))
        assert info.key == key
    for i, key in enumerate(KEYS):
        assert bucket.get(key) == key.encode() * (i + 1)
        assert bucket.head(key).key == key
    # Listing is in UTF-8 byte order, which is code point order.
    assert bucket.list().keys == sorted(KEYS, key=lambda k: k.encode())
    assert bucket.list(prefix="café/").keys == ["café/menu du jour.txt"]
    assert bucket.list(prefix="caf").keys == ["café/menu du jour.txt"]
    assert "日本語/" in bucket.list(delimiter="/").prefixes
    page = bucket.list(limit=3)
    rest = bucket.list(start_after=page.next_start_after)
    assert page.keys + rest.keys == bucket.list().keys


def test_decomposed_input_is_the_same_key(bucket):
    nfd = unicodedata.normalize("NFD", "café/menu.txt")
    assert nfd != "café/menu.txt"
    info = bucket.put(nfd, b"one")
    assert info.key == "café/menu.txt"
    assert bucket.put("café/menu.txt", b"two").status == "updated"
    assert bucket.get(nfd) == b"two" and bucket.exists(nfd)
    assert bucket.list(prefix=unicodedata.normalize("NFD", "café/")).keys == ["café/menu.txt"]
    assert bucket.list(start_after=unicodedata.normalize("NFD", "café")).keys == ["café/menu.txt"]
    bucket.delete(nfd)
    assert not bucket.exists("café/menu.txt")
    assert [v.key for v in bucket.list_versions(nfd)] == ["café/menu.txt", "café/menu.txt"]


@pytest.mark.parametrize(
    "key",
    [
        " leading.txt",
        "trailing.txt ",
        "dir /file.txt",
        "dir/ file.txt",
        "tab\there",
        "new\nline",
        " nbsp-leading",  # NO-BREAK SPACE counts as space at the ends
        "rtl\u202eoverride.txt",
        "invisible\u200b.txt",
        "bom\ufeff.txt",
        "line\u2028sep",
        "back\\slash",
        "c1\u0085control",
        "lone\ud800surrogate",
        ".hidden",
        "a/../b",
        "x" * 513,
        "é" * 256,  # 512 bytes in one segment: over 255
    ],
)
def test_unsafe_keys_are_refused_with_a_plain_message(bucket, key):
    with pytest.raises(InvalidKey) as err:
        bucket.put(key, b"x")
    assert err.value.exit_code == 2
    # The message never echoes the invisible or control character itself (it could rewrite the terminal line).
    assert not any(c in err.value.message for c in "\n\t\u202e\u200b\ufeff\u2028\u0085\ud800")


def test_length_is_counted_in_utf8_bytes():
    assert validate_key("é" * 127 + "/" + "é" * 127) == "é" * 127 + "/" + "é" * 127
    with pytest.raises(InvalidKey):
        validate_key("/".join(["é" * 127] * 3))  # 764 bytes


def test_fold_path_matches_case_insensitive_disks():
    assert fold_path("É/Straße.TXT") == fold_path("é/strasse.txt")
    assert fold_path("Ǆ") == fold_path("ǆ")
    assert fold_path("A") != fold_path("B")


def _really_case_insensitive(path) -> bool:
    probe = path / "CaseProbe"
    probe.write_bytes(b"")
    try:
        return (path / "caseprobe").exists()
    finally:
        probe.unlink()


def test_export_import_round_trip(bucket, tmp_path):
    for key in KEYS:
        bucket.put(key, hashlib.sha256(key.encode()).digest() * 10)
    out = tmp_path / "export"
    report = bucket.export_dir(out)
    assert report["written"] == len(KEYS)
    for key in KEYS:
        assert (out / key).read_bytes() == bucket.get(key)
    other = bucket.store.bucket("copy", create=True)
    imported = other.import_dir(out)
    assert imported["added"] == len(KEYS) and imported["skipped"] == 0
    assert other.list().keys == bucket.list().keys
    assert [o.etag for o in other.list().objects] == [o.etag for o in bucket.list().objects]


def test_export_refuses_case_clashes_on_a_case_insensitive_disk(bucket, tmp_path, monkeypatch):
    monkeypatch.setattr(transfer_mod, "_case_insensitive", lambda fd: True)
    bucket.put("Été/a.txt", b"upper")
    bucket.put("été/b.txt", b"lower")
    bucket.put("STRASSE.txt", b"ss")
    bucket.put("straße.txt", b"sz")
    bucket.put("plain.txt", b"fine")
    with pytest.raises(ExportConflict) as err:
        bucket.export_dir(tmp_path / "out")
    assert "differs only in letter case" in err.value.message
    assert not (tmp_path / "out" / "plain.txt").exists()  # nothing was exported
    report = bucket.export_dir(tmp_path / "out", skip_conflicts=True)
    assert report["written"] == 1 and report["skipped"] == 4


def test_export_never_writes_two_keys_into_one_file(bucket, tmp_path, monkeypatch):
    """If the case check is wrong (it guessed case-sensitive), the inode guard still stops the overwrite."""
    out = tmp_path / "out"
    out.mkdir()
    if not _really_case_insensitive(out):
        pytest.skip("needs a case-insensitive filesystem (the macOS default)")
    monkeypatch.setattr(transfer_mod, "_case_insensitive", lambda fd: False)
    bucket.put("Photo.jpg", b"first")
    bucket.put("photo.jpg", b"second")
    bucket.put("Dir/a.txt", b"a")
    bucket.put("dir/b.txt", b"b")
    with pytest.raises(ExportConflict):
        bucket.export_dir(out, overwrite=True)
    files = {p.name: p.read_bytes() for p in out.rglob("*") if p.is_file() and not p.name.startswith(".")}
    # Each file on disk holds exactly one key's content: nothing was silently replaced.
    assert files.get("Photo.jpg", files.get("photo.jpg")) == b"first"
    report = bucket.export_dir(tmp_path / "out2", skip_conflicts=True)
    assert report["skipped"] == 2


def test_import_turns_decomposed_names_into_nfc_keys(bucket, tmp_path):
    src = tmp_path / "in"
    (src / unicodedata.normalize("NFD", "Café")).mkdir(parents=True)
    (src / unicodedata.normalize("NFD", "Café") / unicodedata.normalize("NFD", "crème brûlée.txt")).write_bytes(b"dessert")
    (src / "with space.txt").write_bytes(b"space")
    (src / " leading.txt").write_bytes(b"bad")
    (src / "bidi\u202e.txt").write_bytes(b"bad")
    report = bucket.import_dir(src)
    assert report["added"] == 2 and report["skipped"] == 2
    assert sorted(bucket.list().keys) == sorted(["Café/crème brûlée.txt", "with space.txt"])
    assert all(unicodedata.is_normalized("NFC", k) for k in bucket.list().keys)
    assert bucket.import_dir(src)["unchanged"] == 2  # idempotent


def test_import_skips_names_that_are_the_same_key(tmp_path):
    names = ["café.txt", "café.txt", "other.txt"]
    clashes = transfer_mod._same_key_names(names)
    assert sorted(clashes) == sorted(names[:2]) and "Unicode normal form" in clashes[names[0]]
    src = tmp_path / "in"
    src.mkdir()
    for name in names:
        (src / name).write_bytes(name.encode())
    on_disk = os.listdir(src)
    store = open_store(tmp_path / "s")
    b = store.bucket("box", create=True)
    report = b.import_dir(src)
    if len(on_disk) == 3:  # a filesystem that keeps both spellings apart
        assert report["skipped"] == 2 and b.list().keys == ["other.txt"]
    else:  # APFS and HFS+ treat them as one name
        assert report["skipped"] == 0 and b.list().keys == ["café.txt", "other.txt"]
