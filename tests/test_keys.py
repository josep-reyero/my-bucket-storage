"""Key, bucket name, metadata and ETag validation."""

from __future__ import annotations

import posixpath
import string

import pytest
from hypothesis import given
from hypothesis import strategies as st

from mybucket import keys
from mybucket.errors import InvalidArgument, InvalidBucketName, InvalidKey, InvalidMetadata

VALID_KEYS = [
    "a",
    "a/b/c",
    "tracker/days/2026/2026-11-14/day.json",
    "x.y_z~@+=,-",
    "A" * 255,
    "/".join(["a" * 255, "b" * 255]),  # 511 bytes
    "a..b",
    "a/b.",
    "a/b/c.d.e",
]

INVALID_KEYS = [
    "",
    "/a",
    "a/",
    "a//b",
    ".",
    "..",
    "a/./b",
    "a/../b",
    ".hidden",
    "a/.state/token",
    "a b",
    "a\\b",
    "a\x00b",
    "café",
    "a?b",
    "a#b",
    "a%2Fb",
    "a*b",
    "a:b",
    "A" * 256,
    "/".join(["a" * 200] * 3),  # 602 bytes
    "a" * 513,
]


@pytest.mark.parametrize("key", VALID_KEYS)
def test_valid_keys(key):
    assert keys.validate_key(key) == key
    assert keys.is_valid_key(key)


@pytest.mark.parametrize("key", INVALID_KEYS)
def test_invalid_keys(key):
    with pytest.raises(InvalidKey) as err:
        keys.validate_key(key)
    assert err.value.exit_code == 2
    assert err.value.hint
    assert not keys.is_valid_key(key)


@pytest.mark.parametrize("value", [None, 1, b"abc", ["a"]])
def test_non_string_keys(value):
    with pytest.raises(InvalidKey):
        keys.validate_key(value)


def test_key_of_exactly_512_bytes():
    key = "/".join(["a" * 255, "b" * 254, "c"])
    assert len(key) == 512
    keys.validate_key(key)
    with pytest.raises(InvalidKey):
        keys.validate_key(key + "c")


_segment = st.text(alphabet=sorted(keys.SEGMENT_CHARS), min_size=1, max_size=20)
_any_text = st.text(alphabet=string.printable + "é☃", max_size=40)


@given(st.lists(_any_text, min_size=1, max_size=6).map("/".join))
def test_valid_keys_are_always_safe_relative_paths(candidate):
    """Whatever the input, a key that passes validation can never escape a directory."""
    if not keys.is_valid_key(candidate):
        return
    assert not candidate.startswith("/")
    normalized = posixpath.normpath(candidate)
    assert normalized == candidate
    parts = candidate.split("/")
    assert ".." not in parts and "." not in parts
    assert all(not p.startswith(".") for p in parts)
    assert candidate.isascii()
    assert "\\" not in candidate and "\x00" not in candidate


@given(st.lists(_segment, min_size=1, max_size=5))
def test_generated_keys_round_trip(segments):
    key = "/".join(segments)
    if any(s.startswith(".") for s in segments) or len(key) > 512:
        with pytest.raises(InvalidKey):
            keys.validate_key(key)
    else:
        assert keys.validate_key(key) == key
        assert keys.key_segments(key) == segments


@pytest.mark.parametrize("name", ["ab", "photos", "my-app-derived", "a" * 63, "0-0", "9a"])
def test_valid_bucket_names(name):
    assert keys.validate_bucket_name(name) == name


@pytest.mark.parametrize(
    "name", ["", "a", "-ab", "Ab", "a_b", "a.b", "a" * 64, "ab/", "../x", "éa", None, 12]
)
def test_invalid_bucket_names(name):
    with pytest.raises(InvalidBucketName):
        keys.validate_bucket_name(name)


def test_prefixes():
    assert keys.validate_prefix("") == ""
    assert keys.validate_prefix(None) == ""
    assert keys.validate_prefix("a/b/") == "a/b/"
    assert keys.validate_prefix("a/b/pa") == "a/b/pa"
    for bad in ["a b", "café", "a" * 513, 3]:
        with pytest.raises(InvalidKey):
            keys.validate_prefix(bad)


def test_delimiter():
    assert keys.validate_delimiter("/") == "/"
    for bad in ["", None, "x" * 33, "é"]:
        with pytest.raises(InvalidArgument):
            keys.validate_delimiter(bad)


def test_metadata_canonical_form():
    assert keys.validate_metadata(None) is None
    assert keys.validate_metadata({}) is None
    a = keys.validate_metadata({"b": "2", "a": "1"})
    b = keys.validate_metadata({"a": "1", "b": "2"})
    assert a == b == '{"a":"1","b":"2"}'
    assert keys.parse_metadata(a) == {"a": "1", "b": "2"}
    assert keys.parse_metadata(None) == {}
    assert keys.validate_metadata({"note": "café"}) == '{"note":"café"}'


@pytest.mark.parametrize(
    "meta",
    [
        {"": "x"},
        {"-a": "x"},
        {"a b": "x"},
        {"a" * 65: "x"},
        {"a": 1},
        {"a": None},
        {1: "x"},
        {"a": "x" * 2100},
        ["a"],
        "a=b",
    ],
)
def test_invalid_metadata(meta):
    with pytest.raises(InvalidMetadata):
        keys.validate_metadata(meta)


def test_metadata_size_limit_is_on_encoded_bytes():
    ok = {"k": "x" * 2000}
    keys.validate_metadata(ok)
    with pytest.raises(InvalidMetadata):
        keys.validate_metadata({"k": "é" * 1100})  # 2200 UTF-8 bytes


def test_content_type():
    assert keys.validate_content_type(None) is None
    assert keys.validate_content_type("application/json") == "application/json"
    for bad in ["", "a\nb", "x" * 256, 5, "café/x"]:
        with pytest.raises(InvalidArgument):
            keys.validate_content_type(bad)


def test_normalize_etag():
    hexd = "ab" * 32
    assert keys.normalize_etag(hexd) == hexd
    assert keys.normalize_etag(f'"{hexd.upper()}"') == hexd
    assert keys.normalize_etag(f' W/"{hexd}" ') == hexd
    assert keys.normalize_etag("*") == "*"
    with pytest.raises(InvalidArgument):
        keys.normalize_etag("*", allow_star=False)
    for bad in ["", "abc", "g" * 64, None, "ab" * 33]:
        with pytest.raises(InvalidArgument):
            keys.normalize_etag(bad)
