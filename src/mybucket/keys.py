"""Validation of every name and value that comes from outside.

This module is the only place where keys, bucket names, prefixes, metadata,
content types and ETags are checked. Every entry point (SDK, CLI, import,
export) must call these functions instead of re-implementing the rules.

Key rules (v1):
    ASCII, 1-512 bytes, split into segments on "/". Each segment is 1-255
    bytes of ``[A-Za-z0-9._~@+=,-]``, is never "." or "..", and never starts
    with ".". No leading or trailing "/" and no empty segments. Because of
    these rules a valid key is always a safe relative file path on POSIX
    systems: it cannot escape a directory, cannot name a hidden file and needs
    no URL encoding.

Public:
    validate_key(key) -> str
    validate_bucket_name(name) -> str
    is_valid_key(key) -> bool
    is_valid_bucket_name(name) -> bool

Internal for siblings:
    validate_prefix(prefix) -> str        list() prefixes, export/import prefixes
    validate_delimiter(delimiter) -> str
    validate_metadata(metadata) -> str | None   canonical JSON or None when empty
    parse_metadata(text) -> dict[str, str]      inverse of validate_metadata
    validate_content_type(value) -> str | None
    normalize_etag(value, allow_star=True) -> str
    key_segments(key) -> list[str]        validated segments, for building paths
    MAX_KEY_BYTES, MAX_SEGMENT_BYTES, MAX_METADATA_BYTES, SEGMENT_CHARS
"""

from __future__ import annotations

import json
import re

from .errors import InvalidArgument, InvalidBucketName, InvalidKey, InvalidMetadata

MAX_KEY_BYTES = 512
MAX_SEGMENT_BYTES = 255
MAX_METADATA_BYTES = 2048
MAX_METADATA_NAME = 64
MAX_CONTENT_TYPE = 255

#: Characters allowed inside one key segment.
SEGMENT_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._~@+=,-")

_SEGMENT_RE = re.compile(r"[A-Za-z0-9._~@+=,\-]{1,255}")
_BUCKET_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,62}")
_PREFIX_RE = re.compile(r"[A-Za-z0-9._~@+=,\-/]*")
_META_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_ETAG_RE = re.compile(r"[0-9a-f]{64}")
# type "/" subtype, then optional parameters (RFC 9110 token characters).
_MIME_RE = re.compile(r"[A-Za-z0-9!#$%&'*+.^_`|~-]+/[A-Za-z0-9!#$%&'*+.^_`|~-]+(\s*;.*)?")


def _show(value: object) -> str:
    text = repr(value)
    return text if len(text) <= 80 else text[:77] + "..."


def key_segments(key: object) -> list[str]:
    """Validate ``key`` and return its segments. Raises InvalidKey."""
    if not isinstance(key, str):
        raise InvalidKey(f"Key must be a string, got {type(key).__name__}.")
    if not key:
        raise InvalidKey("Key is empty.")
    if not key.isascii():
        raise InvalidKey(f"Key {_show(key)} contains non-ASCII characters.")
    if len(key) > MAX_KEY_BYTES:
        raise InvalidKey(f"Key is {len(key)} bytes long; the limit is {MAX_KEY_BYTES}.")
    segments = key.split("/")
    for seg in segments:
        if not seg:
            raise InvalidKey(
                f"Key {_show(key)} has an empty segment (a leading, trailing or doubled '/')."
            )
        if len(seg) > MAX_SEGMENT_BYTES:
            raise InvalidKey(
                f"Key {_show(key)} has a segment of {len(seg)} bytes; the limit is {MAX_SEGMENT_BYTES}."
            )
        if seg.startswith("."):
            raise InvalidKey(f"Key {_show(key)} has a segment starting with '.' ({_show(seg)}).")
        if not _SEGMENT_RE.fullmatch(seg):
            bad = sorted({c for c in seg if c not in SEGMENT_CHARS})
            raise InvalidKey(f"Key {_show(key)} contains characters that are not allowed: {_show(''.join(bad))}.")
    return segments


def validate_key(key: object) -> str:
    """Return ``key`` unchanged if it is a valid object key; raise InvalidKey otherwise."""
    key_segments(key)
    return key  # type: ignore[return-value]


def is_valid_key(key: object) -> bool:
    try:
        key_segments(key)
    except InvalidKey:
        return False
    return True


def validate_bucket_name(name: object) -> str:
    """Return ``name`` if it matches ^[a-z0-9][a-z0-9-]{1,62}$; raise InvalidBucketName otherwise."""
    if not isinstance(name, str) or not name.isascii() or not _BUCKET_RE.fullmatch(name):
        raise InvalidBucketName(f"{_show(name)} is not a valid bucket name.")
    return name


def is_valid_bucket_name(name: object) -> bool:
    try:
        validate_bucket_name(name)
    except InvalidBucketName:
        return False
    return True


def validate_prefix(prefix: object) -> str:
    """A list prefix: any leading part of a key ("" allowed), same characters as keys plus "/"."""
    if prefix is None:
        return ""
    if not isinstance(prefix, str):
        raise InvalidKey(f"Prefix must be a string, got {type(prefix).__name__}.")
    if len(prefix) > MAX_KEY_BYTES or not prefix.isascii() or not _PREFIX_RE.fullmatch(prefix):
        raise InvalidKey(
            f"Prefix {_show(prefix)} can never match a key.",
            hint="A prefix may only contain the characters allowed in keys, and '/'.",
        )
    return prefix


def validate_delimiter(delimiter: object) -> str:
    if not isinstance(delimiter, str) or not delimiter or len(delimiter) > 32 or not delimiter.isascii():
        raise InvalidArgument(
            f"Delimiter {_show(delimiter)} is not valid.",
            hint="Use a short ASCII string such as '/'.",
        )
    return delimiter


def validate_metadata(metadata: object) -> str | None:
    """Validate a user metadata map and return its canonical JSON text (None when empty).

    Names: 1-64 characters of [A-Za-z0-9._-], starting with a letter or digit.
    Values: strings. The canonical JSON (sorted keys, compact, UTF-8) must be at
    most 2048 bytes. Canonical text makes "same metadata" a string comparison.
    """
    if metadata is None:
        return None
    if not isinstance(metadata, dict):
        raise InvalidMetadata(
            f"Metadata must be a dict of strings, got {type(metadata).__name__}.",
            hint="Pass something like {'source': 'tracker'}.",
        )
    if not metadata:
        return None
    for name, value in metadata.items():
        if not isinstance(name, str) or not _META_NAME_RE.fullmatch(name):
            raise InvalidMetadata(
                f"Metadata name {_show(name)} is not allowed.",
                hint="Names are 1-64 characters of letters, digits, '.', '_' and '-', starting with a letter or digit.",
            )
        if not isinstance(value, str):
            raise InvalidMetadata(
                f"Metadata value for {name!r} must be a string, got {type(value).__name__}.",
                hint="Convert numbers and dates to strings before storing them.",
            )
    text = json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    try:
        size = len(text.encode("utf-8"))
    except UnicodeEncodeError:
        raise InvalidMetadata(
            "Metadata values must be valid text; one contains a lone surrogate character.",
            hint="Decode bytes with errors='replace' (or similar) before putting them in metadata.",
        ) from None
    if size > MAX_METADATA_BYTES:
        raise InvalidMetadata(
            f"Metadata is {size} bytes as JSON; the limit is {MAX_METADATA_BYTES}.",
            hint="Keep metadata small; store larger data as its own object.",
        )
    return text


def parse_metadata(text: str | None) -> dict[str, str]:
    return {} if not text else json.loads(text)


def validate_content_type(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > MAX_CONTENT_TYPE:
        raise InvalidArgument(
            f"Content type {_show(value)} is not valid.",
            hint="Use a short MIME type such as 'application/json', or leave it out.",
        )
    if not all(0x20 <= ord(c) <= 0x7E for c in value):
        raise InvalidArgument(
            f"Content type {_show(value)} contains characters that are not printable ASCII.",
            hint="Use a MIME type such as 'text/plain; charset=utf-8'.",
        )
    if not _MIME_RE.fullmatch(value):
        raise InvalidArgument(
            f"Content type {_show(value)} does not look like a MIME type (type/subtype).",
            hint="Use something like 'application/json' or 'image/jpeg', or leave it out.",
        )
    return value


def normalize_etag(value: object, allow_star: bool = True) -> str:
    """Normalise an ETag given by a caller: strips quotes and whitespace, lowercases.

    Returns 64 lowercase hex characters, or "*" when ``allow_star``.
    """
    if not isinstance(value, str):
        raise InvalidArgument(
            f"ETag must be a string, got {type(value).__name__}.",
            hint="Pass the 64-character hex string from info.etag.",
        )
    text = value.strip()
    if text.startswith("W/"):
        text = text[2:]
    if len(text) >= 2 and text[0] == text[-1] == '"':
        text = text[1:-1]
    text = text.lower()
    if text == "*" and allow_star:
        return text
    if not _ETAG_RE.fullmatch(text):
        raise InvalidArgument(
            f"{_show(value)} is not a valid ETag.",
            hint="An ETag is the 64-character SHA-256 hex that put() returns and head() (or "
            "'mybucket head') shows" + (", or '*'." if allow_star else "."),
        )
    return text
