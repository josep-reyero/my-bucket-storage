"""Validation of every name and value that comes from outside.

This module is the only place where keys, bucket names, prefixes, metadata,
content types and ETags are checked. Every entry point (SDK, CLI, import,
export) must call these functions instead of re-implementing the rules.

Key rules (format 2; every v1 key is still valid):
    Text (``str``), normalised to Unicode NFC (``validate_key`` returns the
    normalised form, and that is the key that is stored). 1-512 bytes once
    encoded as UTF-8, split into segments on "/". Each segment is 1-255
    UTF-8 bytes, never starts with "." (so "." and ".." are impossible) and
    never starts or ends with a space (or other whitespace). Spaces inside
    a segment and any other printable character are allowed, except: control
    characters (U+0000-U+001F, U+007F-U+009F), the backslash, invisible
    direction marks and overrides (U+061C, U+200B, U+200E, U+200F,
    U+202A-U+202E, U+2066-U+2069), line and paragraph separators (U+2028,
    U+2029) and U+FEFF. No leading or trailing "/" and no empty segments.
    Because of these rules a valid key is always a safe relative file path on
    POSIX systems: it cannot escape a directory and cannot name a hidden file.
    Keys that differ only in letter case are different keys.

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
    key_segments(key) -> list[str]        validated segments of the NFC key, for building paths
    fold_path(text) -> str                caseless form used to find names that clash on
                                          case-insensitive disks (over-matches on purpose)
    MAX_KEY_BYTES, MAX_SEGMENT_BYTES, MAX_METADATA_BYTES, FORBIDDEN_CHARS
"""

from __future__ import annotations

import json
import re
import unicodedata

from .errors import InvalidArgument, InvalidBucketName, InvalidKey, InvalidMetadata

MAX_KEY_BYTES = 512
MAX_SEGMENT_BYTES = 255
MAX_METADATA_BYTES = 2048
MAX_METADATA_NAME = 64
MAX_CONTENT_TYPE = 255


def _forbidden() -> frozenset[str]:
    chars = {chr(c) for c in range(0x00, 0x20)} | {chr(c) for c in range(0x7F, 0xA0)}
    chars |= {"\\", "\u061c", "\u200b", "\u200e", "\u200f", "\u2028", "\u2029", "\ufeff"}
    chars |= {chr(c) for c in range(0x202A, 0x202F)} | {chr(c) for c in range(0x2066, 0x206A)}
    return frozenset(chars)


#: Characters never allowed in a key, a prefix or a bucket scope (see the module docstring).
FORBIDDEN_CHARS = _forbidden()
_FORBIDDEN_RE = re.compile("[" + re.escape("".join(sorted(FORBIDDEN_CHARS))) + "\ud800-\udfff]")

_BUCKET_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,62}")
_META_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_ETAG_RE = re.compile(r"[0-9a-f]{64}")
# type "/" subtype, then optional parameters (RFC 9110 token characters).
_MIME_RE = re.compile(r"[A-Za-z0-9!#$%&'*+.^_`|~-]+/[A-Za-z0-9!#$%&'*+.^_`|~-]+(\s*;.*)?")


def _show(value: object) -> str:
    text = repr(value)
    return text if len(text) <= 80 else text[:77] + "..."


def _describe_char(c: str) -> str:
    if c == "\\":
        return "a backslash"
    if 0xD800 <= ord(c) <= 0xDFFF:
        return "a lone surrogate (text that was not valid UTF-8)"
    name = unicodedata.name(c, "")
    return f"U+{ord(c):04X}" + (f" ({name.lower()})" if name else " (a control character)")


def _utf8_len(text: str) -> int:
    return len(text.encode("utf-8"))


def _check_chars(text: str, what: str) -> None:
    bad = _FORBIDDEN_RE.search(text)
    if bad:
        raise InvalidKey(f"{what} {_show(text)} contains {_describe_char(bad.group())}, which is not allowed.")


def key_segments(key: object) -> list[str]:
    """Validate ``key`` and return the segments of its NFC form. Raises InvalidKey."""
    if not isinstance(key, str):
        raise InvalidKey(f"Key must be a string, got {type(key).__name__}.")
    if not key:
        raise InvalidKey("Key is empty.")
    if len(key) > MAX_KEY_BYTES * 4:  # cheap bound before normalising hostile input
        raise InvalidKey(f"Key is longer than {MAX_KEY_BYTES} bytes.")
    _check_chars(key, "Key")  # before normalising: NFC never removes these
    key = unicodedata.normalize("NFC", key)
    _check_chars(key, "Key")
    size = _utf8_len(key)
    if size > MAX_KEY_BYTES:
        raise InvalidKey(f"Key is {size} bytes long in UTF-8; the limit is {MAX_KEY_BYTES}.")
    segments = key.split("/")
    for seg in segments:
        if not seg:
            raise InvalidKey(
                f"Key {_show(key)} has an empty segment (a leading, trailing or doubled '/')."
            )
        seg_size = _utf8_len(seg)
        if seg_size > MAX_SEGMENT_BYTES:
            raise InvalidKey(
                f"Key {_show(key)} has a segment of {seg_size} bytes; the limit is {MAX_SEGMENT_BYTES}."
            )
        if seg.startswith("."):
            raise InvalidKey(f"Key {_show(key)} has a segment starting with '.' ({_show(seg)}).")
        if seg[0].isspace() or seg[-1].isspace():
            raise InvalidKey(f"Key {_show(key)} has a segment that starts or ends with a space ({_show(seg)}).")
    return segments


def validate_key(key: object) -> str:
    """Return the NFC form of ``key`` if it is a valid object key; raise InvalidKey otherwise.

    Keys that are already NFC (every ASCII key, for example) come back unchanged.
    """
    return "/".join(key_segments(key))


def is_valid_key(key: object) -> bool:
    """True when ``key`` is a valid key *as given* (already NFC-normalised)."""
    try:
        return validate_key(key) == key
    except InvalidKey:
        return False


def fold_path(text: str) -> str:
    """A caseless form of a path: two names with the same fold may be one file on a
    case-insensitive disk. Deliberately broader than any real filesystem's rule."""
    return unicodedata.normalize("NFC", unicodedata.normalize("NFD", text).casefold())


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
    """A list prefix: any leading part of a key ("" allowed), NFC-normalised.

    Same characters as keys; it may end anywhere (inside a segment, after a
    "/", with a space).
    """
    if prefix is None:
        return ""
    if not isinstance(prefix, str):
        raise InvalidKey(f"Prefix must be a string, got {type(prefix).__name__}.")
    hint = "A prefix may only contain the characters allowed in keys, and '/'."
    if len(prefix) > MAX_KEY_BYTES * 4:
        raise InvalidKey(f"Prefix is longer than {MAX_KEY_BYTES} bytes, so it can never match a key.", hint=hint)
    try:
        _check_chars(prefix, "Prefix")
    except InvalidKey as exc:
        raise InvalidKey(exc.message + " It can never match a key.", hint=hint) from None
    prefix = unicodedata.normalize("NFC", prefix)
    if _utf8_len(prefix) > MAX_KEY_BYTES:
        raise InvalidKey(f"Prefix is longer than {MAX_KEY_BYTES} bytes, so it can never match a key.", hint=hint)
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
    """The metadata dict stored as ``text``; DamagedError when the stored value is not one."""
    if not text:
        return {}
    try:
        value = json.loads(text) if isinstance(text, (str, bytes)) else None
    except (ValueError, RecursionError):
        value = None
    if not isinstance(value, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
        from .errors import DamagedError

        raise DamagedError("The stored metadata of an object is damaged (it is not a JSON object of strings).")
    return value


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
