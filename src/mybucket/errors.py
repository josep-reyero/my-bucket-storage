"""Errors raised by mybucket.

Every error carries three plain-language parts, so the SDK, the CLI and any
future transport can show the same thing:

- ``message``: what happened;
- ``hint``: what to do next;
- ``code``: a stable machine-readable string (``"not_found"``, ...).

Each class also has an ``exit_code`` that the CLI uses directly:

====  ==========================================================
0     ok
1     error (damaged data, unsupported format, other failures)
2     usage (invalid key, bucket name, argument)
3     not found (bucket, object, version)
4     not allowed (read-only store)
5     refused for safety (precondition failed, bucket not empty,
      already exists)
75    temporary (the bucket is locked by another writer; retry)
====  ==========================================================

Public: all classes below, ``MyBucketError.to_dict()``.
Internal for siblings: ``EXIT_*`` constants (the CLI maps exceptions it does
not own, such as ``argparse`` usage errors, onto the same numbers), and
``from_os_error(exc, what)``, which turns an OSError met while doing
``what`` into the matching error below (disk full, permission denied, ...).
"""

from __future__ import annotations

import errno as _errno

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_NOT_ALLOWED = 4
EXIT_REFUSED = 5
EXIT_TEMPORARY = 75


class MyBucketError(Exception):
    """Base class for every error mybucket raises on purpose."""

    code = "error"
    exit_code = EXIT_ERROR
    default_hint = ""

    def __init__(self, message: str, hint: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.hint = self.default_hint if hint is None else hint

    def __str__(self) -> str:
        if self.hint:
            return f"{self.message} {self.hint}"
        return self.message

    def to_dict(self) -> dict:
        """The JSON shape the CLI prints: {"error": {"code", "message", "hint"}}."""
        return {"error": {"code": self.code, "message": self.message, "hint": self.hint}}


# -- usage (2) ---------------------------------------------------------------


class InvalidArgument(MyBucketError, ValueError):
    code = "invalid_argument"
    exit_code = EXIT_USAGE


class InvalidKey(InvalidArgument):
    code = "invalid_key"
    default_hint = (
        "Keys are 1-512 ASCII bytes made of '/'-separated segments; each segment is "
        "1-255 characters from A-Z a-z 0-9 . _ ~ @ + = , - and must not start with '.'."
    )


class InvalidBucketName(InvalidArgument):
    code = "invalid_bucket_name"
    default_hint = (
        "Bucket names are 2-63 characters of lowercase letters, digits and '-', "
        "starting with a letter or digit."
    )


class InvalidMetadata(InvalidArgument):
    code = "invalid_metadata"


# -- not found (3) -----------------------------------------------------------


class NotFound(MyBucketError, LookupError):
    code = "not_found"
    exit_code = EXIT_NOT_FOUND


class StoreNotFound(NotFound):
    code = "store_not_found"
    default_hint = "Check the store path, or open it without read-only mode to create it."


class BucketNotFound(NotFound):
    code = "bucket_not_found"
    default_hint = "List buckets to see the names that exist, or create this one first."


class ObjectNotFound(NotFound):
    code = "object_not_found"
    default_hint = "List the bucket to see which keys exist."


class VersionNotFound(NotFound):
    code = "version_not_found"
    default_hint = "List the versions of this key to see which ones are still kept."


# -- not allowed (4) ---------------------------------------------------------


class ReadOnlyError(MyBucketError):
    code = "read_only"
    exit_code = EXIT_NOT_ALLOWED
    default_hint = "This store was opened read-only; open it without read-only mode to write."


# -- refused for safety (5) --------------------------------------------------


class Refused(MyBucketError):
    code = "refused"
    exit_code = EXIT_REFUSED


class PreconditionFailed(Refused):
    code = "precondition_failed"
    default_hint = (
        "Someone else may have changed the object. Read it again (head) and retry "
        "with the current ETag, or drop the condition."
    )
    #: Hint for if_none_match='*' on a key that exists.
    HINT_EXISTS = (
        "The key already exists, so it was left unchanged. Use another key, or read the "
        "existing object if you meant to update it (pass if_match with its ETag)."
    )
    #: Hint for if_match on a key that does not exist.
    HINT_MISSING = (
        "The object was deleted or never existed. Create it with if_none_match='*' or "
        "without a condition."
    )


class ContentMismatch(Refused):
    """The data read does not have the size or SHA-256 the caller said to expect."""

    code = "content_mismatch"
    default_hint = (
        "Nothing was written. The input was probably cut short (for example a failed "
        "command earlier in a pipe); check the source and retry."
    )


class BucketExists(Refused):
    code = "bucket_exists"
    default_hint = "Pick another name, or open the existing bucket."


class BucketNotEmpty(Refused):
    code = "bucket_not_empty"
    default_hint = (
        "Delete or export its objects first, or pass force=True (--force) if you "
        "really want to remove everything in it, including its trash."
    )


class NotAStore(Refused):
    code = "not_a_store"
    default_hint = "Point at an empty directory, a new path, or an existing mybucket store."


# -- temporary (75) ----------------------------------------------------------


class Busy(MyBucketError):
    code = "busy"
    exit_code = EXIT_TEMPORARY
    default_hint = "Another process is writing to this bucket. Retry in a moment."


# -- not allowed by the operating system (4) ---------------------------------


class PermissionDenied(MyBucketError):
    code = "permission_denied"
    exit_code = EXIT_NOT_ALLOWED
    default_hint = "Check that your user may read and write the store directory and its files."


# -- generic errors (1) ------------------------------------------------------


class DamagedError(MyBucketError):
    """Stored bytes do not match their recorded hash or size, or are missing."""

    code = "damaged"
    default_hint = (
        "The stored copy is damaged. Run verify to see everything affected. If you still "
        "have the original data, put it again (under any key): that repairs the stored "
        "copy. Otherwise restore the bucket from a snapshot or backup."
    )


class UnsupportedFormat(MyBucketError):
    code = "unsupported_format"
    default_hint = "This data was written by a newer mybucket. Upgrade mybucket to read it."


class StoreError(MyBucketError):
    code = "store_error"


class DiskFull(StoreError):
    code = "disk_full"
    default_hint = "Free some disk space (gc removes expired trash), then retry."


_FULL = {_errno.ENOSPC, getattr(_errno, "EDQUOT", _errno.ENOSPC), _errno.EFBIG}


def from_os_error(exc: OSError, what: str) -> MyBucketError:
    """A plain-language error for an OSError met while trying to ``what``."""
    reason = exc.strerror or str(exc)
    if exc.errno in _FULL:
        return DiskFull(f"Could not {what}: the disk is full or the file is too large for it ({reason}). Nothing was written.")
    if exc.errno in (_errno.EACCES, _errno.EPERM) or isinstance(exc, PermissionError):
        return PermissionDenied(f"Could not {what}: permission denied ({exc.filename or reason}).")
    if isinstance(exc, FileNotFoundError):
        return StoreError(
            f"Could not {what}: a file or folder of the bucket is missing ({exc.filename or reason}).",
            hint="The bucket may have been deleted by another process. Open it again; if it still "
            "exists, run verify.",
        )
    return StoreError(f"Could not {what}: {reason}.", hint="Check the disk, then retry.")


__all__ = [
    "MyBucketError",
    "InvalidArgument",
    "InvalidKey",
    "InvalidBucketName",
    "InvalidMetadata",
    "NotFound",
    "StoreNotFound",
    "BucketNotFound",
    "ObjectNotFound",
    "VersionNotFound",
    "ReadOnlyError",
    "Refused",
    "PreconditionFailed",
    "ContentMismatch",
    "BucketExists",
    "BucketNotEmpty",
    "NotAStore",
    "Busy",
    "PermissionDenied",
    "DamagedError",
    "UnsupportedFormat",
    "StoreError",
    "DiskFull",
]
