"""Access profiles: what one ``mybucket serve`` connection may do.

A profile is a list of grants. Each grant names a bucket (or every bucket),
a key prefix ("" for the whole bucket) and whether it allows writing
(writing always includes reading). The serving side builds the profile from
its own command line, which a forced SSH command fixes, and checks every
request against it; the client can only make it narrower (``hello`` with
``readonly``), never wider. A request outside the profile gets AccessDenied
(exit code 4) whether or not the bucket or key exists, so a profile never
tells what lies outside it.

Command-line spelling (``mybucket serve``)::

    --read-all              read every bucket
    --write-all             read and write every bucket, and create buckets
    --read BUCKET[/PREFIX]  read one bucket, or only keys starting with PREFIX
    --write BUCKET[/PREFIX] read and write one bucket, or only keys under PREFIX

Prefixes are compared after Unicode NFC normalisation, on the normalised key
the engine will actually store, so a decomposed spelling cannot slip past a
prefix. Use a trailing "/" to mean a folder: ``--read photos/public/``. A
prefix without one (``logs/2026``) also covers ``logs/2026-backup/...``.

What each operation needs:

- object operations (head, exists, get, open, versions): a readable grant
  whose prefix starts the key; put, delete, restore: a writable one;
- list: a readable grant whose prefix starts the list prefix (list inside
  your prefix: ``list(prefix="public/")``);
- stats: a readable grant for the whole bucket (it counts everything);
- seeing a bucket (buckets, opening it): any grant on it;
- creating buckets: ``--write-all``. Deleting buckets and maintenance (gc,
  verify, snapshot, export, import, dictionaries) are never available over
  the connection.

Public:
    Grant(bucket, prefix, write)          bucket None means every bucket
    Profile(grants)                       .parse(read=[], write=[], read_all=False, write_all=False)
                                          .narrowed(readonly) .readonly .describe()
                                          .require_bucket(name) .require_key(name, key, write)
                                          .require_list(name, prefix) .require_whole_bucket(name)
                                          .require_create() .visible(name) .writable(name)
"""

from __future__ import annotations

from typing import NamedTuple

from . import keys as _keys
from .errors import AccessDenied, InvalidArgument, InvalidBucketName, InvalidKey


class Grant(NamedTuple):
    bucket: str | None  # None: every bucket
    prefix: str
    write: bool

    def label(self) -> str:
        where = "every bucket" if self.bucket is None else f"bucket {self.bucket!r}"
        if self.prefix:
            where += f", keys starting with {self.prefix!r}"
        return ("read and write " if self.write else "read ") + where


def parse_grant(spec: object, write: bool) -> Grant:
    """``BUCKET`` or ``BUCKET/PREFIX``; raises InvalidArgument with a plain message."""
    if not isinstance(spec, str) or not spec:
        raise InvalidArgument("An access grant must look like BUCKET or BUCKET/PREFIX.")
    bucket, sep, prefix = spec.partition("/")
    try:
        bucket = _keys.validate_bucket_name(bucket)
        prefix = _keys.validate_prefix(prefix) if sep else ""
    except (InvalidBucketName, InvalidKey) as exc:
        raise InvalidArgument(
            f"The access grant {spec!r} is not valid: {exc.message}",
            hint="Write it as BUCKET or BUCKET/PREFIX, for example 'photos' or 'photos/public/'.",
        ) from None
    return Grant(bucket, prefix, write)


class Profile:
    def __init__(self, grants: list[Grant]) -> None:
        self.grants = tuple(grants)

    @classmethod
    def parse(
        cls,
        read: list[str] | None = None,
        write: list[str] | None = None,
        read_all: bool = False,
        write_all: bool = False,
    ) -> Profile:
        grants = [parse_grant(spec, False) for spec in read or []]
        grants += [parse_grant(spec, True) for spec in write or []]
        if read_all:
            grants.append(Grant(None, "", False))
        if write_all:
            grants.append(Grant(None, "", True))
        if not grants:
            raise InvalidArgument(
                "No access was granted.",
                hint="Pass --read-all, --write-all, --read BUCKET[/PREFIX] or --write BUCKET[/PREFIX].",
            )
        return cls(grants)

    def __repr__(self) -> str:
        return f"<Profile {'; '.join(g.label() for g in self.grants)}>"

    @property
    def readonly(self) -> bool:
        return not any(g.write for g in self.grants)

    def narrowed(self, readonly: bool) -> Profile:
        if not readonly:
            return self
        return Profile([Grant(g.bucket, g.prefix, False) for g in self.grants])

    def describe(self) -> list[dict]:
        return [{"bucket": g.bucket, "prefix": g.prefix, "write": g.write} for g in self.grants]

    # -- checks ------------------------------------------------------------------

    def _for(self, name: str) -> list[Grant]:
        return [g for g in self.grants if g.bucket is None or g.bucket == name]

    def visible(self, name: str) -> bool:
        return bool(self._for(name))

    def writable(self, name: str) -> bool:
        return any(g.write for g in self._for(name))

    def _deny(self, what: str) -> AccessDenied:
        return AccessDenied(f"This connection may not {what}.")

    def require_bucket(self, name: str) -> None:
        if not self.visible(name):
            raise self._deny(f"use bucket {name!r}")

    def require_key(self, name: str, key: str, write: bool) -> None:
        """``key`` must already be the validated (NFC) key."""
        for g in self._for(name):
            if key.startswith(g.prefix) and (g.write or not write):
                return
        verb = "change" if write else "read"
        raise self._deny(f"{verb} {key!r} in bucket {name!r}")

    def require_list(self, name: str, prefix: str) -> None:
        """``prefix`` must already be validated (NFC)."""
        grants = self._for(name)
        if any(prefix.startswith(g.prefix) for g in grants):
            return
        allowed = sorted({g.prefix for g in grants})
        error = self._deny(f"list keys starting with {prefix!r} in bucket {name!r}")
        if allowed:
            error.hint = "It may only list inside: " + ", ".join(repr(p) for p in allowed) + " (pass it as the prefix)."
        raise error

    def require_whole_bucket(self, name: str) -> None:
        if not any(g.prefix == "" for g in self._for(name)):
            raise self._deny(f"see the totals of bucket {name!r} (its profile covers only part of it)")

    def require_create(self) -> None:
        if not any(g.bucket is None and g.prefix == "" and g.write for g in self.grants):
            raise self._deny("create buckets")


__all__ = ["Grant", "Profile", "parse_grant"]
