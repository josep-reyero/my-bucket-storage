"""Plain result types returned by the SDK.

All of them are frozen dataclasses with a ``to_dict()`` that gives the JSON
shape the CLI prints. Times are unix milliseconds (UTC).

Public:
    ObjectInfo   head(), list(), delete() results
    PutInfo      put() result: an ObjectInfo plus ``status`` ("created",
                 "updated" or "unchanged")
    VersionInfo  list_versions() entries (current version and trash)
    ListResult   list() result: objects, common prefixes, truncation cursor
    BucketStats  stats() result
    DictInfo     dictionaries() entries
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

STATUS_CREATED = "created"
STATUS_UPDATED = "updated"
STATUS_UNCHANGED = "unchanged"


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    etag: str  # SHA-256 hex of the uncompressed bytes
    size: int  # uncompressed bytes
    content_type: str | None
    metadata: dict[str, str]
    modified: int  # unix ms when this version was written
    version: int  # per-bucket increasing number
    #: Bytes actually used for the content (shared when deduplicated); None when
    #: the connection may not see it (a prefix-scoped remote connection).
    stored_size: int | None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class PutInfo(ObjectInfo):
    status: str = STATUS_CREATED
    #: True when this write also replaced a damaged stored copy of the content.
    repaired: bool = False

    @property
    def unchanged(self) -> bool:
        return self.status == STATUS_UNCHANGED


@dataclass(frozen=True)
class VersionInfo:
    key: str
    version: int
    etag: str
    size: int
    content_type: str | None
    metadata: dict[str, str]
    modified: int
    current: bool
    trashed_at: int | None = None  # None for the current version
    expires_at: int | None = None
    reason: str | None = None  # "overwritten" or "deleted" for trash entries

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ListResult:
    objects: list[ObjectInfo] = field(default_factory=list)
    prefixes: list[str] = field(default_factory=list)
    truncated: bool = False
    #: Pass as ``start_after`` to get the next page (None when not truncated).
    next_start_after: str | None = None
    #: True when this listing reached the end of the keys asked for. A remote
    #: listing is complete only when the server said so explicitly.
    complete: bool = True

    def __iter__(self):
        """Iterate over objects and common prefixes together, in key order."""
        entries: list[tuple[str, object]] = [(o.key, o) for o in self.objects]
        entries += [(p, p) for p in self.prefixes]
        entries.sort(key=lambda e: e[0])
        return iter(e[1] for e in entries)

    def __len__(self) -> int:
        return len(self.objects) + len(self.prefixes)

    @property
    def keys(self) -> list[str]:
        return [o.key for o in self.objects]

    def to_dict(self) -> dict:
        return {
            "objects": [o.to_dict() for o in self.objects],
            "prefixes": list(self.prefixes),
            "truncated": self.truncated,
            "next_start_after": self.next_start_after,
            "complete": self.complete,
        }


@dataclass(frozen=True)
class BucketStats:
    bucket: str
    objects: int  # current keys
    logical_bytes: int  # sum of sizes of current objects
    trash_versions: int
    trash_logical_bytes: int
    blobs: int  # distinct contents stored
    blob_bytes: int  # uncompressed bytes of distinct contents
    stored_bytes: int  # compressed bytes of distinct contents
    inline_blobs: int
    file_blobs: int
    database_bytes: int  # bucket.sqlite plus its WAL file
    file_bytes: int  # bytes under blobs/ (as recorded)
    trash_days: int
    dict_blobs: int = 0  # distinct contents compressed with a trained dictionary
    dictionaries: int = 0  # dictionaries stored (they are inside the database)
    dict_bytes: int = 0  # their size
    active_dict: int | None = None  # the dictionary new small objects are compressed with
    format: int = 2  # the bucket's on-disk format version

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class DictInfo:
    id: int  # the value of blobs.dict_id
    zstd_id: int  # the dictionary ID written in each zstd frame
    size: int  # bytes
    created_at: int
    samples: int  # objects it was trained on
    prefix: str  # the key prefix the samples came from
    active: bool  # new small objects are compressed with it
    blobs: int  # stored contents that need it
    stored_bytes: int  # their stored (compressed) size

    def to_dict(self) -> dict:
        return asdict(self)
