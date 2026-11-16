"""mybucket: a small S3-like object store in a directory, with no server process.

Quick start::

    from mybucket import open_store

    store = open_store("~/data/mystore")
    photos = store.bucket("photos", create=True)
    info = photos.put("2026/11/cat.jpg", data, content_type="image/jpeg")
    photos.get("2026/11/cat.jpg")

    photos.restore("2026/11/cat.jpg", version=1)   # from the trash
    photos.export_dir("~/export/photos")            # plain files, no lock-in

    remote = open_store("ssh://backup-host/srv/mystore")   # same API, over ssh
    sync(open_store("~/data/mystore", readonly=True), remote)

Public names: open_store, Store, Bucket, Batch, ObjectStream, ObjectInfo,
PutInfo, VersionInfo, ListResult, BucketStats, DictInfo, validate_key,
validate_bucket_name, the maintenance and transfer functions (gc, gc_store,
verify, verify_store, snapshot, restore, export_dir, import_dir; also
available as Bucket / Store methods), sync, and every error class from
mybucket.errors.

Module map (each module's docstring lists what it offers to sibling code):
    keys.py     the only validation of keys, names, metadata, ETags
    errors.py   error classes with code, message, hint and CLI exit code
    schema.py   SQLite schema, pragmas, format checks
    blobs.py    blob files: paths, temp files, fsync, install, file invariants
    codec.py    staging (hash + zstd) and verified streaming reads
    bucket.py   object operations, batches, conditional writes, trash
    store.py    the store directory and bucket lifecycle
    maintenance.py  gc, verify, snapshot, restore
    transfer.py export_dir / import_dir (plain files in and out)
    dicts.py    trained zstd dictionaries (train, recompress, list)
    protocol.py the wire format of "mybucket serve --stdio" (framing, limits, errors)
    access.py   access profiles (read / write, per bucket and prefix)
    server.py   the serving side of remote access (the trust boundary)
    remote.py   the client side: open_store("ssh://host/path")
    replication.py  sync(): replication between two stores (local or remote)
    cli.py      the mybucket command
    models.py   result dataclasses
    _zstd.py    zstd implementation shim
"""

from .bucket import Batch, Bucket, ObjectStream
from .errors import (
    AccessDenied,
    BucketExists,
    BucketNotEmpty,
    BucketNotFound,
    Busy,
    ContentMismatch,
    DamagedError,
    DiskFull,
    InvalidArgument,
    InvalidBucketName,
    InvalidKey,
    InvalidMetadata,
    MyBucketError,
    NotAStore,
    NotAvailable,
    UnsafeStore,
    NotFound,
    ObjectNotFound,
    PermissionDenied,
    PreconditionFailed,
    ProtocolError,
    ReadOnlyError,
    Refused,
    StoreError,
    StoreNotFound,
    UnsupportedFormat,
    VersionNotFound,
)
from .keys import validate_bucket_name, validate_key
from .maintenance import gc, gc_store, restore, snapshot, verify, verify_store
from .models import BucketStats, DictInfo, ListResult, ObjectInfo, PutInfo, VersionInfo
from .store import Store, open_store
from .replication import sync
from .transfer import ExportConflict, export_dir, import_dir

__version__ = "0.2.0"

__all__ = [
    "open_store",
    "Store",
    "Bucket",
    "Batch",
    "ObjectStream",
    "ObjectInfo",
    "PutInfo",
    "VersionInfo",
    "ListResult",
    "BucketStats",
    "DictInfo",
    "validate_key",
    "validate_bucket_name",
    "gc",
    "gc_store",
    "verify",
    "verify_store",
    "snapshot",
    "restore",
    "export_dir",
    "import_dir",
    "sync",
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
    "ExportConflict",
    "BucketExists",
    "BucketNotEmpty",
    "NotAStore",
    "UnsafeStore",
    "Busy",
    "PermissionDenied",
    "AccessDenied",
    "NotAvailable",
    "ProtocolError",
    "DamagedError",
    "UnsupportedFormat",
    "StoreError",
    "DiskFull",
]
