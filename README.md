# my-bucket-storage

An S3-like object store for one person's apps that:

- **is built to save disk space**: objects are compressed with zstd (unless
  that saves less than 5 %), identical content is stored once, and small
  objects are packed into a SQLite file instead of each costing a 4 KiB
  filesystem block. On a synthetic year of JSON exports a bucket took about
  a quarter of the disk of the same files stored plainly, and about 30 % less
  than compressing each file with `zstd -19` ([measured below](#space)).
  Incompressible data (photos, video) gains little or nothing, and every
  bucket costs at least about 40 KiB;
- **runs with no server process**: it is a Python library (`mybucket`) that
  opens the files directly, so it uses no memory when nothing is using it;
- **never locks your data in**: the format is plain SQLite plus plain zstd,
  documented in [docs/FORMAT.md](docs/FORMAT.md) well enough to recover
  everything with only the `sqlite3` and `zstd` command-line tools, and
  `export` writes every object as a plain file.

It is meant for things like app data archives, synced exports and generated
files on your own machine: many small-to-medium objects, a few writers, many
readers. It is not a network service and not a replacement for S3.

## Why

Apps that keep their data as loose files each end up re-implementing the
same things: atomic writes, safe paths, "did this change?", backups. This
gives them one small, tested primitive instead:

- atomic puts, and `batch()` to commit several writes together;
- conditional writes (`if_match`, `if_none_match="*"`) for safe concurrent updates;
- keys that are validated in one place, so a key can never escape a
  directory when exported;
- re-putting identical content is cheap and reports `unchanged`;
- overwritten and deleted objects stay in a trash for 7 days (configurable);
- crash safety: a crash at any moment never loses a committed object and
  never leaves a readable-but-corrupt one. `get()` checks the size and
  SHA-256 of everything it returns; a stream from `open()` is checked when it
  reaches the end (see [Reading](#reading)).

## Install

Requires Python 3.12 or newer. The only runtime dependency is zstd: the
standard library's `compression.zstd` on Python 3.14+, the `backports.zstd`
package on older versions. (If neither is available, mybucket also works
with the `zstandard` package.)

```sh
uv add "my-bucket-storage @ git+https://github.com/<owner>/my-bucket-storage"
# or, from a checkout:
uv sync && uv run pytest
```

## Quick start (Python SDK)

```python
from mybucket import open_store, PreconditionFailed

store = open_store("~/data/mystore")           # a directory; created if missing
notes = store.bucket("notes", create=True)     # one SQLite file per bucket

info = notes.put(
    "2026/11/14/day.json",
    b'{"steps": 10432}',
    content_type="application/json",
    metadata={"source": "watch"},
)
info.etag        # SHA-256 of the content
info.status      # "created", "updated" or "unchanged"

notes.get("2026/11/14/day.json")               # -> bytes, verified
notes.head("2026/11/14/day.json")              # -> ObjectInfo (raises ObjectNotFound)
notes.exists("2026/11/14/day.json")            # -> True

# Large objects stream in and out without being held in memory.
with open("video.mp4", "rb") as f:
    notes.put("media/video.mp4", f, content_type="video/mp4")
with notes.open("media/video.mp4") as stream:
    first_mb = stream.read(1 << 20)            # not verified yet: see "Reading"

# S3-style listing with common prefixes and pagination.
page = notes.list(prefix="2026/", delimiter="/", limit=100)
page.objects, page.prefixes, page.truncated, page.next_start_after

# Optimistic concurrency.
notes.put("counter", b"41")
current = notes.head("counter")
try:
    notes.put("counter", b"42", if_match=current.etag)
except PreconditionFailed:
    ...  # someone else changed it first: read again and retry
notes.put("lock", b"me", if_none_match="*")    # only if it does not exist yet

# Several writes that become visible together, or not at all.
notes.put("drafts/old.json", b"{}")
with notes.batch() as tx:
    tx.put("2026/11/15/day.json", b"{}")
    tx.put("2026/11/15/summary.json", b"{}")
    tx.delete("drafts/old.json")
tx.results                                     # one PutInfo / ObjectInfo per operation

# Deleting a key that does not exist raises ObjectNotFound (unlike S3),
# unless you say that is fine:
notes.delete("drafts/never-existed.json", missing_ok=True)   # -> None

# Deleted and overwritten versions stay in the trash for 7 days.
notes.delete("2026/11/14/day.json")
versions = notes.list_versions("2026/11/14/day.json")   # newest first
notes.restore("2026/11/14/day.json", versions[0].version)
notes.configure(trash_days=30)

notes.stats()                                  # objects, logical vs stored bytes, ...

# Consumers that must never write open the store read-only
# (SQLite itself is opened read-only too).
reader = open_store("~/data/mystore", readonly=True)
```

Errors are subclasses of `mybucket.MyBucketError`. Each has a plain-language
`message`, a `hint` saying what to do next, a stable `code` and an
`exit_code` used by the CLI (see [the exit code table](#exit-codes)).

### Keys

A key is 1-512 ASCII bytes made of segments separated by `/`. Each segment
is 1-255 characters from `A-Z a-z 0-9 . _ ~ @ + = , -`, and must not start
with `.` (so `.` and `..` are impossible). There is no leading or trailing
`/` and no empty segment. **Spaces and non-ASCII characters are not
allowed** (unlike S3): `IMG 1234.jpg` or `café.txt` are refused with
InvalidKey. These rules are what make every key a safe relative file path
for `export`. Keys that differ only in letter case are different keys.

### Batches

- Nothing in a batch is visible to anyone, including reads made inside the
  `with` block, until the block ends without an exception; then every
  operation applies or none does.
- `tx.put()` and `tx.delete()` return nothing; results are in `tx.results`
  after the block.
- Batches do not nest: opening a second batch on the same bucket inside the
  first raises StoreError.

### Content types

`put()` stores the content type you pass, or none. The `mybucket put`
command guesses one from the key's extension (`.json` becomes
`application/json`) unless you pass `-t` or `--no-guess-type`. Content type
is part of what makes a put "unchanged", so if you write the same objects
from both, pass the content type explicitly in both. Content types must
look like `type/subtype`.

### Reading

- `get(key)` returns the whole object and has checked its size and SHA-256:
  damaged data raises DamagedError instead of being returned.
- `open(key)` returns a stream. It is checked as it is read, but the check
  can only conclude at the end: reading to the end of damaged content raises
  DamagedError. **Bytes from a partial read (`stream.read(1000)`) are not
  verified yet.** `stream.verified` becomes True once the end was reached and
  everything matched.

### Repairing damage

`verify()` (or `mybucket verify`) re-reads and re-hashes everything. If an
object is damaged and you still have the original bytes, put them again
(under the same or any other key), or import a good export again: mybucket
re-checks the stored copy whenever a write brings content it already has,
and replaces a damaged copy with the new, hash-checked bytes
(`info.repaired` is True). Otherwise restore the bucket from a snapshot.
Because of this check, a re-put of content the bucket already has reads its
stored copy once.

### Guarding against cut-off input

A put from a pipe stores whatever arrived, even when the program feeding
the pipe failed half-way. Use `set -o pipefail` in scripts, and when you
know the size or checksum the data should have, pass it:

```sh
set -o pipefail   # a failure anywhere in the pipe fails the whole command
zstd -dc db.dump.zst | mybucket put backups db.dump --expect-sha256 "$(cat db.dump.sha256)"
```

`put(..., expected_size=N, expected_sha256=hex)` (and `--expect-size`,
`--expect-sha256`) refuse the write (ContentMismatch, exit 5, nothing
written) when the data read does not match.

## Maintenance, backups and export

All of these are methods (and also functions in `mybucket.maintenance` and
`mybucket.transfer`, exported from `mybucket`):

| Python | CLI | What it does |
|---|---|---|
| `bucket.restore(key, version)` | `mybucket restore BUCKET KEY VERSION` | makes a trashed version current again (as a new version) |
| `bucket.gc()` / `store.gc()` | `mybucket gc [BUCKET...]` | removes expired trash and content nothing uses any more |
| `bucket.verify()` / `store.verify()` | `mybucket verify [BUCKET...]` | re-reads everything and reports damage; changes nothing |
| `store.snapshot(dest)` | `mybucket snapshot FOLDER` | a consistent copy of the store, safe while writers run |
| `bucket.export_dir(dest)` | `mybucket export BUCKET FOLDER` | every object as a plain file at its key path |
| `bucket.import_dir(src, prefix="")` | `mybucket import BUCKET FOLDER` | every file of a folder as an object (idempotent) |

### Recover a deleted object

```python
from mybucket import open_store

notes = open_store("~/data/mystore").bucket("notes")
for v in notes.list_versions("2026/11/14/day.json"):
    print(v.version, v.current, v.reason, v.expires_at)
notes.restore("2026/11/14/day.json", 1)        # version 1 becomes current again
```

A version stays restorable until gc removes it after `trash_days`.

### gc

gc never deletes anything a concurrent writer is about to use. It removes
leftover blob files from interrupted writes only once they are 10 minutes
old, and abandoned temp files after 1 hour, so running gc right after a
crash may report that it kept them for now.

### Backups

- `snapshot` writes a new store (open it with `open_store(dest)` or
  `--store dest`). On the same filesystem it **hard-links** blob files
  instead of copying them, so the snapshot shares disk blocks with the
  store: it protects against mistakes and bugs, not against disk failure.
  Copy the snapshot to another disk for a real backup.
- To restore a bucket from a snapshot: stop every program that writes to the
  store, then replace `<store>/buckets/<name>` with the snapshot's
  `buckets/<name>` folder (or just point `--store` / `open_store()` at the
  snapshot).
- `export` is not a point-in-time copy while writers run; export from a
  snapshot for a frozen view.

### Export and import

`export` writes every object as a plain file at its key path plus a
`.mybucket-export.jsonl` manifest with content types and metadata
([format](docs/FORMAT.md#53-export-manifest)). It refuses to replace
existing files that differ (`--overwrite`), and reports keys that cannot
coexist as files: a key that is also a folder of other keys, and keys that
differ only in letter case on a case-insensitive disk (`--skip-conflicts`
exports the rest).

`import` stores every regular file under a folder. Files whose names cannot
be keys (spaces, hidden files, symbolic links, ...) are skipped and listed;
`mybucket import` then exits 1 so scripts notice, unless you pass
`--allow-skips`.

## Command line

```sh
export MYBUCKET_STORE=~/data/mystore           # or pass --store PATH
mybucket mb photos
mybucket put photos 2026/11/cat.jpg cat.jpg    # content type guessed from .jpg
cat notes.txt | mybucket put photos notes.txt  # from stdin
mybucket ls photos 2026/ -d /
mybucket get photos 2026/11/cat.jpg ./out/     # into an existing folder
mybucket --json head photos 2026/11/cat.jpg
mybucket rm photos 2026/11/cat.jpg
mybucket versions photos 2026/11/cat.jpg
mybucket restore photos 2026/11/cat.jpg 1
mybucket gc && mybucket verify
mybucket snapshot /backups/mystore-2026-11-16
python -m mybucket --help                      # the same tool
```

Commands: `buckets`, `mb`, `rb`, `put`, `get`, `cat`, `head`, `ls`, `rm`,
`versions`, `restore`, `export`, `import`, `gc`, `verify`, `snapshot`,
`stats`. `mybucket COMMAND --help` describes each one.

- The store comes from `--store PATH` or the `MYBUCKET_STORE` environment
  variable. Commands that only read open the store read-only.
- `--json` (before or after the command) makes stdout carry exactly one JSON
  document. Errors are then `{"error": {"code", "message", "hint"}}` on
  stdout; without `--json` they go to stderr as plain text.
- `get` and `export` refuse destinations inside the store.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | ok |
| 1 | error: damaged data, `verify` found damage, `import` skipped files, disk full, other failures |
| 2 | usage: invalid key, bucket name or argument |
| 3 | not found: store, bucket, object, version, folder |
| 4 | not allowed: read-only store, permission denied |
| 5 | refused for safety: failed `--if-match` / `--if-none-match`, `--expect-*` mismatch, bucket not empty, export conflict |
| 75 | temporary: another process holds the write lock; retry |

## How it stores data

- A **store** is a directory with one sub-directory per **bucket**.
- Each bucket is one SQLite database in WAL mode (many readers and one
  writer at a time, across processes) plus a `blobs/` directory.
- Content is identified by its SHA-256 and stored once per bucket.
- zstd level 19 for objects up to 4 MiB, level 9 for larger or streamed
  ones; raw when compression would save less than 5 %.
- Stored content up to 128 KiB lives inside the SQLite file; larger content
  is an immutable file under `blobs/`, written to a temp file, fsynced and
  renamed into place.
- If another process deletes a bucket, handles that are still open on it
  refuse further reads and writes (BucketNotFound) instead of writing into a
  deleted file.

Details, including how to read everything back by hand: [docs/FORMAT.md](docs/FORMAT.md).

Do not put a store on a network or cloud-synced filesystem (NFS, SMB, iCloud
Drive, Dropbox): SQLite's locking needs a local filesystem. A read-only open
still needs the bucket directory to be writable by the reading user, because
SQLite keeps a shared-memory file next to a WAL-mode database.

## Space

Measured with `uv run python bench/bench_space.py` (defaults) on an Apple
M1 Pro laptop (macOS, APFS, Python 3.12.8, backports.zstd). The archive is
synthetic and highly repetitive JSON, the kind of data mybucket is built for
([bench/README.md](bench/README.md) describes it):

```
mybucket space benchmark (Python 3.12.8, backports.zstd, trash_days=7)
---------------------------------------------------------------------------------------
Archive                                365 days, 2,319 keys, 15,353 puts (2,319 created, 40 updated, 12,994 unchanged)
Logical bytes (final content)          43.0 MiB
Plain files: apparent size             43.0 MiB
Plain files: on disk (st_blocks)       49.3 MiB  (2,319 files, 4096 B blocks)
Plain files, each zstd -19 (computed)  17.7 MiB
Bucket: on disk (st_blocks)            12.1 MiB  (6 files)
Bucket: database / blob files on disk  7.1 MiB / 5.0 MiB  (1,980 inline, 4 file blobs)
Bucket: distinct contents              1,984 blobs, 40 old versions in trash
Ratio plain on disk / bucket on disk   4.08x
Ratio logical / bucket on disk         3.56x
Put throughput (all puts)              749 puts/s, 11.0 MiB/s (20.5 s)
  new or changed content               120 puts/s, 2.3 MiB/s (2,359 puts, 19.6 s)
  unchanged re-puts                    15,120 puts/s, 209.9 MiB/s (12,994 puts, 0.9 s)
Get throughput (every key)             16,319 gets/s, 302.4 MiB/s (0.1 s)
Streaming put of 256.0 MiB             48.3 MiB/s, stored as 59.9 MiB
  peak memory during put               6.2 MiB Python heap, 19.2 MiB process RSS growth
  streaming get                        539.0 MiB/s, 6.3 MiB Python heap peak
```

So on this data a bucket used about a quarter of the disk of the plain
files (4.08x less) and about 30 % less than compressing every file on its
own with `zstd -19` (17.7 MiB, computed per file and rounded up to 4 KiB
blocks). Unchanged re-puts are cheap but not free: they re-check the stored
copy (see [Repairing damage](#repairing-damage)).

The worst case is incompressible data, measured with
`uv run python bench/bench_incompressible.py`:

```
Empty bucket on disk: 40.0 KiB
500 random (incompressible) objects per size; disk use counted from st_blocks
 object size     logical   plain files      bucket  bucket / plain
       500 B   244.1 KiB       2.0 MiB   384.0 KiB           0.19x
      2000 B   976.6 KiB       2.0 MiB     2.0 MiB           1.05x
      4000 B     1.9 MiB       2.0 MiB     2.0 MiB           1.05x
      8000 B     3.8 MiB       3.9 MiB     4.0 MiB           1.02x
     64000 B    30.5 MiB      31.2 MiB    32.1 MiB           1.03x
    200000 B    95.4 MiB      95.7 MiB    95.8 MiB           1.00x
```

Tiny incompressible objects still gain from not costing a 4 KiB block each,
but from about 2 KB upwards random data takes as much disk in a bucket as
in plain files, or a few percent more (SQLite stores each row of 2-4 KB in
its own 4 KiB page). Each bucket costs at least 40 KiB, and while a store is
open its WAL file can add a few MiB per bucket (`stats().database_bytes`
counts it); closing the store folds it back.

## Status

Version 0.1, the first version. It is young: keep backups.

| Area | State |
|---|---|
| Storage format, dedup, compression, inline/file placement | done |
| put / get / open / head / exists / list / delete / batch | done |
| Conditional writes, trash, list_versions, restore | done |
| gc, verify (and repair by re-put), snapshot | done |
| export_dir / import_dir, docs/FORMAT.md recovery script | done |
| Read-only mode, multi-process safety, crash tests | done |
| `mybucket` command-line tool (`--json`, stable exit codes) | done |

### Planned, not in v1

- Remote access over SSH and over HTTP (a unix socket first).
- Access tokens and per-app profiles (read-only, per bucket or prefix).
- Trained zstd dictionaries per content type (the format already reserves a
  dictionary id per blob).
- Range reads inside large objects.
- Replication to another machine.
- S3 API compatibility.
- A TypeScript client.

## Development

```sh
uv sync
uv run pytest                 # the whole suite, including stress and crash tests
uv run python bench/bench_space.py
```

## License

MIT, see [LICENSE](LICENSE).
