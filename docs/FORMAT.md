# mybucket on-disk format, version 2

This document describes everything mybucket writes, in enough detail to read
your data back **without mybucket installed**, using only the `sqlite3` and
`zstd` command-line tools. If anything here disagrees with the code, the code
is wrong: please report it.

Format version: **2** for buckets (`meta.format`, `PRAGMA user_version`);
the store marker `mybucket-store.json` still says `{"format": 1}`, because
the store directory layout did not change. Format 1 buckets (written by
mybucket 0.1) are described by this same document minus the parts marked
*format 2*; section 6 says how they are upgraded.

## 1. Directory layout

```
<store>/
  mybucket-store.json                 {"format": 1}
  buckets/
    <bucket>/
      bucket.sqlite                   SQLite database (WAL mode)
      bucket.sqlite-wal               SQLite write-ahead log (may be absent)
      bucket.sqlite-shm               SQLite shared-memory index (may be absent)
      blobs/<h0h1>/<h2h3>/<hash>      large stored blobs, immutable
      tmp/<pid>-<random>.part         writes in progress (never referenced)
    .creating-<bucket>-<random>/      a bucket being created (not yet visible)
    .deleting-<bucket>-<random>/      a bucket being deleted
```

- `<bucket>` matches `^[a-z0-9][a-z0-9-]{1,62}$`. Directories under
  `buckets/` whose names do not match (all of them start with `.`) are not
  buckets and can be ignored.
- `<hash>` is the lowercase hex SHA-256 of the object's **uncompressed**
  bytes (64 characters). `<h0h1>` and `<h2h3>` are its first and second pairs
  of characters, for example
  `blobs/ea/b4/eab43d21a7f5f0224a6e2b86b9d65c2aaa567d0fcb89279a2af01a7412edd836`.
- Everything is created with owner-only permissions (directories `0700`,
  files `0600`).

**Always copy `bucket.sqlite` together with its `-wal` file**, or read it in
place: recently committed changes may live only in the WAL until SQLite
checkpoints them. Opening the database with any SQLite tool merges the WAL
correctly.

## 2. The bucket database

`bucket.sqlite` is an ordinary SQLite 3 database:

| Setting | Value |
|---|---|
| `PRAGMA application_id` | `1836671595` (`0x6D79626B`, ASCII `mybk`) |
| `PRAGMA user_version` | `2` (`1` in a format 1 bucket) |
| `journal_mode` | `WAL` |
| `auto_vacuum` | `INCREMENTAL` |
| per connection | `synchronous=FULL`, `foreign_keys=ON`, `busy_timeout` (default 10 s); on macOS also `fullfsync=ON` |

### 2.1 Tables

```sql
CREATE TABLE meta (
    k TEXT PRIMARY KEY NOT NULL,
    v NOT NULL
) WITHOUT ROWID;

CREATE TABLE blobs (
    id          INTEGER PRIMARY KEY,
    hash        BLOB NOT NULL UNIQUE CHECK (length(hash) = 32),
    size        INTEGER NOT NULL CHECK (size >= 0),
    stored_size INTEGER NOT NULL CHECK (stored_size >= 0),
    codec       INTEGER NOT NULL,
    dict_id     INTEGER,
    data        BLOB,
    created_at  INTEGER NOT NULL
);

CREATE TABLE objects (
    key          TEXT PRIMARY KEY NOT NULL,
    version      INTEGER NOT NULL,
    blob_id      INTEGER NOT NULL REFERENCES blobs(id),
    content_type TEXT,
    meta         TEXT,
    modified     INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX objects_blob ON objects(blob_id);

CREATE TABLE trash (
    key          TEXT NOT NULL,
    version      INTEGER NOT NULL,
    blob_id      INTEGER NOT NULL REFERENCES blobs(id),
    content_type TEXT,
    meta         TEXT,
    modified     INTEGER NOT NULL,
    trashed_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    PRIMARY KEY (key, version)
) WITHOUT ROWID;
CREATE INDEX trash_blob ON trash(blob_id);
CREATE INDEX trash_expires ON trash(expires_at);

-- format 2
CREATE TABLE dicts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    hash       BLOB NOT NULL CHECK (length(hash) = 32),
    zstd_id    INTEGER NOT NULL,
    data       BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    samples    INTEGER NOT NULL,
    prefix     TEXT NOT NULL
);

CREATE TABLE frames (
    blob_id      INTEGER NOT NULL REFERENCES blobs(id) ON DELETE CASCADE,
    start        INTEGER NOT NULL CHECK (start >= 0),
    size         INTEGER NOT NULL CHECK (size > 0),
    stored_start INTEGER NOT NULL CHECK (stored_start >= 0),
    stored_size  INTEGER NOT NULL CHECK (stored_size > 0),
    hash         BLOB NOT NULL CHECK (length(hash) = 32),
    PRIMARY KEY (blob_id, start)
) WITHOUT ROWID;

CREATE INDEX blobs_dict ON blobs(dict_id) WHERE dict_id IS NOT NULL;
```

All times are integers in **unix milliseconds, UTC**.

### 2.2 `meta`

| k | v |
|---|---|
| `format` | `2` (`1` in a format 1 bucket) |
| `created_at` | when the bucket was created |
| `trash_days` | how many days old versions are kept (`0` = not kept) |
| `last_version` | the last version number handed out in this bucket |
| `active_dict` | *format 2*, optional: the `dicts.id` new small objects are compressed with. Absent when the bucket uses no dictionary |
| `deleted` | only present while the bucket is being deleted (unix ms). mybucket writes it, then moves the bucket directory aside to `.deleting-*`; a program that still has the old database open sees it and stops writing. A database with this row is not a live bucket |

### 2.3 `blobs`: one row per distinct content

| Column | Meaning |
|---|---|
| `id` | internal row id, referenced by `objects` and `trash` |
| `hash` | 32-byte SHA-256 of the uncompressed content. Its lowercase hex is the object's **ETag**. Identical content is stored once per bucket |
| `size` | uncompressed size in bytes |
| `stored_size` | size of the stored bytes (what is in `data` or in the blob file) |
| `codec` | `0` = raw: the stored bytes **are** the content. `1` = zstd: the stored bytes are one standard zstd frame. `2` = zstd frames (*format 2*): several standard zstd frames, one per `frames` row, followed by a zstd *seek table* (section 2.8). Both zstd forms are what `zstd -d` reads |
| `dict_id` | `NULL`, or (*format 2*) the `dicts.id` of the dictionary the zstd frame was compressed with; decompress with `zstd -d -D <that dictionary>`. Always `NULL` in format 1 |
| `data` | the stored bytes when the blob is **inline**. `NULL` means the stored bytes are in the file `blobs/<h0h1>/<h2h3>/<hash>` |
| `created_at` | when this content was first stored |

How mybucket chooses (informative; readers do not need it):

- content of at most 4 MiB is compressed with zstd level 19 into one frame
  (`codec = 1`); when the bucket has an active dictionary and the content
  is at most 1 MiB, with that dictionary;
- larger content is cut into frames of 1 MiB of content, each compressed on
  its own with zstd level 9 (`codec = 2`), so any byte range can be read
  and checked without decompressing what comes before it;
- if compression saves less than 5 %, the raw bytes are stored (`codec = 0`).
  For content above 4 MiB this is decided on the way: after the first 16 MiB,
  mybucket keeps only the zstd copy if it is below 85 % of the input so far,
  or only the raw copy if it is above 95 %, so a large upload never needs
  twice its size in `tmp/`. A raw blob of more than 4 MiB still gets
  `frames` rows (with `stored_start = start`), for their hashes;
- stored bytes of at most 128 KiB (131072) are inline; larger ones go to a
  blob file.

When a write brings content whose stored copy is damaged (for example a
blob file with flipped bits), mybucket replaces the stored bytes of that
`blobs` row (and its `frames` rows) with the new, hash-checked ones; the
file path stays the same.

Blobs written by mybucket 0.1 keep their form after an upgrade: large ones
are a single `codec = 1` frame or raw, without `frames` rows. They read
exactly as before; a range read of one reads it whole to check it.

### 2.4 `objects`: the current version of every key

| Column | Meaning |
|---|---|
| `key` | the object key (see section 4). Sorted in byte order, which is also the listing order |
| `version` | a number from the bucket-wide counter `meta.last_version`; it only grows |
| `blob_id` | the content, `blobs.id` |
| `content_type` | MIME type, or `NULL` |
| `meta` | user metadata as compact JSON with sorted keys (`{"source":"example"}`), or `NULL` when empty. Names match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`, values are strings, at most 2048 bytes in total |
| `modified` | when this version was written |

A key that is not in `objects` does not exist (it may still have versions in
`trash`).

### 2.5 `trash`: previous versions

When a key is overwritten or deleted, its previous `objects` row is copied
here with `trashed_at` (when that happened), `expires_at`
(`trashed_at + trash_days * 86400000`) and `reason` (`overwritten` or
`deleted`). A trash entry keeps its content alive until garbage collection
removes it after `expires_at`. Restoring a version makes its content the
current version again under a **new** version number; `(key, version)` is
never reused.

### 2.6 References and garbage

A blob is in use while any `objects` or `trash` row points at its `id`.
There is no reference counter; the foreign keys enforce that a referenced
blob row cannot be deleted (and deleting a blob row deletes its `frames`
rows). Unreferenced blobs are exactly:

```sql
SELECT id, lower(hex(hash)) FROM blobs b
WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.blob_id = b.id)
  AND NOT EXISTS (SELECT 1 FROM trash t WHERE t.blob_id = b.id);
```

A dictionary is in use while it is `meta.active_dict` or any `blobs` row
names it; gc deletes the others. Dictionary ids are never reused.

### 2.7 `dicts`: trained dictionaries (format 2)

| Column | Meaning |
|---|---|
| `id` | the value `blobs.dict_id` refers to |
| `hash` | SHA-256 of `data` |
| `zstd_id` | the dictionary ID inside `data`; zstd writes it into every frame compressed with it, and refuses to decompress such a frame with another dictionary |
| `data` | a standard zstd dictionary, exactly what `zstd --train` writes and `zstd -D` reads |
| `created_at` | when it was trained |
| `samples`, `prefix` | how many objects it was trained on, and the key prefix they came from |

Rows are never changed. Training again adds a new row and makes it active;
stored blobs keep the dictionary they were compressed with.

### 2.8 `frames` and the seek table (format 2)

A `codec = 2` blob is a sequence of independent zstd frames. Each `frames`
row says that content bytes `[start, start + size)` are the zstd frame at
stored bytes `[stored_start, stored_start + stored_size)`, and `hash` is the
SHA-256 of those content bytes (so a range read checks exactly what it
returns). Frames are contiguous from 0 and every frame but the last holds
1 MiB (1048576 bytes) of content.

After the last frame comes a seek table in the [zstd seekable
format](https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md),
which is a zstd *skippable frame*, so `zstd -d` simply skips it. All
integers are little-endian:

```
magic 0x184D2A5E (4 bytes)  size of what follows (4 bytes)
for every frame: stored_size (4 bytes), size (4 bytes)
number of frames (4 bytes)  descriptor 0x00 (1 byte)  magic 0x8F92EAB1 (4 bytes)
```

For a large raw blob (`codec = 0`) the rows have `stored_start = start` and
`stored_size = size`; there is no seek table.

## 3. Blob files and crash safety

A blob file holds the stored bytes exactly: a single zstd frame when
`codec = 1`, the raw content when `codec = 0`. Files are never modified in
place. The only time a file at a blob path is replaced is a repair (section
2.3): a new file is renamed over a damaged one, under the write lock and in
the same transaction that updates its `blobs` row.

These rules make every state after a crash safe:

1. A writer streams new content into `tmp/*.part`, holds an exclusive
   `flock` on it while writing, and fsyncs it.
2. While holding the bucket's SQLite write lock (`BEGIN IMMEDIATE`), it
   renames the temp file to its blob path, fsyncs the directory, inserts the
   `blobs` row and the `objects` / `trash` rows, and commits.
3. A blob file is deleted only while holding the write lock, and only when
   no `blobs` row has its hash.

So a committed row always points to a complete file. A crash can leave:

- temp files in `tmp/` (garbage once nobody holds their `flock`);
- a blob file with no `blobs` row (garbage; written but never committed).

Neither is ever visible as an object. A file with no row must not be read
as data: its object was never committed.

## 4. Keys

A key is text in Unicode **NFC** form, 1 to 512 bytes in UTF-8, made of
segments separated by `/`. Each segment is 1 to 255 UTF-8 bytes, never
starts with `.` (so it is never `.` or `..`) and never starts or ends with
a space or other whitespace. Spaces inside a segment and any printable
character are allowed, except: control characters (U+0000-U+001F,
U+007F-U+009F), the backslash, the invisible direction marks and overrides
U+061C, U+200B, U+200E, U+200F, U+202A-U+202E and U+2066-U+2069, the line
and paragraph separators U+2028 and U+2029, and U+FEFF. There is no
leading or trailing `/` and no empty segment. Every key is therefore a safe
relative path: writing an object to `<dir>/<key>` can never leave `<dir>`
or create a hidden file. Format 1 keys were the ASCII subset
`A-Z a-z 0-9 . _ ~ @ + = , -` of these rules, so every format 1 key is valid.

Keys are stored as SQLite `TEXT` (UTF-8) and sorted by SQLite's default
`BINARY` collation, which is byte order of the UTF-8, the same as Unicode
code point order.

Keys that differ only in letter case (`A.txt` and `a.txt`, `É` and `é`) are
different keys. On a case-insensitive filesystem (default macOS APFS) they
would collide as files.

## 5. Reading data with only `sqlite3` and `zstd`

**Work on a copy.** Stop every program that uses the store, then copy the
bucket directory (with `bucket.sqlite-wal` if it is there) and read the
copy. That way nothing you type can change the original, and it works with
every `sqlite3`: the one that ships with macOS (`/usr/bin/sqlite3`) cannot
open an idle WAL-mode database with `-readonly` and fails with "unable to
open database file". Do not use `?immutable=1` on the original instead: it
ignores the WAL and can miss recent changes.

### 5.1 Worked example

A bucket `notes` with two objects. One JSON object was overwritten once, so
its first version is in the trash:

```console
$ cp -R store/buckets/notes /tmp/notes-copy && cd /tmp/notes-copy && mkdir -p /tmp/out
$ sqlite3 -header -column bucket.sqlite \
    "SELECT key, version, blob_id, content_type, meta, modified FROM objects;"
key                  version  blob_id  content_type      meta                  modified
-------------------  -------  -------  ----------------  --------------------  -------------
2026/11/14/day.json  3        3        application/json  {"source":"example"}  1794787200000
photos/cat.jpg       2        2        image/jpeg                              1794787200000

$ sqlite3 -header -column bucket.sqlite \
    "SELECT id, lower(hex(hash)) AS hash, size, stored_size, codec, dict_id, data IS NULL AS in_file FROM blobs;"
id  hash                                                              size    stored_size  codec  dict_id  in_file
--  ----------------------------------------------------------------  ------  -----------  -----  -------  -------
1   285e41c00de9877a9d81086e95a955f318e8cf886c3cca6eec7bf5bbf609f47f  1292    223          1               0
2   344a806bb4a1637c05370a18c1317bb846dc791dc5e48beec9c936352d3ec8d5  200000  200000       0               1
3   550c33e54b8bb8ec140772a7899afaf567a8c3cd1029d18967346115918ca0c4  1292    220          1               0

$ sqlite3 -header -column bucket.sqlite \
    "SELECT key, version, blob_id, reason, trashed_at, expires_at FROM trash;"
key                  version  blob_id  reason       trashed_at     expires_at
-------------------  -------  -------  -----------  -------------  -------------
2026/11/14/day.json  1        1        overwritten  1794787200000  1795392000000
```

**An inline, compressed object** (`codec = 1`, `in_file = 0`): write the
stored bytes out of the row with the CLI's `writefile()`, then decompress:

```console
$ sqlite3 bucket.sqlite \
    "SELECT writefile('/tmp/out/day.json.zst', b.data) FROM objects o JOIN blobs b ON b.id = o.blob_id
     WHERE o.key = '2026/11/14/day.json';"
220
$ zstd -d -q /tmp/out/day.json.zst -o /tmp/out/day.json
$ head -2 /tmp/out/day.json
{"time": "2026-11-14T00:00", "steps": 1957, "hr": 62}
{"time": "2026-11-14T01:00", "steps": 187, "hr": 65}
$ shasum -a 256 /tmp/out/day.json        # equals blobs.hash: the content is intact
550c33e54b8bb8ec140772a7899afaf567a8c3cd1029d18967346115918ca0c4  /tmp/out/day.json
```

**A file blob stored raw** (`codec = 0`, `in_file = 1`): the file is the
content; just copy it:

```console
$ cp blobs/34/4a/344a806bb4a1637c05370a18c1317bb846dc791dc5e48beec9c936352d3ec8d5 /tmp/out/cat.jpg
```

For a file blob with `codec = 1` or `codec = 2`, run
`zstd -d -c <blob file> > <output>`: zstd decompresses one frame after the
other and skips the seek table. For an inline blob with `codec = 0`,
`writefile()` already gives the content.

**An object compressed with a dictionary** (`dict_id` is not empty): write
the dictionary out of `dicts` once, then pass it to zstd with `-D`:

```console
$ sqlite3 bucket.sqlite "SELECT o.key, b.dict_id FROM objects o JOIN blobs b ON b.id = o.blob_id WHERE b.dict_id IS NOT NULL LIMIT 1;"
logs/2026-11-14.json|1
$ sqlite3 bucket.sqlite "SELECT writefile('/tmp/out/dict-1', data) FROM dicts WHERE id = 1;"
32768
$ sqlite3 bucket.sqlite \
    "SELECT writefile('/tmp/out/log.json.zst', b.data) FROM objects o JOIN blobs b ON b.id = o.blob_id
     WHERE o.key = 'logs/2026-11-14.json';"
127
$ zstd -d -q -D /tmp/out/dict-1 /tmp/out/log.json.zst -o /tmp/out/log.json
```

Without `-D`, zstd stops with "Dictionary mismatch" instead of producing
wrong bytes.

**An old version from the trash**: same steps, joining `trash` instead of
`objects`:

```sql
SELECT writefile('/tmp/out/day.v1.json.zst', b.data) FROM trash t JOIN blobs b ON b.id = t.blob_id
WHERE t.key = '2026/11/14/day.json' AND t.version = 1;
```

### 5.2 Recovering a whole bucket

This POSIX shell script writes every current object to `<out>/<key>`
(keys are safe paths, see section 4) and checks each file's SHA-256. It
works on a copy of the database, so it never changes the bucket, and reads
format 1 and format 2 buckets, dictionaries and large objects included.
Keys that cannot all exist as files in `<out>` are reported on stderr and
skipped while the rest is recovered, and the script then exits 1:

- a key that is also a folder of other keys (`dir` and `dir/child`);
- keys that differ only in letter case (`A.txt` and `a.txt`) when `<out>` is
  on a case-insensitive filesystem (the macOS default). Recover into a
  case-sensitive volume, or recover the skipped keys one by one with the
  steps of section 5.1.

`sqlite3` prints the columns separated by the control character U+001F,
which no key can contain, and the key comes last, so spaces, `|` and any
other characters in keys are safe.

```sh
#!/bin/sh
# Usage: recover.sh <bucket directory> <output directory>
# Writes every current object of the bucket to <output directory>/<key> and
# checks its SHA-256. Stop every program that uses the store first.
# Exits 0 when every object was recovered, 1 when some were skipped (each
# one is reported on stderr with the reason).
set -eu
bucket=$1
out=$2
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
case "$work" in *"'"*) echo "the temp folder name $work contains a quote" >&2; exit 1 ;; esac

# Work on a copy of the database and its WAL: this never changes the bucket,
# and works with every sqlite3 (the one in macOS cannot open an idle WAL
# database with -readonly).
cp -- "$bucket/bucket.sqlite" "$work/db"
if [ -f "$bucket/bucket.sqlite-wal" ]; then cp -- "$bucket/bucket.sqlite-wal" "$work/db-wal"; fi
# Format 2: every trained dictionary as a file, for zstd -D.
if [ "$(sqlite3 "$work/db" "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'dicts';")" = 1 ]; then
  for id in $(sqlite3 "$work/db" "SELECT id FROM dicts;"); do
    sqlite3 "$work/db" "SELECT writefile('$work/dict-$id', data) FROM dicts WHERE id = $id;" > /dev/null
  done
fi
sep=$(printf '\037')
sqlite3 -separator "$sep" "$work/db" "SELECT b.codec, b.data IS NULL, lower(hex(b.hash)), coalesce(b.dict_id, ''), o.key
  FROM objects o JOIN blobs b ON b.id = o.blob_id ORDER BY o.key;" > "$work/list"

sha256() { if command -v sha256sum >/dev/null; then sha256sum < "$1"; else shasum -a 256 < "$1"; fi | cut -d' ' -f1; }
skip() { echo "skipped $1: $2" >&2; failed=1; }

failed=0
mkdir -p -- "$out"
while IFS=$sep read -r codec in_file hash dict key; do
  dest="$out/$key"
  case "$key" in */*) dir="$out/${key%/*}" ;; *) dir="$out" ;; esac
  if [ -e "$dest" ] || [ -L "$dest" ]; then
    skip "$key" "$dest already exists (on a case-insensitive disk, a key that differs only in letter case)"
    continue
  fi
  if ! mkdir -p -- "$dir" 2>/dev/null || [ ! -d "$dir" ]; then
    skip "$key" "the folder $dir cannot be created (another key is a file at that path)"
    continue
  fi
  if [ "$in_file" = 1 ]; then
    src="$bucket/blobs/$(echo "$hash" | cut -c1-2)/$(echo "$hash" | cut -c3-4)/$hash"
  else
    src="$work/stored"
    sqlite3 "$work/db" "SELECT writefile('$src', data) FROM blobs WHERE hash = x'$hash';" > /dev/null
  fi
  if [ "$codec" = 0 ]; then
    cp -- "$src" "$dest" || { rm -f -- "$dest"; skip "$key" "its stored file cannot be copied"; continue; }
  elif [ -n "$dict" ]; then
    zstd -q -d -c -D "$work/dict-$dict" -- "$src" > "$dest" || { rm -f -- "$dest"; skip "$key" "zstd could not decompress it with dictionary $dict"; continue; }
  else
    zstd -q -d -c -- "$src" > "$dest" || { rm -f -- "$dest"; skip "$key" "zstd could not decompress it"; continue; }
  fi
  [ "$(sha256 "$dest")" = "$hash" ] || skip "$key" "its content does not match its SHA-256 (damaged; kept as $dest)"
done < "$work/list"
exit "$failed"
```

The script needs `sqlite3`, `zstd`, and `sha256sum` or `shasum`. A failure
of `sqlite3` itself (for example a damaged database) stops it at once with a
non-zero exit status. tests/test_format_doc.py runs this exact script, on
format 1 and format 2 buckets with dictionaries, large objects and keys with
spaces and non-ASCII characters.

### 5.3 Export manifest

`mybucket export` (and `export_dir()`) writes plain files plus one hidden
file, `.mybucket-export.jsonl`, at the top of the export folder. It is
JSON Lines (UTF-8). Line 1 is a header:

```json
{"bucket": "notes", "exported_at": 1794787200000, "format": "mybucket-export", "prefix": "", "version": 1}
```

and then one line per exported object, in key order:

```json
{"content_type": "application/json", "etag": "550c33e5...", "key": "2026/11/14/day.json", "metadata": {"source": "example"}, "modified": 1794787200000, "size": 1292, "version": 3}
```

`etag` is the full 64-character SHA-256 of the file's content, `size` its
length, `modified` the object's time (the file's mtime is set to it too).
The manifest is replaced at the end of each export and describes the last
one. `mybucket import` reads it, when present, to restore content types and
metadata; its name starts with `.` so it is never a key.

## 6. Upgrading from format 1

mybucket 0.2 reads format 1 buckets as they are, and never changes a bucket
it opens read-only. The first time it opens a format 1 bucket for writing,
it upgrades it in **one SQLite transaction**:

```sql
-- the "format 2" statements of section 2.1 (CREATE TABLE dicts, CREATE TABLE frames, CREATE INDEX blobs_dict)
UPDATE meta SET v = 2 WHERE k = 'format';
PRAGMA user_version = 2;
```

Nothing else changes: no object, blob row or blob file is rewritten, so
every format 1 object reads back byte for byte, and the recovery script of
section 5.2 works before and after. A crash during the upgrade leaves the
bucket in format 1. After it, mybucket 0.1 refuses the bucket ("uses format
2"); it would otherwise see `codec = 2` and dictionary blobs it cannot read.
Take a snapshot first if you may need to go back to 0.1.

## 7. Compatibility rules

- Readers must check `PRAGMA application_id` and refuse `meta.format`
  values greater than they know.
- Readers must refuse blobs with an unknown `codec`, and must not decode a
  blob whose `dict_id` names a dictionary they cannot find (that is damage,
  not something to guess around).
- Later formats may add tables and columns. They will bump `format` when an
  older reader could otherwise misread data.
