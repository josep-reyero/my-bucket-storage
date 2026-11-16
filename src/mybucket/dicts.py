"""Trained zstd dictionaries: many small, similar objects compress much better.

A dictionary is trained from a sample of a bucket's own small objects. It is
stored in the bucket (``dicts`` table, format 2) and becomes the bucket's
*active* dictionary: from then on every new object of at most
``codec.DICT_MAX`` bytes (1 MiB) is compressed with it, and ``blobs.dict_id``
records which dictionary a blob needs. Existing small objects are
recompressed with it too (``recompress=True``), so the saving applies to what
is already stored.

Safety:

- A dictionary row is never changed. Training again adds a new one and makes
  it active; blobs keep pointing at the dictionary they were compressed
  with. gc deletes a dictionary only when it is neither active nor used by
  any blob (ids are never reused: AUTOINCREMENT).
- A writer that compressed with a dictionary that gc removed before its
  commit notices inside its write transaction and recompresses without it
  (``Bucket._keep_dictionary``).
- Recompression reads each blob through the normal verified reader, checks
  that the new bytes decompress back to the same content, and replaces the
  row only if it is unchanged since it was read (one transaction per batch),
  so a crash or a concurrent writer can never leave a wrong row. Only inline
  blobs are recompressed: no blob file is touched.
- Objects compressed with a dictionary stay readable with the zstd
  command-line tool: ``zstd -d -D <dictionary file>`` (docs/FORMAT.md).

Public:
    train_dictionary(bucket, prefix="", dict_size=32768, max_samples=2000, recompress=True) -> dict
    dictionaries(bucket) -> list[DictInfo]
    set_dictionary_use(bucket, enabled) -> None    (Bucket.configure(use_dictionary=...))
"""

from __future__ import annotations

import hashlib
import random
import sqlite3
from typing import TYPE_CHECKING

from . import _zstd
from . import keys as _keys
from .codec import BLOB_COLUMNS, CODEC_FRAMES, CODEC_ZSTD, DICT_MAX, LEVEL_SMALL, BlobReader, BlobRow
from .errors import DamagedError, InvalidArgument, NotFound, ObjectNotFound, Refused
from .models import DictInfo
from .schema import translate_sqlite_error

if TYPE_CHECKING:
    from .bucket import Bucket

DEFAULT_DICT_SIZE = 32 * 1024
DEFAULT_MAX_SAMPLES = 2000
#: Objects larger than this are not used as training samples.
SAMPLE_MAX = 128 * 1024
MAX_SAMPLE_BYTES = 32 * 1024 * 1024
MIN_SAMPLES = 20
#: A new dictionary is kept only if it saves at least this share on objects it was not trained on.
MIN_SAVING = 0.05
_RECOMPRESS_BATCH = 200


def _plural(n: int, word: str, many: str | None = None) -> str:
    return f"{n:,} {word if n == 1 else (many or word + 's')}"


def _check_int(name: str, value: object, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise InvalidArgument(f"{name} must be a whole number from {low} to {high}, got {value!r}.")
    return value


def _sample_keys(bucket: Bucket, prefix: str, max_samples: int) -> tuple[list[str], int]:
    """A deterministic random sample of keys of small objects under ``prefix``, and how many there were."""
    rng = random.Random(20261116)
    chosen: list[str] = []
    seen = 0
    start_after = None
    while True:
        page = bucket.list(prefix=prefix, limit=1000, start_after=start_after)
        for info in page.objects:
            if not 0 < info.size <= SAMPLE_MAX:
                continue
            seen += 1
            if len(chosen) < max_samples:
                chosen.append(info.key)
            else:
                j = rng.randrange(seen)
                if j < max_samples:
                    chosen[j] = info.key
        if not page.truncated:
            return sorted(chosen), seen
        start_after = page.next_start_after


def train_dictionary(
    bucket: Bucket,
    prefix: str = "",
    dict_size: int = DEFAULT_DICT_SIZE,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    recompress: bool = True,
) -> dict:
    """Train a dictionary from the bucket's small objects (under ``prefix``) and start using it.

    Every fifth sample is held back to measure the saving honestly; the
    dictionary is kept only when it saves at least 5 % on those. Returns a
    report with ``kept``, the measured sizes, and (with ``recompress``) how
    many stored objects were recompressed and the bytes saved.
    """
    bucket._require_writable()
    prefix = _keys.validate_prefix(prefix)
    dict_size = _check_int("dict_size", dict_size, 1024, 1024 * 1024)
    max_samples = _check_int("max_samples", max_samples, MIN_SAMPLES, 100_000)
    name = bucket.name

    keys, candidates = _sample_keys(bucket, prefix, max_samples)
    samples: list[bytes] = []
    total = 0
    for key in keys:
        try:
            data = bucket.get(key)
        except (ObjectNotFound, DamagedError):
            continue  # deleted meanwhile, or damaged (verify reports it): not a sample
        if total + len(data) > MAX_SAMPLE_BYTES:
            break
        samples.append(data)
        total += len(data)
    where = f" under {prefix!r}" if prefix else ""
    if len(samples) < MIN_SAMPLES:
        raise Refused(
            f"Bucket {name!r} has {_plural(len(samples), 'small object')}{where}; a dictionary needs at "
            f"least {MIN_SAMPLES} (of at most {SAMPLE_MAX // 1024} KiB each).",
            hint="Dictionaries pay off for hundreds or thousands of small, similar objects. Train again later.",
        )
    held_out = samples[4::5] if len(samples) >= 50 else samples
    training = [s for i, s in enumerate(samples) if i % 5 != 4] if len(samples) >= 50 else samples
    try:
        content = _zstd.train(training, dict_size)
        dictionary = _zstd.Dictionary(content)
    except (_zstd.ZstdError, ValueError) as exc:
        raise Refused(
            f"Could not train a dictionary from {_plural(len(training), 'object')} of bucket {name!r}{where} ({exc}).",
            hint="Dictionaries need many small, similar objects; try more objects or a smaller --dict-size.",
        ) from None
    plain = sum(len(_zstd.compress(s, LEVEL_SMALL)) for s in held_out)
    with_dict = sum(len(_zstd.compress(s, LEVEL_SMALL, dictionary)) for s in held_out)
    saving = 1.0 - with_dict / plain if plain else 0.0
    report = {
        "bucket": name,
        "prefix": prefix,
        "kept": False,
        "dict_id": None,
        "dict_bytes": len(content),
        "candidates": candidates,
        "samples": len(training),
        "sample_bytes": sum(len(s) for s in training),
        "test_objects": len(held_out),
        "test_bytes_without": plain,
        "test_bytes_with": with_dict,
        "saving": round(saving, 4),
        "recompressed": 0,
        "bytes_saved": 0,
        "summary": "",
    }
    if saving < MIN_SAVING:
        report["summary"] = (
            f"A dictionary trained on {_plural(len(training), 'object')} of {name!r}{where} would save only "
            f"{saving:.1%} on the objects held back to test it, so it was not kept; nothing changed."
        )
        return report

    digest = hashlib.sha256(content).digest()
    with bucket._write_txn(f"store a dictionary in bucket {name!r}") as txn:
        cur = txn.conn.execute(
            "INSERT INTO dicts (hash, zstd_id, data, created_at, samples, prefix) VALUES (?, ?, ?, ?, ?, ?)",
            (digest, dictionary.zstd_id, content, txn.now, len(training), prefix),
        )
        dict_id = int(cur.lastrowid)  # type: ignore[arg-type]
        txn.conn.execute("INSERT OR REPLACE INTO meta (k, v) VALUES ('active_dict', ?)", (dict_id,))
    report["kept"] = True
    report["dict_id"] = dict_id
    summary = (
        f"Trained dictionary {dict_id} ({len(content):,} bytes) on {_plural(len(training), 'object')} of "
        f"{name!r}{where}; on {_plural(len(held_out), 'object')} held back to test it, it saves {saving:.1%}. "
        f"New objects of up to {DICT_MAX // (1024 * 1024)} MiB are now compressed with it."
    )
    if recompress:
        done, saved = _recompress(bucket, dict_id)
        report["recompressed"], report["bytes_saved"] = done, saved
        summary += f" Recompressed {_plural(done, 'stored object')}, saving {saved:,} bytes."
        from .maintenance import _vacuum

        _vacuum(bucket)
    report["summary"] = summary
    return report


def _recompress(bucket: Bucket, dict_id: int) -> tuple[int, int]:
    """Recompress inline blobs with dictionary ``dict_id`` where that makes them smaller."""
    with bucket._locked_conn() as conn:
        loaded = bucket._load_dict(conn, dict_id)
    if loaded is None:
        return 0, 0
    done = saved = 0
    after = 0
    while True:
        try:
            with bucket._locked_conn() as conn:
                rows = [
                    BlobRow(*r)
                    for r in conn.execute(
                        f"SELECT {BLOB_COLUMNS} FROM blobs WHERE id > ? AND data IS NOT NULL AND size > 0 "
                        "AND size <= ? AND codec != ? AND dict_id IS NOT ? ORDER BY id LIMIT ?",
                        (after, DICT_MAX, CODEC_FRAMES, dict_id, _RECOMPRESS_BATCH),
                    )
                ]
                parts = [bucket._read_parts(conn, row) for row in rows]
        except sqlite3.Error as exc:
            raise translate_sqlite_error(exc, f"read bucket {bucket.name!r}") from exc
        except DamagedError:
            parts = None  # an old dictionary is damaged: handle rows one by one below
        if not rows:
            return done, saved
        after = rows[-1].id
        updates = []
        for i, row in enumerate(rows):
            try:
                if parts is None:
                    with bucket._locked_conn() as conn:
                        frames, old = bucket._read_parts(conn, row)
                else:
                    frames, old = parts[i]
                with BlobReader(bucket.path, row, frames=frames, dictionary=old) as reader:
                    raw = reader.readall()  # verified: damaged content is never recompressed
            except (DamagedError, NotFound, OSError):
                continue
            packed = _zstd.compress(raw, LEVEL_SMALL, loaded.dictionary)
            if len(packed) >= row.stored_size:
                continue
            if _zstd.Decompressor(loaded.dictionary).decompress(packed, len(raw) + 1) != raw:
                continue  # never store bytes that do not read back
            updates.append((packed, row))
        if not updates:
            continue
        with bucket._write_txn(f"recompress objects of bucket {bucket.name!r}") as txn:
            if txn.conn.execute("SELECT hash FROM dicts WHERE id = ?", (dict_id,)).fetchone() != (loaded.hash,):
                return done, saved  # the dictionary was replaced and removed meanwhile: stop
            for packed, row in updates:
                changed = txn.conn.execute(
                    "UPDATE blobs SET data = ?, stored_size = ?, codec = ?, dict_id = ? "
                    "WHERE id = ? AND hash = ? AND data IS NOT NULL AND stored_size = ? AND codec = ? AND dict_id IS ?",
                    (packed, len(packed), CODEC_ZSTD, dict_id, row.id, row.hash, row.stored_size, row.codec, row.dict_id),
                ).rowcount
                if changed:
                    done += 1
                    saved += row.stored_size - len(packed)


def dictionaries(bucket: Bucket) -> list[DictInfo]:
    """Every dictionary stored in the bucket, oldest first."""
    try:
        with bucket._read_txn() as conn:
            if not bucket._frames_table(conn):
                return []
            active = bucket._meta_value(conn, "active_dict")
            rows = conn.execute(
                "SELECT d.id, d.zstd_id, length(d.data), d.created_at, d.samples, d.prefix, "
                "(SELECT count(*) FROM blobs b WHERE b.dict_id = d.id), "
                "(SELECT coalesce(sum(b.stored_size), 0) FROM blobs b WHERE b.dict_id = d.id) "
                "FROM dicts d ORDER BY d.id"
            ).fetchall()
    except sqlite3.Error as exc:
        raise translate_sqlite_error(exc, f"read bucket {bucket.name!r}") from exc
    return [
        DictInfo(
            id=r[0],
            zstd_id=r[1],
            size=r[2],
            created_at=r[3],
            samples=r[4],
            prefix=r[5],
            active=active is not None and int(active) == r[0],
            blobs=r[6],
            stored_bytes=r[7],
        )
        for r in rows
    ]


def set_dictionary_use(bucket: Bucket, enabled: bool) -> None:
    """Stop (False) or resume (True: the newest dictionary) compressing new objects with a dictionary."""
    with bucket._write_txn(f"configure bucket {bucket.name!r}") as txn:
        if not enabled:
            txn.conn.execute("DELETE FROM meta WHERE k = 'active_dict'")
            return
        row = txn.conn.execute("SELECT max(id) FROM dicts").fetchone()
        if row is None or row[0] is None:
            raise InvalidArgument(
                f"Bucket {bucket.name!r} has no dictionary to use.",
                hint="Train one first (bucket.train_dictionary() or 'mybucket dict train BUCKET').",
            )
        txn.conn.execute("INSERT OR REPLACE INTO meta (k, v) VALUES ('active_dict', ?)", (int(row[0]),))


__all__ = ["train_dictionary", "dictionaries", "set_dictionary_use"]
