"""Replication: make the current objects of one store's buckets appear in another.

``sync(src, dst)`` copies every current object of the source buckets to the
destination, where it is missing or different (content, content type or
metadata), and with ``delete=True`` also removes destination objects that
the source no longer has (they go to the destination's trash, like any
delete). Either side may be local or remote (``ssh://``). It is the
off-machine backup path.

Properties:

- **incremental**: both buckets are listed in key order and compared by
  ETag (SHA-256), content type and metadata; only differences are sent.
  Listings are paged, so memory does not grow with the bucket;
- **verified**: every object is written with ``expected_sha256`` and
  ``expected_size`` taken from the source's own record of the version being
  read, so the destination refuses (and does not write) anything that does
  not hash to the source's ETag. The source checks its stored copy while it
  is read; a damaged source object is reported and skipped, never copied;
- **safe to interrupt**: every write is an atomic put or batch of puts
  (at most 500 objects or 32 MiB), so a crash, kill or lost connection
  leaves the destination with some objects updated and the rest as they
  were, never a partial object. Run it again to finish;
- **safe next to other writers**: every write is conditional on the
  destination object being exactly what the listing saw (``if_match`` /
  ``if_none_match``); one that changed in the meantime is reported as a
  conflict and left alone;
- only current objects are copied. Trash and old versions stay on each side,
  and each side keeps its own settings (a new destination bucket gets the
  source's ``trash_days``).

Public:
    sync(src, dst, buckets=None, prefix="", delete=False, dry_run=False) -> dict
"""

from __future__ import annotations

from collections.abc import Iterator

from . import keys as _keys
from .errors import (
    BucketNotFound,
    ContentMismatch,
    DamagedError,
    InvalidArgument,
    ObjectNotFound,
    PreconditionFailed,
    UnsupportedFormat,
)
from .models import ObjectInfo

BATCH_OPS = 500
BATCH_BYTES = 32 * 1024 * 1024
#: Objects up to this size are copied in batches; larger ones are streamed one by one.
SMALL = 4 * 1024 * 1024
_MAX_LISTED = 50
_PAGE = 1000

#: Test hook: called with "committed" after every destination write.
_hook = None


def _plural(n: int, word: str, many: str | None = None) -> str:
    return f"{n:,} {word if n == 1 else (many or word + 's')}"


def _iter_objects(bucket, prefix: str) -> Iterator[ObjectInfo]:
    start_after = None
    while True:
        page = bucket.list(prefix=prefix, limit=_PAGE, start_after=start_after)
        yield from page.objects
        if not page.truncated:
            return
        start_after = page.next_start_after


def _same(a: ObjectInfo, b: ObjectInfo) -> bool:
    return a.etag == b.etag and a.content_type == b.content_type and (a.metadata or {}) == (b.metadata or {})


class _BucketSync:
    def __init__(self, src, dst, name: str, prefix: str, delete: bool, dry_run: bool) -> None:
        self.src = src
        self.dst = dst
        self.name = name
        self.prefix = prefix
        self.delete = delete
        self.dry_run = dry_run
        self.pending: list[tuple] = []  # ("put", key, data, info, dst_etag) / ("delete", key, dst_etag)
        self.pending_bytes = 0
        self.r = {
            "bucket": name,
            "created_bucket": False,
            "copied": 0,
            "updated": 0,
            "deleted": 0,
            "unchanged": 0,
            "extra_in_destination": 0,
            "gone": 0,
            "bytes_copied": 0,
            "conflicts": [],
            "damaged": [],
            "conflicts_count": 0,
            "damaged_count": 0,
        }

    def problem(self, kind: str, key: str, reason: str) -> None:
        self.r[f"{kind}_count"] += 1
        if len(self.r[kind]) < _MAX_LISTED:
            self.r[kind].append({"key": key, "reason": reason})

    def run(self) -> dict:
        src_objects = _iter_objects(self.src, self.prefix)
        dst_objects = _iter_objects(self.dst, self.prefix) if self.dst is not None else iter(())
        a = next(src_objects, None)
        b = next(dst_objects, None)
        while a is not None or b is not None:
            if b is None or (a is not None and a.key < b.key):
                self.copy(a, None)
                a = next(src_objects, None)
            elif a is None or b.key < a.key:
                self.extra(b)
                b = next(dst_objects, None)
            else:
                if _same(a, b):
                    self.r["unchanged"] += 1
                else:
                    self.copy(a, b)
                a = next(src_objects, None)
                b = next(dst_objects, None)
        self.flush()
        return self.r

    def extra(self, info: ObjectInfo) -> None:
        if not self.delete:
            self.r["extra_in_destination"] += 1
            return
        if self.dry_run:
            self.r["deleted"] += 1
            return
        self.pending.append(("delete", info.key, info.etag))
        if len(self.pending) >= BATCH_OPS:
            self.flush()

    def copy(self, info: ObjectInfo, current: ObjectInfo | None) -> None:
        counter = "copied" if current is None else "updated"
        if self.dry_run:
            self.r[counter] += 1
            self.r["bytes_copied"] += info.size
            return
        dst_etag = None if current is None else current.etag
        if info.size > SMALL:
            self.flush()
            self.copy_streamed(info.key, dst_etag, counter)
            return
        try:
            with self.src.open(info.key) as stream:
                data = stream.read()
                fresh = stream.info
        except ObjectNotFound:
            self.r["gone"] += 1
            return
        except (DamagedError, UnsupportedFormat) as exc:
            self.problem("damaged", info.key, exc.message)
            return
        self.pending.append(("put", info.key, data, fresh, dst_etag, counter))
        self.pending_bytes += len(data)
        if len(self.pending) >= BATCH_OPS or self.pending_bytes >= BATCH_BYTES:
            self.flush()

    def _conditions(self, dst_etag: str | None) -> dict:
        return {"if_none_match": "*"} if dst_etag is None else {"if_match": dst_etag}

    def copy_streamed(self, key: str, dst_etag: str | None, counter: str) -> None:
        try:
            with self.src.open(key) as stream:
                info = stream.info
                self.dst.put(
                    key,
                    stream,
                    content_type=info.content_type,
                    metadata=info.metadata or None,
                    expected_size=info.size,
                    expected_sha256=info.etag,
                    **self._conditions(dst_etag),
                )
        except ObjectNotFound:
            self.r["gone"] += 1  # deleted in the source after it was listed
            return
        except PreconditionFailed:
            self.problem("conflicts", key, "it changed in the destination while sync ran; it was left alone")
            return
        except (DamagedError, UnsupportedFormat, ContentMismatch) as exc:
            self.problem("damaged", key, exc.message)
            return
        self.r[counter] += 1
        self.r["bytes_copied"] += info.size
        _point()

    def flush(self) -> None:
        ops, self.pending, self.pending_bytes = self.pending, [], 0
        if not ops:
            return
        try:
            with self.dst.batch() as tx:
                for op in ops:
                    if op[0] == "put":
                        _, key, data, info, dst_etag, _counter = op
                        tx.put(
                            key,
                            data,
                            content_type=info.content_type,
                            metadata=info.metadata or None,
                            expected_size=info.size,
                            expected_sha256=info.etag,
                            **self._conditions(dst_etag),
                        )
                    else:
                        _, key, dst_etag = op
                        tx.delete(key, if_match=dst_etag, missing_ok=False)
        except (PreconditionFailed, ObjectNotFound):
            # Something changed in the destination meanwhile: apply one by one, so
            # only the operations that conflict are left out.
            for op in ops:
                self.apply_one(op)
            return
        for op in ops:
            self.count(op)
        _point()

    def apply_one(self, op: tuple) -> None:
        try:
            if op[0] == "put":
                _, key, data, info, dst_etag, _counter = op
                self.dst.put(
                    key,
                    data,
                    content_type=info.content_type,
                    metadata=info.metadata or None,
                    expected_size=info.size,
                    expected_sha256=info.etag,
                    **self._conditions(dst_etag),
                )
            else:
                _, key, dst_etag = op
                self.dst.delete(key, if_match=dst_etag)
        except (PreconditionFailed, ObjectNotFound):
            self.problem("conflicts", op[1], "it changed in the destination while sync ran; it was left alone")
            return
        self.count(op)
        _point()

    def count(self, op: tuple) -> None:
        if op[0] == "put":
            self.r[op[5]] += 1
            self.r["bytes_copied"] += op[3].size
        else:
            self.r["deleted"] += 1


def _point() -> None:
    if _hook is not None:
        _hook("committed")


def _human(n: int) -> str:
    value = float(n)
    for unit in ("bytes", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{int(value)} bytes" if unit == "bytes" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{n} bytes"  # pragma: no cover


def _same_store(src, dst) -> bool:
    if getattr(src, "is_remote", False) or getattr(dst, "is_remote", False):
        return getattr(src, "url", None) == getattr(dst, "url", None)
    try:
        return src.path.resolve() == dst.path.resolve()
    except OSError:
        return False


def sync(src, dst, buckets: list[str] | None = None, prefix: str = "", delete: bool = False, dry_run: bool = False) -> dict:
    """Copy the current objects of ``src``'s buckets (or of ``buckets``) to ``dst``; see the module docstring.

    ``src`` and ``dst`` are open stores (local or remote). ``prefix`` limits
    both sides to keys starting with it (``delete`` then only removes keys
    under it). ``dry_run`` reports what would change without reading or
    writing any object. Returns a report with one entry per bucket; ``ok``
    is False when an object could not be copied (damaged in the source, or
    changed in the destination while sync ran).
    """
    prefix = _keys.validate_prefix(prefix)
    if _same_store(src, dst):
        raise InvalidArgument("The source and the destination are the same store.", hint="Give two different stores.")
    if not dry_run:
        dst._require_writable()
    names = list(src.list_buckets()) if buckets is None else [_keys.validate_bucket_name(n) for n in buckets]
    reports = []
    for name in names:
        source = src.bucket(name)
        try:
            target = dst.bucket(name)
            created = False
        except BucketNotFound:
            if dry_run:
                target, created = None, True
            else:
                target, created = dst.create_bucket(name, trash_days=source.trash_days), True
        job = _BucketSync(source, target, name, prefix, delete, dry_run)
        report = job.run()
        report["created_bucket"] = created
        reports.append(report)

    def total(field: str) -> int:
        return sum(r[field] for r in reports)

    changed = total("copied") + total("updated")
    problems = total("damaged_count") + total("conflicts_count")
    verb = "Would copy" if dry_run else "Copied"
    summary = (
        f"{verb} {_plural(changed, 'object')} ({_human(total('bytes_copied'))}) in {_plural(len(reports), 'bucket')}: "
        f"{total('copied'):,} new, {total('updated'):,} changed, {total('unchanged'):,} already up to date"
    )
    if delete:
        summary += f", {total('deleted'):,} {'to delete' if dry_run else 'deleted'} from the destination"
    elif total("extra_in_destination"):
        summary += f"; {total('extra_in_destination'):,} only in the destination (kept; --delete removes them)"
    summary += "."
    if total("gone"):
        summary += f" {_plural(total('gone'), 'object')} disappeared from the source while sync ran."
    hint = ""
    if problems:
        summary += (
            f" {_plural(total('damaged_count'), 'object')} damaged in the source and "
            f"{_plural(total('conflicts_count'), 'object')} changed in the destination meanwhile were not copied."
        )
        hint = "Run verify on the source for damaged objects; run sync again for the others."
    return {
        "ok": problems == 0,
        "dry_run": dry_run,
        "buckets": reports,
        "copied": total("copied"),
        "updated": total("updated"),
        "deleted": total("deleted"),
        "unchanged": total("unchanged"),
        "bytes_copied": total("bytes_copied"),
        "summary": summary,
        "hint": hint,
    }


__all__ = ["sync"]
