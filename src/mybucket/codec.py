"""Turning caller input into stored blobs, and stored blobs back into bytes.

Write side ("staging"): ``stage(source, tmp_dir)`` consumes bytes, a binary
file object or an iterable of bytes chunks, computes the SHA-256 of the
uncompressed content and produces a ``Staged`` blob:

- input of at most 4 MiB is held in memory and compressed with zstd level 19
  (lazily, see ``Staged.prepare``, so a re-put of content the bucket already
  has never pays for compression);
- larger input is streamed in 1 MiB chunks, so memory stays bounded whatever
  the object size. It goes through zstd level 9 into one temp file and, as
  raw bytes, into a second one, until the running sizes decide the question
  (after at least ``DECIDE_AFTER`` bytes): compression clearly pays, so the
  raw copy is dropped, or it saves less than 5% so far, so the zstd copy is
  dropped and the rest is stored raw. Disk use in ``tmp/`` therefore stays
  close to the stored size instead of the stored size plus the raw size;
- if compression saves less than 5% the stored form is the raw bytes;
- a stored size of at most 128 KiB is kept inline (it goes into the SQLite
  row); anything larger stays in an fsynced temp file that the writer
  installs under ``blobs/`` while holding the write lock.

Read side: ``BlobReader`` streams the uncompressed content of one blob row,
never producing more than the recorded size and checking size and SHA-256
at the end. Any mismatch raises DamagedError, so damaged data is never
returned silently by ``get()``. A streaming reader checks as it goes and
raises when it reaches the end: bytes handed out *before* the end are not
yet verified (``BlobReader.verified`` turns True once they are).

Internal for siblings:
    INLINE_MAX, SMALL_MAX, LEVEL_SMALL, LEVEL_LARGE, CODEC_RAW, CODEC_ZSTD, CHUNK
    DECIDE_AFTER
    BlobRow(id, hash, size, stored_size, codec, dict_id, data)   namedtuple
    BLOB_COLUMNS                     "id, hash, size, stored_size, codec, dict_id, data"
    stage(source, tmp_dir) -> Staged
    Staged: .hash (32 bytes), .hex, .size, .prepare(), .codec, .stored_size,
            .inline (bytes | None), .tmp (TempFile | None), .discard(),
            .stored_bytes_file() -> TempFile (the stored bytes in a temp file)
    BlobReader(bucket_dir, row)      io.RawIOBase; raises FileNotFoundError at
                                     construction when a file blob is missing;
                                     .verified
    check_blob(bucket_dir, row) -> str | None   problem description, None if intact
"""

from __future__ import annotations

import hashlib
import io
import os
from collections.abc import Iterable
from pathlib import Path
from typing import NamedTuple

from . import _zstd
from .blobs import TempFile, blob_path
from .errors import DamagedError, InvalidArgument, UnsupportedFormat

INLINE_MAX = 128 * 1024
SMALL_MAX = 4 * 1024 * 1024
LEVEL_SMALL = 19
LEVEL_LARGE = 9
MIN_SAVING = 0.05
#: Streamed input: keep both the raw and the zstd copy until at least this many bytes were seen.
DECIDE_AFTER = 16 * 1024 * 1024
#: Streamed input: drop the raw copy once zstd output is below this share of the input.
CLEARLY_WORTH = 0.85
CHUNK = 1024 * 1024
READ_CHUNK = 256 * 1024

CODEC_RAW = 0
CODEC_ZSTD = 1
CODEC_NAMES = {CODEC_RAW: "raw", CODEC_ZSTD: "zstd"}

BLOB_COLUMNS = "id, hash, size, stored_size, codec, dict_id, data"


class BlobRow(NamedTuple):
    id: int
    hash: bytes
    size: int
    stored_size: int
    codec: int
    dict_id: int | None
    data: bytes | None  # None: the stored bytes are a file under blobs/


def _worth_compressing(size: int, compressed: int) -> bool:
    return size > 0 and compressed <= size * (1.0 - MIN_SAVING)


class Staged:
    """A blob ready to be referenced by a write transaction."""

    def __init__(self, digest: bytes, size: int, raw: bytes | None, tmp_dir: Path) -> None:
        self.hash = digest
        self.hex = digest.hex()
        self.size = size
        self._raw = raw
        self._tmp_dir = tmp_dir
        self.codec: int | None = None
        self.stored_size: int | None = None
        self.inline: bytes | None = None
        self.tmp: TempFile | None = None
        #: Set by the bucket when the copy it already stores is damaged: the
        #: write then replaces the stored bytes with these (proven by their hash).
        self.heal = False
        self.repaired = False

    @property
    def prepared(self) -> bool:
        return self.codec is not None

    def prepare(self) -> None:
        """Compress (small input) so that codec/stored_size/inline-or-tmp are set."""
        if self.prepared:
            return
        raw = self._raw
        assert raw is not None
        packed = _zstd.compress(raw, LEVEL_SMALL) if raw else b""
        if _worth_compressing(len(raw), len(packed)):
            # Cheap insurance against compressor or memory faults.
            if _zstd.Decompressor().decompress(packed, len(raw) + 1) != raw:
                raise DamagedError(
                    "Compression produced bytes that do not decompress to the input.",
                    hint="Nothing was written. This points at a hardware or library fault; retry.",
                )
            self.codec, stored = CODEC_ZSTD, packed
        else:
            self.codec, stored = CODEC_RAW, raw
        self.stored_size = len(stored)
        if self.stored_size <= INLINE_MAX:
            self.inline = bytes(stored)
        else:
            self.tmp = _temp_with(self._tmp_dir, stored)
        self._raw = None

    def stored_bytes_file(self) -> TempFile:
        """The stored bytes in a committed temp file (moves ``tmp`` out when there is one)."""
        assert self.prepared
        if self.tmp is not None:
            tmp, self.tmp = self.tmp, None
            return tmp
        assert self.inline is not None
        return _temp_with(self._tmp_dir, self.inline)

    def discard(self) -> None:
        if self.tmp is not None:
            self.tmp.discard()
            self.tmp = None
        self._raw = None


def _temp_with(tmp_dir: Path, data: bytes) -> TempFile:
    tmp = TempFile(tmp_dir)
    try:
        tmp.write(data)
        tmp.commit()
    except BaseException:
        tmp.discard()
        raise
    return tmp


def _iter_source(source: object) -> Iterable[bytes]:
    if isinstance(source, (bytes, bytearray, memoryview)):
        view = memoryview(source).cast("B")
        for start in range(0, len(view), CHUNK):
            yield view[start : start + CHUNK]
        return
    if isinstance(source, str):
        raise InvalidArgument(
            "put() needs bytes, a binary file or an iterator of bytes, not a str.",
            hint="Encode text first, for example data.encode('utf-8').",
        )
    read = getattr(source, "read", None)
    if callable(read):
        while True:
            chunk = read(CHUNK)
            if not chunk:
                return
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise InvalidArgument(
                    "The file returned text instead of bytes.", hint="Open the file in binary mode ('rb')."
                )
            yield chunk
        return
    try:
        iterator = iter(source)  # type: ignore[call-overload]
    except TypeError:
        raise InvalidArgument(
            f"put() cannot read data from {type(source).__name__}.",
            hint="Pass bytes, a binary file object or an iterator of bytes chunks.",
        ) from None
    for chunk in iterator:
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise InvalidArgument(
                f"The data iterator produced {type(chunk).__name__} instead of bytes.",
                hint="Yield bytes chunks only.",
            )
        if chunk:
            yield chunk


def stage(source: object, tmp_dir: str | os.PathLike) -> Staged:
    """Read ``source`` fully and return a Staged blob. Bounded memory for any size."""
    tmp_dir = Path(tmp_dir)
    if isinstance(source, (bytes, bytearray, memoryview)) and memoryview(source).nbytes <= SMALL_MAX:
        raw = bytes(source)  # a copy: the caller may mutate a bytearray later
        return Staged(hashlib.sha256(raw).digest(), len(raw), raw, tmp_dir)

    chunks = iter(_iter_source(source))
    buffer = bytearray()
    for chunk in chunks:
        buffer += chunk
        if len(buffer) > SMALL_MAX:
            break
    else:
        raw = bytes(buffer)
        return Staged(hashlib.sha256(raw).digest(), len(raw), raw, tmp_dir)
    return _stage_stream(buffer, chunks, tmp_dir)


def _stage_stream(head: bytearray, rest: Iterable[bytes], tmp_dir: Path) -> Staged:
    hasher = hashlib.sha256()
    size = 0
    compressor = _zstd.Compressor(LEVEL_LARGE)
    packed: TempFile | None = None
    raw: TempFile | None = None
    try:
        packed = TempFile(tmp_dir)
        raw = TempFile(tmp_dir)

        def feed(chunk) -> None:
            nonlocal size, packed, raw
            hasher.update(chunk)
            size += len(chunk)
            if raw is not None:
                raw.write(chunk)
            if packed is None:
                return
            out = compressor.compress(chunk)
            if out:
                packed.write(out)
            if raw is not None and size >= DECIDE_AFTER:
                if packed.size > size * (1.0 - MIN_SAVING):
                    packed.discard()  # saves less than 5% so far: store raw
                    packed = None
                elif packed.size < size * CLEARLY_WORTH:
                    raw.discard()  # compression clearly pays: keep only the zstd copy
                    raw = None

        view = memoryview(head)
        for start in range(0, len(view), CHUNK):
            feed(view[start : start + CHUNK])
        view.release()
        head.clear()  # the caller does not use it again: give the memory back now
        for chunk in rest:
            feed(chunk)
        if packed is not None:
            tail = compressor.flush()
            if tail:
                packed.write(tail)
        digest = hasher.digest()
        if packed is not None and _worth_compressing(size, packed.size):
            codec, keep = CODEC_ZSTD, packed
            packed = None
        elif raw is not None:
            codec, keep = CODEC_RAW, raw
            raw = None
        else:
            # The raw copy was dropped early, but later input did not compress:
            # rebuild it from the zstd copy (rare), and prove it is the input.
            assert packed is not None
            keep = _decompress_to_raw(packed, tmp_dir, size, digest)
            codec = CODEC_RAW
        for other in (packed, raw):
            if other is not None:
                other.discard()
        packed = raw = None
        if codec == CODEC_RAW and keep.size != size:
            keep.discard()
            raise DamagedError(
                f"A temporary file holds {keep.size} bytes instead of the {size} bytes that were read.",
                hint="Nothing was written. Check the disk, then retry.",
            )
        staged = Staged(digest, size, None, tmp_dir)
        staged.codec = codec
        staged.stored_size = keep.size
        try:
            if keep.size <= INLINE_MAX:
                os.lseek(keep.fd, 0, os.SEEK_SET)  # type: ignore[arg-type]
                staged.inline = _read_all_fd(keep.fd, keep.size)  # type: ignore[arg-type]
                keep.discard()
            else:
                keep.commit()
                staged.tmp = keep
        except BaseException:
            keep.discard()
            raise
        return staged
    except BaseException:
        for tmp in (packed, raw):
            if tmp is not None:
                tmp.discard()
        raise


def _read_all_fd(fd: int, size: int) -> bytes:
    parts = []
    remaining = size
    while remaining > 0:
        part = os.read(fd, min(remaining, CHUNK))
        if not part:
            break
        parts.append(part)
        remaining -= len(part)
    return b"".join(parts)


def _decompress_to_raw(packed: TempFile, tmp_dir: Path, size: int, digest: bytes) -> TempFile:
    """Replace a poorly-compressed temp file by its raw content (streaming), checked against the input."""
    raw = TempFile(tmp_dir)
    try:
        hasher = hashlib.sha256()
        with open(packed.path, "rb") as fp:
            reader = _zstd.FrameReader(fp, READ_CHUNK, CHUNK)
            while True:
                out = reader.read()
                if not out:
                    break
                hasher.update(out)
                raw.write(out)
                if raw.size > size:
                    break
        if raw.size != size or hasher.digest() != digest or reader.truncated or reader.trailing:
            raise DamagedError(
                "A temporary file did not decompress back to the data that was read.",
                hint="Nothing was written. This points at a disk, memory or library fault; retry.",
            )
    except _zstd.ZstdError as exc:
        raw.discard()
        raise DamagedError(
            f"A temporary file could not be decompressed ({exc}).",
            hint="Nothing was written. This points at a disk, memory or library fault; retry.",
        ) from None
    except BaseException:
        raw.discard()
        raise
    finally:
        packed.discard()
    return raw


class BlobReader(io.RawIOBase):
    """Readable stream of one blob's uncompressed bytes, verified at the end."""

    def __init__(self, bucket_dir: str | os.PathLike, row: BlobRow, label: str = "") -> None:
        super().__init__()
        self._fp = None
        self._row = row
        self._label = label or row.hash.hex()
        self.verified = False
        if row.dict_id is not None:
            raise UnsupportedFormat(
                f"Object {self._label} was compressed with dictionary {row.dict_id}, "
                "which this version of mybucket cannot read."
            )
        if row.codec not in CODEC_NAMES:
            raise UnsupportedFormat(f"Object {self._label} uses unknown codec {row.codec}.")
        if row.data is not None:
            self._fp = io.BytesIO(row.data)
        else:
            self._fp = open(blob_path(bucket_dir, row.hash.hex()), "rb")  # noqa: SIM115
        self._z = _zstd.FrameReader(self._fp, READ_CHUNK, CHUNK) if row.codec == CODEC_ZSTD else None
        self._pending = memoryview(b"")
        self._hasher = hashlib.sha256()
        self._count = 0
        self._done = False

    def readable(self) -> bool:
        return True

    def _damaged(self, why: str) -> DamagedError:
        self._done = True
        where = "inline in the database" if self._row.data is not None else "in its blob file"
        return DamagedError(f"Object {self._label} is damaged ({where}): {why}.")

    def _next(self, n: int) -> bytes:
        if self._z is None:
            return self._fp.read(min(n, self._row.size - self._count + 1))
        if not self._pending:
            try:
                chunk = self._z.read()
            except _zstd.ZstdError as exc:
                raise self._damaged(f"the compressed data is invalid ({exc})") from None
            if not chunk:
                if self._z.trailing:
                    raise self._damaged("unexpected bytes after the end of the compressed data")
                if self._z.truncated:
                    raise self._damaged("the compressed data is truncated")
                return b""
            self._pending = memoryview(chunk)
        out = bytes(self._pending[:n])
        self._pending = self._pending[n:]
        return out

    def readinto(self, buffer) -> int:  # type: ignore[override]
        if self._done or len(buffer) == 0:
            return 0
        try:
            chunk = self._next(len(buffer))
        except OSError as exc:
            raise self._damaged(f"it cannot be read from disk ({exc.strerror or exc})") from None
        if not chunk:
            self._finish()
            return 0
        self._count += len(chunk)
        if self._count > self._row.size:
            raise self._damaged(f"it holds more than the recorded {self._row.size} bytes")
        self._hasher.update(chunk)
        n = len(chunk)
        buffer[:n] = chunk
        return n

    def _finish(self) -> None:
        self._done = True
        if self._count != self._row.size:
            raise self._damaged(f"expected {self._row.size} bytes, found {self._count}")
        if self._hasher.digest() != self._row.hash:
            raise self._damaged("its SHA-256 does not match the recorded ETag")
        self.verified = True

    def readall(self) -> bytes:
        parts = []
        while True:
            chunk = self.read(CHUNK)
            if not chunk:
                break
            parts.append(chunk)
        return b"".join(parts)

    def close(self) -> None:
        if not self.closed:
            try:
                if self._fp is not None:
                    self._fp.close()
            finally:
                super().close()


def check_blob(bucket_dir: str | os.PathLike, row: BlobRow) -> str | None:
    """Re-read and re-hash one blob. Returns a plain description of the problem, or None."""
    if row.data is not None and len(row.data) != row.stored_size:
        return f"stored size is {len(row.data)} bytes, the row records {row.stored_size}"
    if row.data is None:
        path = blob_path(bucket_dir, row.hash.hex())
        try:
            actual = path.stat().st_size
        except FileNotFoundError:
            return "its blob file is missing"
        if actual != row.stored_size:
            return f"its blob file is {actual} bytes, the row records {row.stored_size}"
    try:
        with BlobReader(bucket_dir, row) as reader:
            while reader.read(CHUNK):
                pass
    except FileNotFoundError:
        return "its blob file is missing"
    except (DamagedError, UnsupportedFormat) as exc:
        return exc.message
    except OSError as exc:
        return f"its blob file cannot be read ({exc})"
    return None
