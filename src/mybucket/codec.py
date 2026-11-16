"""Turning caller input into stored blobs, and stored blobs back into bytes.

Write side ("staging"): ``stage(source, tmp_dir)`` consumes bytes, a binary
file object or an iterable of bytes chunks, computes the SHA-256 of the
uncompressed content and produces a ``Staged`` blob:

- input of at most 4 MiB is held in memory and compressed with zstd level 19
  into one frame (lazily, see ``Staged.prepare``, so a re-put of content the
  bucket already has never pays for compression). When the bucket has an
  active trained dictionary and the input is at most ``DICT_MAX`` bytes, the
  frame is compressed with that dictionary;
- larger input is streamed, so memory stays bounded whatever the object
  size. It is cut into frames of ``FRAME_SIZE`` (1 MiB) content bytes; each
  frame is hashed (SHA-256) and compressed on its own with zstd level 9, and
  the frames go into one temp file and, as raw bytes, into a second one,
  until the running sizes decide the question (after at least
  ``DECIDE_AFTER`` bytes): compression clearly pays, so the raw copy is
  dropped, or it saves less than 5% so far, so the zstd copy is dropped and
  the rest is stored raw. The zstd form ends with a standard zstd seek table
  (a skippable frame), so ``zstd -d`` reads it as it is (codec 2). Either way
  the frame list (``Staged.frames``) is stored in the ``frames`` table and
  makes verified range reads possible;
- if compression saves less than 5% the stored form is the raw bytes;
- a stored size of at most 128 KiB is kept inline (it goes into the SQLite
  row); anything larger stays in an fsynced temp file that the writer
  installs under ``blobs/`` while holding the write lock.

Read side: ``BlobReader`` streams the uncompressed content (or a range of it)
of one blob row, never producing more than the recorded size. A blob file is
opened without following links and must be a regular file of exactly the
recorded stored size before anything is read. Every byte handed out has
been checked:

- a blob with a frame table is checked frame by frame: each frame is
  verified against its SHA-256 before any of its bytes are handed out;
- a blob without one (small objects, and large objects written by mybucket
  0.1) has a single SHA-256 for the whole content, so it is checked in full
  *before* the first byte is handed out: content of up to ``SMALL_MAX``
  (4 MiB) is decompressed once into memory, checked and then served from
  there; larger content (only mybucket 0.1 wrote such blobs) is read twice,
  once to check it and once to stream it (``verify_first=False``, used by
  ``get()``, which returns nothing before the end anyway, reads it once).

Any mismatch raises DamagedError.

Internal for siblings:
    INLINE_MAX, SMALL_MAX, LEVEL_SMALL, LEVEL_LARGE, CHUNK, FRAME_SIZE, DICT_MAX
    CODEC_RAW, CODEC_ZSTD, CODEC_FRAMES, CODEC_NAMES, DECIDE_AFTER
    BlobRow(id, hash, size, stored_size, codec, dict_id, data)   namedtuple
    BLOB_COLUMNS                     "id, hash, size, stored_size, codec, dict_id, data"
    Frame(start, size, stored_start, stored_size, hash)          namedtuple
    FRAME_COLUMNS                    the same names, for SELECT
    LoadedDict(id, hash, dictionary) a dicts row ready for use
    seek_table(frames) -> bytes      the zstd seekable-format skippable frame
    stage(source, tmp_dir) -> Staged
    Staged: .hash (32 bytes), .hex, .size, .prepare(dictionary=None), .codec,
            .stored_size, .dict_id, .frames, .inline (bytes | None),
            .tmp (TempFile | None), .discard(), .drop_dictionary(),
            .stored_bytes_file() -> TempFile (the stored bytes in a temp file)
    BlobReader(bucket_dir, row, label="", frames=None, dictionary=None, start=0, end=None,
               verify_first=True)
                                     io.RawIOBase; raises FileNotFoundError at
                                     construction when a file blob is missing,
                                     DamagedError when it is a link, not a regular
                                     file or not the recorded size; .verified
    check_blob(bucket_dir, row, frames=None, dictionary=None) -> str | None
                                     problem description, None if intact
"""

from __future__ import annotations

import hashlib
import io
import os
from collections.abc import Iterable
from pathlib import Path
from typing import NamedTuple

import struct

from . import _zstd
from .blobs import TempFile, UnsafePath, open_blob
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

#: Content bytes per frame of a streamed (large) blob.
FRAME_SIZE = 1024 * 1024
#: A frame table entry larger than this is treated as damage (the writer never makes one).
FRAME_MAX = 64 * 1024 * 1024
#: Objects up to this size are compressed with the bucket's active dictionary, if any.
DICT_MAX = 1024 * 1024

CODEC_RAW = 0
CODEC_ZSTD = 1
CODEC_FRAMES = 2
CODEC_NAMES = {CODEC_RAW: "raw", CODEC_ZSTD: "zstd", CODEC_FRAMES: "zstd frames"}

#: CAST: a database from elsewhere may hold TEXT there; it must read as damage, not fail to decode.
BLOB_COLUMNS = "id, CAST(hash AS BLOB), size, stored_size, codec, dict_id, CAST(data AS BLOB)"
FRAME_COLUMNS = "start, size, stored_start, stored_size, hash"

#: zstd seekable format (contrib/seekable_format in the zstd sources).
SKIPPABLE_MAGIC = 0x184D2A5E
SEEKABLE_MAGIC = 0x8F92EAB1


class BlobRow(NamedTuple):
    id: int
    hash: bytes
    size: int
    stored_size: int
    codec: int
    dict_id: int | None
    data: bytes | None  # None: the stored bytes are a file under blobs/


class Frame(NamedTuple):
    start: int  # offset of the frame's first content byte
    size: int  # content bytes in the frame
    stored_start: int  # offset of the frame in the stored bytes
    stored_size: int
    hash: bytes  # SHA-256 of the frame's content bytes


class LoadedDict(NamedTuple):
    id: int  # dicts.id, the value of blobs.dict_id
    hash: bytes  # SHA-256 of the dictionary bytes
    dictionary: _zstd.Dictionary


def seek_table(frames: list[Frame]) -> bytes:
    """The skippable frame that ends a codec 2 blob (zstd seekable format, no checksums)."""
    entries = b"".join(struct.pack("<II", f.stored_size, f.size) for f in frames)
    footer = struct.pack("<IBI", len(frames), 0, SEEKABLE_MAGIC)
    body = entries + footer
    return struct.pack("<II", SKIPPABLE_MAGIC, len(body)) + body


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
        #: The dicts.id the stored bytes were compressed with (None: no dictionary).
        self.dict_id: int | None = None
        self._dictionary: LoadedDict | None = None
        #: Frame table of a streamed blob (None for small blobs).
        self.frames: list[Frame] | None = None
        #: Set by the bucket when the copy it already stores is damaged: the
        #: write then replaces the stored bytes with these (proven by their hash).
        self.heal = False
        self.repaired = False

    @property
    def prepared(self) -> bool:
        return self.codec is not None

    def prepare(self, dictionary: LoadedDict | None = None) -> None:
        """Compress (small input) so that codec/stored_size/inline-or-tmp are set.

        ``dictionary`` is the bucket's active dictionary; it is used for input
        of at most DICT_MAX bytes. Streamed input was prepared while it was read.
        """
        if self.prepared:
            return
        raw = self._raw
        assert raw is not None
        use = dictionary if dictionary is not None and 0 < len(raw) <= DICT_MAX else None
        zd = use.dictionary if use is not None else None
        packed = _zstd.compress(raw, LEVEL_SMALL, zd) if raw else b""
        if _worth_compressing(len(raw), len(packed)):
            # Cheap insurance against compressor or memory faults.
            if _zstd.Decompressor(zd).decompress(packed, len(raw) + 1) != raw:
                raise DamagedError(
                    "Compression produced bytes that do not decompress to the input.",
                    hint="Nothing was written. This points at a hardware or library fault; retry.",
                )
            self.codec, stored = CODEC_ZSTD, packed
            self.dict_id, self._dictionary = (use.id, use) if use is not None else (None, None)
        else:
            self.codec, stored = CODEC_RAW, raw
        self.stored_size = len(stored)
        if self.stored_size <= INLINE_MAX:
            self.inline = bytes(stored)
        else:
            self.tmp = _temp_with(self._tmp_dir, stored)
        self._raw = None

    def drop_dictionary(self) -> None:
        """Re-prepare without a dictionary (the one used was removed meanwhile).

        The content is recovered from the stored bytes with the dictionary
        still held in memory, and checked against the hash before it is used.
        """
        if self.dict_id is None:
            return
        assert self._dictionary is not None
        if self.inline is not None:
            stored = self.inline
        else:
            assert self.tmp is not None
            os.lseek(self.tmp.fd, 0, os.SEEK_SET)  # type: ignore[arg-type]
            stored = _read_all_fd(self.tmp.fd, self.tmp.size)  # type: ignore[arg-type]
        raw = _zstd.Decompressor(self._dictionary.dictionary).decompress(stored, self.size + 1)
        if len(raw) != self.size or hashlib.sha256(raw).digest() != self.hash:
            raise DamagedError(
                "Recompressing the data without its dictionary gave different bytes.",
                hint="Nothing was written. This points at a hardware or library fault; retry.",
            )
        if self.tmp is not None:
            self.tmp.discard()
            self.tmp = None
        self.codec = self.stored_size = self.inline = None
        self.dict_id = self._dictionary = None
        self._raw = raw
        self.prepare(None)

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
    packed: TempFile | None = None
    raw: TempFile | None = None
    # One entry per frame: (content size, compressed size or 0 once the zstd copy is gone, hash).
    entries: list[tuple[int, int, bytes]] = []
    pending = bytearray()
    try:
        packed = TempFile(tmp_dir)
        raw = TempFile(tmp_dir)

        def frame(data) -> None:
            nonlocal packed, raw
            digest = hashlib.sha256(data).digest()
            stored = 0
            if packed is not None:
                out = _zstd.compress(bytes(data), LEVEL_LARGE)
                packed.write(out)
                stored = len(out)
            entries.append((len(data), stored, digest))
            if raw is not None and packed is not None and size >= DECIDE_AFTER:
                if packed.size > size * (1.0 - MIN_SAVING):
                    packed.discard()  # saves less than 5% so far: store raw
                    packed = None
                elif packed.size < size * CLEARLY_WORTH:
                    raw.discard()  # compression clearly pays: keep only the zstd copy
                    raw = None

        def feed(chunk) -> None:
            nonlocal size
            hasher.update(chunk)
            size += len(chunk)
            if raw is not None:
                raw.write(chunk)
            pending.extend(chunk)
            while len(pending) >= FRAME_SIZE:
                frame(memoryview(pending)[:FRAME_SIZE])
                del pending[:FRAME_SIZE]

        view = memoryview(head)
        for start in range(0, len(view), CHUNK):
            feed(view[start : start + CHUNK])
        view.release()
        head.clear()  # the caller does not use it again: give the memory back now
        for chunk in rest:
            feed(chunk)
        if pending:
            frame(memoryview(pending))
            pending.clear()
        digest = hasher.digest()
        if packed is not None and _worth_compressing(size, packed.size):
            frames = _frame_table(entries, compressed=True)
            packed.write(seek_table(frames))
            codec, keep = CODEC_FRAMES, packed
            packed = None
        elif raw is not None:
            codec, keep = CODEC_RAW, raw
            raw = None
            frames = _frame_table(entries, compressed=False)
        else:
            # The raw copy was dropped early, but later input did not compress:
            # rebuild it from the zstd frames (rare), and prove it is the input.
            assert packed is not None
            keep = _decompress_to_raw(packed, _frame_table(entries, compressed=True), tmp_dir, size, digest)
            packed = None
            codec = CODEC_RAW
            frames = _frame_table(entries, compressed=False)
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
        staged.frames = frames
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


def _frame_table(entries: list[tuple[int, int, bytes]], compressed: bool) -> list[Frame]:
    frames: list[Frame] = []
    start = stored = 0
    for size, packed_size, digest in entries:
        stored_size = packed_size if compressed else size
        frames.append(Frame(start, size, stored if compressed else start, stored_size, digest))
        start += size
        stored += stored_size
    return frames


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


def _decompress_to_raw(packed: TempFile, frames: list[Frame], tmp_dir: Path, size: int, digest: bytes) -> TempFile:
    """Replace poorly-compressed zstd frames by their raw content (frame by frame), checked against the input."""
    raw = TempFile(tmp_dir)
    try:
        hasher = hashlib.sha256()
        for f in frames:
            data = os.pread(packed.fd, f.stored_size, f.stored_start)  # type: ignore[arg-type]
            out = _decompress_frame(data, f.size, None)
            if out is None or hashlib.sha256(out).digest() != f.hash:
                raise DamagedError(
                    "A temporary file did not decompress back to the data that was read.",
                    hint="Nothing was written. This points at a disk, memory or library fault; retry.",
                )
            hasher.update(out)
            raw.write(out)
        if raw.size != size or hasher.digest() != digest:
            raise DamagedError(
                "A temporary file did not decompress back to the data that was read.",
                hint="Nothing was written. This points at a disk, memory or library fault; retry.",
            )
    except BaseException:
        raw.discard()
        raise
    finally:
        packed.discard()
    return raw


def _decompress_frame(data: bytes, size: int, dictionary: _zstd.Dictionary | None) -> bytes | None:
    """Exactly one zstd frame of ``size`` content bytes, or None when ``data`` is not that."""
    try:
        d = _zstd.Decompressor(dictionary)
        out = d.decompress(data, size + 1)
    except _zstd.ZstdError:
        return None
    if len(out) != size or not d.eof or d.unused_data:
        return None
    return out


def _checked_row(row: BlobRow) -> BlobRow:
    """``row`` with the types a reader relies on, or DamagedError (a database from elsewhere may hold anything)."""
    ints = (row.id, row.size, row.stored_size, row.codec)
    ok = (
        all(isinstance(v, int) and not isinstance(v, bool) for v in ints)
        and row.size >= 0
        and row.stored_size >= 0
        and (row.dict_id is None or (isinstance(row.dict_id, int) and not isinstance(row.dict_id, bool)))
        and isinstance(row.hash, bytes)
        and len(row.hash) == 32
        and (row.data is None or isinstance(row.data, bytes))
    )
    if not ok:
        raise DamagedError(f"Stored content {row.id!r} is damaged: its database row holds values of the wrong type.")
    return row


class BlobReader(io.RawIOBase):
    """Readable stream of one blob's uncompressed bytes (or of ``[start, end)``), always checked.

    ``frames`` is the blob's frame table (only the frames that overlap the
    range are needed) and ``dictionary`` the dictionary of ``row.dict_id``;
    the caller loads both from the same database snapshot as ``row``.
    """

    def __init__(
        self,
        bucket_dir: str | os.PathLike,
        row: BlobRow,
        label: str = "",
        frames: list[Frame] | None = None,
        dictionary: LoadedDict | None = None,
        start: int = 0,
        end: int | None = None,
        verify_first: bool = True,
    ) -> None:
        super().__init__()
        self._fp = None
        self._row = row = _checked_row(row)
        self._label = label or row.hash.hex()
        self.verified = False
        self._start = max(0, min(start, row.size))
        self._end = row.size if end is None else max(self._start, min(end, row.size))
        if row.codec not in CODEC_NAMES:
            raise UnsupportedFormat(f"Object {self._label} uses unknown codec {row.codec}.")
        self._zdict = None
        if row.dict_id is not None:
            if dictionary is None or dictionary.id != row.dict_id:
                raise self._damaged(f"the zstd dictionary it was compressed with ({row.dict_id}) is missing")
            if row.codec == CODEC_RAW:
                raise self._damaged("it is stored raw but names a dictionary")
            self._zdict = dictionary.dictionary
        self._frames: list[Frame] | None = None
        if row.codec == CODEC_FRAMES or frames:
            self._frames = self._check_frames(frames or [])
        if row.data is not None:
            if len(row.data) != row.stored_size:
                raise self._damaged(f"its stored size is {len(row.data)} bytes, the row records {row.stored_size}")
            self._fp = io.BytesIO(row.data)
        else:
            try:
                fd = open_blob(bucket_dir, row.hash.hex())
            except UnsafePath:
                raise self._damaged("its blob file is a symbolic link or not a regular file, which mybucket never reads") from None
            try:
                actual = os.fstat(fd).st_size
                if actual != row.stored_size:
                    raise self._damaged(f"its blob file is {actual} bytes, the row records {row.stored_size}")
            except BaseException:
                os.close(fd)
                raise
            self._fp = os.fdopen(fd, "rb")
        self._pending = memoryview(b"")
        self._hasher = hashlib.sha256()
        self._count = 0  # content bytes read from the blob so far (unframed only)
        self._done = False
        if self._start >= self._end and not self.whole:
            self._done = self.verified = True  # an empty range: no bytes to hand out or check
        if self._frames is not None:
            self._index = 0
            self._z = None
        else:
            self._z = _zstd.FrameReader(self._fp, READ_CHUNK, CHUNK, self._zdict) if row.codec == CODEC_ZSTD else None
            self._skip = self._start  # unframed: bytes before the range are read and hashed, not handed out
        #: Unframed content is checked in full before the first byte is handed out.
        self._verify_first = verify_first and self._frames is None and not self._done
        self._prechecked = False

    @property
    def whole(self) -> bool:
        return self._start == 0 and self._end == self._row.size

    def readable(self) -> bool:
        return True

    def _damaged(self, why: str) -> DamagedError:
        self._done = True
        where = "inline in the database" if self._row.data is not None else "in its blob file"
        return DamagedError(f"Object {self._label} is damaged ({where}): {why}.")

    def _check_frames(self, frames: list[Frame]) -> list[Frame]:
        """Frames covering [start, end), contiguous and within the stored bytes, or DamagedError."""
        row = self._row
        if self._start >= self._end:
            return []
        if not frames:
            raise self._damaged("its frame table is missing")
        prev = None
        for f in frames:
            bad = (
                f.size <= 0
                or f.size > FRAME_MAX
                or f.stored_size <= 0
                or f.stored_size > FRAME_MAX + 1024
                or f.start < 0
                or f.start + f.size > row.size
                or f.stored_start < 0
                or f.stored_start + f.stored_size > row.stored_size
                or len(f.hash) != 32
                or (row.codec == CODEC_RAW and (f.stored_start != f.start or f.stored_size != f.size))
                or (prev is not None and (f.start != prev.start + prev.size or f.stored_start != prev.stored_start + prev.stored_size))
            )
            if bad:
                raise self._damaged("its frame table is inconsistent")
            prev = f
        if frames[0].start > self._start or frames[-1].start + frames[-1].size < self._end:
            raise self._damaged("its frame table does not cover the content")
        if self.whole and (frames[0].start != 0 or frames[0].stored_start != 0):
            raise self._damaged("its frame table does not start at the beginning")
        return frames

    # -- framed blobs: every frame is checked before it is handed out ---------

    def _next_frame(self) -> bytes:
        frames = self._frames
        assert frames is not None
        while self._index < len(frames):
            f = frames[self._index]
            self._index += 1
            if f.start + f.size <= self._start:
                continue
            if f.start >= self._end:
                break
            self._fp.seek(f.stored_start)
            data = self._fp.read(f.stored_size)
            if len(data) != f.stored_size:
                raise self._damaged("its stored bytes are shorter than recorded")
            if self._row.codec == CODEC_FRAMES:
                content = _decompress_frame(data, f.size, self._zdict)
                if content is None:
                    raise self._damaged(f"the compressed frame at content offset {f.start} is invalid")
            else:
                content = data
            if hashlib.sha256(content).digest() != f.hash:
                raise self._damaged(f"the frame at content offset {f.start} does not match its SHA-256")
            if self.whole:
                self._hasher.update(content)
            lo = max(self._start, f.start) - f.start
            hi = min(self._end, f.start + f.size) - f.start
            return content[lo:hi]
        return b""

    # -- unframed blobs: checked when the end of the blob is reached ----------

    def _next_unframed(self, n: int) -> bytes:
        if self._z is None:
            return self._fp.read(min(max(n, CHUNK), self._row.size - self._count + 1))
        try:
            chunk = self._z.read()
        except _zstd.ZstdError as exc:
            raise self._damaged(f"the compressed data is invalid ({exc})") from None
        if not chunk:
            if self._z.trailing:
                raise self._damaged("unexpected bytes after the end of the compressed data")
            if self._z.truncated:
                raise self._damaged("the compressed data is truncated")
        return chunk

    def _pull(self, n: int) -> bytes:
        """The next piece of the range, or b"" at its end (after the final checks)."""
        if self._frames is not None:
            chunk = self._next_frame()
            if not chunk:
                self._finish()
            return chunk
        if self._verify_first and not self._prechecked:
            data = self._precheck()
            if data is not None:
                self._done = True
                return data
        while True:
            if self._count >= self._end:
                if self._prechecked:
                    self._done = True  # already checked in full: no need to read on
                    return b""
                # The range is complete: read on to the end of the blob so it can be checked.
                while not self._done:
                    rest = self._next_unframed(CHUNK)
                    if not rest:
                        self._finish()
                        break
                    self._account(rest)
                return b""
            chunk = self._next_unframed(n)
            if not chunk:
                self._finish()
                return b""
            before = self._count
            self._account(chunk)
            lo = max(self._start, before) - before
            hi = min(self._end, self._count) - before
            if hi > lo:
                return chunk[lo:hi]

    def _precheck(self) -> bytes | None:
        """Read and check the whole unframed blob before anything is handed out.

        Small content is kept and the range of it returned; larger content is
        only checked, then the blob is read again from the start (returns None).
        """
        keep: list[bytes] | None = [] if self._row.size <= SMALL_MAX else None
        while not self._done:
            chunk = self._next_unframed(CHUNK)
            if not chunk:
                self._finish()  # raises DamagedError on any mismatch
                break
            self._account(chunk)
            if keep is not None:
                keep.append(chunk)
        self._prechecked = True
        if keep is not None:
            return b"".join(keep)[self._start : self._end]
        # Second pass over the same open file (blob files are never changed in place).
        self._fp.seek(0)
        self._z = _zstd.FrameReader(self._fp, READ_CHUNK, CHUNK, self._zdict) if self._row.codec == CODEC_ZSTD else None
        self._count = 0
        self._hasher = hashlib.sha256()
        self._done = False
        return None

    def _account(self, chunk: bytes) -> None:
        self._count += len(chunk)
        if self._count > self._row.size:
            raise self._damaged(f"it holds more than the recorded {self._row.size} bytes")
        self._hasher.update(chunk)

    def readinto(self, buffer) -> int:  # type: ignore[override]
        if len(buffer) == 0:
            return 0
        if not self._pending:
            if self._done:
                return 0
            try:
                chunk = self._pull(len(buffer))
            except OSError as exc:
                raise self._damaged(f"it cannot be read from disk ({exc.strerror or exc})") from None
            if not chunk:
                return 0
            self._pending = memoryview(chunk)
        n = min(len(buffer), len(self._pending))
        buffer[:n] = self._pending[:n]
        self._pending = self._pending[n:]
        return n

    def _finish(self) -> None:
        self._done = True
        if self._frames is not None:
            if self.whole and self._row.size and self._hasher.digest() != self._row.hash:
                raise self._damaged("its SHA-256 does not match the recorded ETag")
            self.verified = True
            return
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


def check_blob(
    bucket_dir: str | os.PathLike,
    row: BlobRow,
    frames: list[Frame] | None = None,
    dictionary: LoadedDict | None = None,
) -> str | None:
    """Re-read and re-hash one blob. Returns a plain description of the problem, or None."""
    try:
        row = _checked_row(row)
    except DamagedError as exc:
        return exc.message
    if row.data is not None and len(row.data) != row.stored_size:
        return f"stored size is {len(row.data)} bytes, the row records {row.stored_size}"
    try:
        with BlobReader(bucket_dir, row, frames=frames, dictionary=dictionary, verify_first=False) as reader:
            while reader.read(CHUNK):
                pass
            if row.codec == CODEC_FRAMES and reader._frames:
                last = reader._frames[-1]
                reader._fp.seek(last.stored_start + last.stored_size)
                if reader._fp.read() != seek_table(reader._frames):
                    return "its zstd seek table (at the end of the stored bytes) is damaged"
    except FileNotFoundError:
        return "its blob file is missing"
    except (DamagedError, UnsupportedFormat) as exc:
        return exc.message
    except OSError as exc:
        return f"its blob file cannot be read ({exc})"
    return None
