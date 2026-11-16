"""Tiny shim over the zstd implementation in use.

Order of preference:

1. ``compression.zstd`` from the standard library (Python 3.14+);
2. ``backports.zstd`` (same API, declared as a dependency for Python < 3.14);
3. ``zstandard`` (different API, adapted here) as a last resort.

The rest of mybucket only uses the names below, so swapping implementations
never touches the engine. tests/test_zstd_shim.py runs the codec against the
``zstandard`` fallback too.

Internal for siblings:
    BACKEND: str                       which implementation was loaded
    ZstdError: exception class         raised on malformed input
    Dictionary(content)                a trained zstd dictionary (the bytes ``zstd -D``
                                       reads): .content, .zstd_id (the ID written in
                                       every frame compressed with it)
    train(samples, size) -> bytes      train a dictionary of at most ``size`` bytes
    compress(data, level, dictionary=None) -> bytes
                                       one frame, content size in the header
    Compressor(level)                  .compress(chunk) -> bytes, .flush() -> bytes
    Decompressor(dictionary=None)      .decompress(data, max_length=-1) -> bytes,
                                       .eof, .needs_input, .unused_data
                                       (bz2/lzma-style incremental decompressor)
    FrameReader(fp, read_size, write_size, dictionary=None)
                                       pull-style reader of one zstd frame from a
                                       binary file object: .read() returns at most
                                       ``write_size`` bytes, b"" at the end; after
                                       that, .truncated / .trailing say whether the
                                       input ended early or had bytes after the
                                       frame (always False when the backend cannot
                                       tell; callers check size and SHA-256 anyway).
                                       Memory stays bounded by ``write_size``
                                       whatever the compression ratio.
"""

from __future__ import annotations

try:  # Python 3.14+
    from compression import zstd as _impl  # type: ignore[import-not-found]

    BACKEND = "compression.zstd"
except ImportError:  # pragma: no cover - depends on the interpreter
    try:
        from backports import zstd as _impl  # type: ignore[no-redef]

        BACKEND = "backports.zstd"
    except ImportError:
        _impl = None  # type: ignore[assignment]
        BACKEND = "zstandard"

if _impl is not None:
    ZstdError = _impl.ZstdError

    class Dictionary:
        def __init__(self, content: bytes) -> None:
            self.content = bytes(content)
            self._d = _impl.ZstdDict(self.content)
            self.zstd_id = int(self._d.dict_id)

    def train(samples: list[bytes], size: int) -> bytes:
        return bytes(_impl.train_dict(samples, size).dict_content)

    def compress(data: bytes, level: int, dictionary: Dictionary | None = None) -> bytes:
        if dictionary is None:
            return _impl.compress(data, level=level)
        return _impl.compress(data, level=level, zstd_dict=dictionary._d.as_digested_dict)

    class Compressor:
        def __init__(self, level: int) -> None:
            self._c = _impl.ZstdCompressor(level=level)

        def compress(self, chunk: bytes) -> bytes:
            return self._c.compress(chunk)

        def flush(self) -> bytes:
            return self._c.flush()

    def Decompressor(dictionary: Dictionary | None = None):  # noqa: N802 - mirrors a class constructor
        if dictionary is None:
            return _impl.ZstdDecompressor()
        return _impl.ZstdDecompressor(zstd_dict=dictionary._d)

    class FrameReader:
        def __init__(self, fp, read_size: int, write_size: int, dictionary: Dictionary | None = None) -> None:
            self._fp = fp
            self._d = Decompressor(dictionary)
            self._read_size = read_size
            self._write_size = write_size
            self._ended = False
            self.truncated = False
            self.trailing = False

        def read(self) -> bytes:
            d = self._d
            while not self._ended:
                if d.eof:
                    self._ended = True
                    self.trailing = bool(d.unused_data) or bool(self._fp.read(1))
                    break
                if d.needs_input:
                    data = self._fp.read(self._read_size)
                    if not data:
                        self._ended = self.truncated = True
                        break
                else:
                    data = b""
                out = d.decompress(data, self._write_size)
                if out:
                    return out
            return b""

else:  # only used when neither the stdlib module nor the backport exists
    try:
        import zstandard as _zs
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "mybucket needs zstd: use Python 3.14+ (compression.zstd) or install "
            "'backports.zstd' (or 'zstandard')."
        ) from exc

    ZstdError = _zs.ZstdError

    class Dictionary:  # type: ignore[no-redef]
        def __init__(self, content: bytes) -> None:
            self.content = bytes(content)
            self._d = _zs.ZstdCompressionDict(self.content)
            self.zstd_id = int(self._d.dict_id())

    def train(samples: list[bytes], size: int) -> bytes:
        return bytes(_zs.train_dictionary(size, samples).as_bytes())

    def compress(data: bytes, level: int, dictionary: Dictionary | None = None) -> bytes:
        if dictionary is None:
            return _zs.ZstdCompressor(level=level, write_content_size=True).compress(data)
        return _zs.ZstdCompressor(level=level, dict_data=dictionary._d, write_content_size=True).compress(data)

    class Compressor:  # type: ignore[no-redef]
        def __init__(self, level: int) -> None:
            self._c = _zs.ZstdCompressor(level=level).compressobj()

        def compress(self, chunk: bytes) -> bytes:
            return self._c.compress(chunk)

        def flush(self) -> bytes:
            return self._c.flush()

    class Decompressor:  # type: ignore[no-redef]
        """bz2-style incremental decompressor built on zstandard's decompressobj.

        zstandard cannot bound the output of one call, so whatever one input
        chunk expands to is buffered here and handed out ``max_length`` bytes
        at a time. The engine only uses this class on input of known small
        size; streaming reads go through FrameReader, which is bounded.
        """

        def __init__(self, dictionary: Dictionary | None = None) -> None:
            if dictionary is None:
                self._d = _zs.ZstdDecompressor().decompressobj()
            else:
                self._d = _zs.ZstdDecompressor(dict_data=dictionary._d).decompressobj()
            self._pending = b""
            self._frame_done = False
            self.unused_data = b""

        @property
        def eof(self) -> bool:
            # The frame is over only once everything it produced was handed out.
            return self._frame_done and not self._pending

        @property
        def needs_input(self) -> bool:
            return not self._pending and not self._frame_done

        def decompress(self, data: bytes, max_length: int = -1) -> bytes:
            if data and not self._frame_done:
                self._pending += self._d.decompress(data)
                if self._d.eof:
                    self._frame_done = True
                    self.unused_data = self._d.unused_data
            elif data:
                self.unused_data += data
            if max_length < 0:
                out, self._pending = self._pending, b""
            else:
                out, self._pending = self._pending[:max_length], self._pending[max_length:]
            return out

    class FrameReader:  # type: ignore[no-redef]
        def __init__(self, fp, read_size: int, write_size: int, dictionary: Dictionary | None = None) -> None:
            # read_to_iter never yields more than write_size bytes at a time.
            d = _zs.ZstdDecompressor() if dictionary is None else _zs.ZstdDecompressor(dict_data=dictionary._d)
            self._it = d.read_to_iter(fp, read_size=read_size, write_size=write_size)
            self.truncated = False  # zstandard does not report it; the size check catches it
            self.trailing = False

        def read(self) -> bytes:
            return next(self._it, b"")
