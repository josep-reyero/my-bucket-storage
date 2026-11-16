"""How tests/data/v1-store was made: a format 1 store written by mybucket 0.1 itself.

It is kept so the upgrade and compatibility tests read real format 1 files,
not ones recreated by newer code. To make it again (it never needs to be):

    git archive 9f39c3d src | tar -x -C /tmp/v1
    PYTHONPATH=/tmp/v1/src uv run --no-project --with backports.zstd \
        python tests/data/make_v1_store.py tests/data/v1-store

tests/data/v1-store-expected.json lists every current object (read from the
databases right after this script ran).
"""
import json, random, sys
from pathlib import Path
import mybucket
assert mybucket.__version__ == "0.1.0", mybucket.__version__
from mybucket import open_store
dest = Path(sys.argv[1])
T = 1794787200000
store = open_store(dest, clock=lambda: T)
b = store.bucket("archive", create=True)
rng = random.Random(42)
def day(seed):
    r = random.Random(seed)
    return "".join(json.dumps({"time": f"2026-11-14T{h:02d}:00", "steps": r.randrange(2000), "hr": r.randrange(55, 120)}) + "\n" for h in range(24)).encode()
b.put("days/2026-11-14.json", day(1), content_type="application/json", metadata={"source": "example"})
b.put("days/2026-11-14.json", day(2), content_type="application/json", metadata={"source": "example"})  # v1 in trash
b.put("days/2026-11-15.json", day(3), content_type="application/json")
b.put("empty", b"")
b.put("raw/noise.bin", rng.randbytes(1000))                 # inline raw
b.put("raw/photo.jpg", rng.randbytes(140_000), content_type="image/jpeg")  # raw blob file
lines = []
size = 0
while size <= 4 * 1024 * 1024 + 100:
    line = f'{{"t":{size},"hr":{rng.randrange(60, 64)},"ok":true}}\n'.encode()
    lines.append(line); size += len(line)
big = b"".join(lines)
b.put("logs/big.ndjson", iter([big[i:i + (1 << 20)] for i in range(0, len(big), 1 << 20)]))  # v1 streamed: one zstd frame, file
b.put("gone.txt", b"deleted later")
b.delete("gone.txt")
other = store.bucket("second", create=True)
other.put("a/b/c.txt", b"hello from format 1")
store.close()
