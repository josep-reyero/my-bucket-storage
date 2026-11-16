"""How much disk do incompressible objects take in a bucket, compared with plain files?

The worst case for mybucket's space saving: random bytes (like already
compressed photos or video) cannot be compressed, so a bucket can only save
the per-file block rounding of small files, and it adds its own overhead.

For each object size it stores the same N random objects as plain files and
in a new bucket, closes the store, and counts the disk used (st_blocks) of
every file. It also reports what an empty bucket costs.

Usage (from the repository root):

    uv run python bench/bench_incompressible.py
    uv run python bench/bench_incompressible.py --count 1000 --sizes 500,4000,200000
"""

from __future__ import annotations

import argparse
import os
import random
import tempfile
from pathlib import Path

from mybucket import open_store


def disk_bytes(root: Path) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            total += os.lstat(os.path.join(dirpath, name)).st_blocks * 512
    return total


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n} B"


def measure(tmp: Path, size: int, count: int, seed: int) -> tuple[int, int]:
    rng = random.Random(seed)
    objects = [rng.randbytes(size) for _ in range(count)]
    plain = tmp / f"plain-{size}"
    for i, data in enumerate(objects):
        path = plain / f"{i % 10}" / f"obj-{i}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    store_path = tmp / f"store-{size}"
    store = open_store(store_path)
    bucket = store.bucket("data", create=True)
    with bucket.batch() as tx:
        for i, data in enumerate(objects):
            tx.put(f"{i % 10}/obj-{i}.bin", data)
    store.close()
    return disk_bytes(plain), disk_bytes(store_path / "buckets" / "data")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--count", type=int, default=500, help="objects per size (default 500)")
    parser.add_argument("--sizes", default="500,2000,4000,8000,64000,200000", help="comma-separated object sizes")
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    sizes = [int(s) for s in args.sizes.split(",")]
    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        empty = open_store(tmp / "empty")
        empty.create_bucket("data")
        empty.close()
        print(f"Empty bucket on disk: {human(disk_bytes(tmp / 'empty' / 'buckets' / 'data'))}")
        print(f"{args.count} random (incompressible) objects per size; disk use counted from st_blocks")
        print(f"{'object size':>12}  {'logical':>10}  {'plain files':>12}  {'bucket':>10}  {'bucket / plain':>14}")
        for size in sizes:
            plain, bucket = measure(tmp, size, args.count, args.seed)
            logical = size * args.count
            print(f"{size:>10} B  {human(logical):>10}  {human(plain):>12}  {human(bucket):>10}  {bucket / plain:>13.2f}x")


if __name__ == "__main__":
    main()
