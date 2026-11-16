"""Space and speed benchmark on a synthetic fitness archive.

Generates a realistic but entirely synthetic archive (no real data): one
folder of JSON documents per day shaped like wearable exports (heart rate,
stress, sleep, steps, a daily summary, plus device/settings documents that
rarely change), binary activity recordings, and a few incompressible
photo-like files. It mimics a sync tool that re-downloads the last week on
every run, so most puts are unchanged re-puts, and some days are revised
later (late sync) so old versions land in the trash.

The same final content is stored two ways and compared:

- as plain files in a directory (bytes on disk counted from st_blocks, so
  filesystem block overhead is included);
- in a mybucket bucket (every file of the store directory counted the same way).

It also measures put and get throughput and the peak memory of one large
streaming put and streaming get, in a separate process so earlier phases do
not distort the numbers.

Then it compares trained zstd dictionaries off and on:

- the same fitness bucket after ``train_dictionary()`` (which recompresses
  the stored small objects) and gc;
- a second dataset of many small, similar records (one JSON document per
  workout lap / notification, a few hundred bytes each), stored once without
  a dictionary and once with one trained after the first 2,000 records.

Usage (from the repository root):

    uv run python bench/bench_space.py              # 365 days, 256 MiB stream
    uv run python bench/bench_space.py --days 90 --stream-mib 64
    uv run python bench/bench_space.py --json       # machine-readable result
    uv run python bench/bench_space.py --records 50000

Everything is written to a temporary directory that is deleted afterwards,
unless --keep DIR is given.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import random
import resource
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

from mybucket import _zstd, open_store

MiB = 1024 * 1024
RESYNC_DAYS = 6  # each daily sync re-downloads the previous 6 days unchanged


# -- synthetic data --------------------------------------------------------------


def _iso(day: dt.date, seconds: int) -> str:
    t = dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC) + dt.timedelta(seconds=seconds)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _walk(rng: random.Random, n: int, start: int, lo: int, hi: int, step: int) -> list[int]:
    out, v = [], start
    for _ in range(n):
        v = min(hi, max(lo, v + rng.randint(-step, step)))
        out.append(v)
    return out


def heart_rate(day: dt.date, rng: random.Random, worn: bool, extra: int = 0) -> bytes:
    n = (720 if worn else 0) + extra  # one sample every 2 minutes
    values = _walk(rng, n, 64, 42, 175, 4)
    doc = {
        "date": day.isoformat(),
        "type": "heart_rate",
        "unit": "bpm",
        "source": "wrist",
        "resting": min(values) if values else None,
        "samples": [{"time": _iso(day, i * 120), "bpm": v} for i, v in enumerate(values)],
    }
    return json.dumps(doc, indent=1).encode()


def stress(day: dt.date, rng: random.Random, worn: bool) -> bytes:
    values = _walk(rng, 480 if worn else 0, 30, 0, 100, 6)  # every 3 minutes
    cat = lambda v: "rest" if v < 25 else "low" if v < 50 else "medium" if v < 75 else "high"  # noqa: E731
    doc = {
        "date": day.isoformat(),
        "type": "stress",
        "samples": [{"time": _iso(day, i * 180), "level": v, "category": cat(v)} for i, v in enumerate(values)],
    }
    return json.dumps(doc, indent=1).encode()


def sleep(day: dt.date, rng: random.Random, worn: bool) -> bytes:
    stages, t = [], -2 * 3600 + rng.randint(-1800, 1800)
    end = 7 * 3600 + rng.randint(-1800, 1800)
    while worn and t < end:
        length = rng.randint(5, 50) * 60
        stages.append(
            {
                "start": _iso(day, t),
                "end": _iso(day, t + length),
                "stage": rng.choice(["light", "light", "deep", "rem", "awake"]),
            }
        )
        t += length
    doc = {"date": day.isoformat(), "type": "sleep", "score": rng.randint(50, 95) if worn else None, "stages": stages}
    return json.dumps(doc, indent=1).encode()


def steps(day: dt.date, rng: random.Random, worn: bool) -> bytes:
    buckets = [
        {"start": _iso(day, i * 900), "steps": rng.choice([0, 0, 0, rng.randint(0, 1800)])}
        for i in range(96 if worn else 0)
    ]
    doc = {"date": day.isoformat(), "type": "steps", "total": sum(b["steps"] for b in buckets), "buckets": buckets}
    return json.dumps(doc, indent=1).encode()


def summary(day: dt.date, rng: random.Random) -> bytes:
    doc = {"date": day.isoformat(), "type": "daily_summary"}
    for name in [
        "calories_total",
        "calories_active",
        "distance_m",
        "floors",
        "intensity_minutes",
        "body_battery_high",
        "body_battery_low",
        "respiration_avg",
        "spo2_avg",
        "hrv_ms",
        "vo2max",
        "training_load",
        "recovery_hours",
        "hydration_ml",
        "weight_g",
    ]:
        doc[name] = rng.randint(0, 5000)
    return json.dumps(doc, indent=1).encode()


def device(firmware: int) -> bytes:
    # Identical on most days: a dedup case across keys.
    doc = {
        "type": "device",
        "model": "generic-watch-2",
        "firmware": f"{firmware}.{firmware % 7}.0",
        "sensors": ["optical_hr", "accelerometer", "gyroscope", "barometer", "gps", "spo2"],
        "settings": {"hr_interval_s": 120, "stress_interval_s": 180, "units": "metric", "locale": "en"},
    }
    return json.dumps(doc, indent=1).encode()


def activity(rng: random.Random) -> bytes:
    """A FIT-like binary recording: a header and one record per second."""
    seconds = rng.randint(20, 100) * 60
    lat, lon, hr = 400_000_000 + rng.randint(0, 10**6), -30_000_000 + rng.randint(0, 10**6), 120
    out = bytearray(b"SYNTHFIT" + struct.pack("<I", seconds))
    ts = 1_000_000_000 + rng.randint(0, 10**7)
    for i in range(seconds):
        lat += rng.randint(-90, 90)
        lon += rng.randint(-90, 90)
        hr = min(195, max(90, hr + rng.randint(-2, 2)))
        out += struct.pack("<IiiBBHH", ts + i, lat, lon, hr, rng.randint(80, 95), rng.randint(150, 400), i % 1000)
    return bytes(out)


def build_archive(days: int, seed: int):
    """Return (operations, final): operations is the put sequence of a daily
    sync tool; final maps each key to its last content."""
    rng = random.Random(seed)
    start = dt.date(2025, 1, 1)
    per_day: list[list[tuple[str, bytes]]] = []
    ops: list[tuple[str, bytes]] = []
    revisions: dict[int, list[tuple[str, bytes]]] = {}
    firmware = 11
    for d in range(days):
        day = start + dt.timedelta(days=d)
        worn = rng.random() > 0.04
        if rng.random() < 0.02:
            firmware += 1
        base = f"daily/{day.isoformat()}"
        files = [
            (f"{base}/heart_rate.json", heart_rate(day, rng, worn)),
            (f"{base}/stress.json", stress(day, rng, worn)),
            (f"{base}/sleep.json", sleep(day, rng, worn)),
            (f"{base}/steps.json", steps(day, rng, worn)),
            (f"{base}/summary.json", summary(day, rng)),
            (f"{base}/device.json", device(firmware)),
        ]
        if worn and rng.random() < 0.33:
            files.append((f"activities/{day.isoformat()}-{rng.randint(1, 9)}.fit", activity(rng)))
        if rng.random() < 0.02:
            files.append((f"photos/{day.isoformat()}-scale.jpg", rng.randbytes(rng.randint(800_000, 2_500_000))))
        if worn and rng.random() < 0.05:
            # A late sync adds a few samples to yesterday's heart rate the next day.
            revisions[d + 1] = [(f"{base}/heart_rate.json", heart_rate(day, random.Random(seed * 7919 + d), worn, 30))]
        per_day.append(files)
        # Today's sync: new files, any revisions, then the last week again.
        ops.extend(files)
        ops.extend(revisions.pop(d, []))
        for back in range(1, RESYNC_DAYS + 1):
            if d - back >= 0:
                ops.extend((k, v) for k, v in per_day[d - back] if not k.startswith(("photos/", "activities/")))
    final: dict[str, bytes] = {}
    for key, data in ops:
        final[key] = data
    return ops, final


def small_record(i: int, rng: random.Random) -> bytes:
    """One small JSON document, as a per-lap or per-notification export would write."""
    kind = rng.choice(["lap", "lap", "lap", "notification", "hr_zone"])
    start = dt.datetime(2025, 1, 1, tzinfo=dt.UTC) + dt.timedelta(minutes=7 * i)
    doc: dict = {"id": f"rec-{i:07d}", "type": kind, "start": start.strftime("%Y-%m-%dT%H:%M:%SZ")}
    if kind == "lap":
        doc |= {
            "activity": rng.choice(["running", "cycling", "walking"]),
            "lap": rng.randint(1, 30),
            "duration_s": rng.randint(120, 900),
            "distance_m": rng.randint(300, 5000),
            "avg_hr": rng.randint(110, 175),
            "max_hr": rng.randint(150, 195),
            "avg_cadence": rng.randint(70, 95),
            "elevation_gain_m": rng.randint(0, 80),
            "calories": rng.randint(20, 200),
            "device": "generic-watch-2",
        }
    elif kind == "notification":
        doc |= {
            "app": rng.choice(["calendar", "messages", "weather"]),
            "title": rng.choice(["Reminder", "New message", "Rain expected"]),
            "shown_s": rng.randint(1, 10),
            "dismissed": rng.random() < 0.8,
            "device": "generic-watch-2",
        }
    else:
        doc |= {"zones_s": [rng.randint(0, 1200) for _ in range(5)], "threshold_hr": 172, "max_hr": 190}
    return json.dumps(doc, indent=1).encode()


# -- disk accounting ----------------------------------------------------------------


def disk_usage(root: Path) -> tuple[int, int, int]:
    """(apparent bytes of files, bytes on disk of files and directories, number of files)."""
    apparent = on_disk = files = 0
    seen: set[tuple[int, int]] = set()
    for dirpath, _dirnames, filenames in os.walk(root):
        st = os.lstat(dirpath)
        on_disk += st.st_blocks * 512
        for name in filenames:
            st = os.lstat(os.path.join(dirpath, name))
            if (st.st_dev, st.st_ino) in seen:
                continue
            seen.add((st.st_dev, st.st_ino))
            apparent += st.st_size
            on_disk += st.st_blocks * 512
            files += 1
    return apparent, on_disk, files


def disk_usage_file(path: Path) -> int:
    try:
        return os.lstat(path).st_blocks * 512
    except FileNotFoundError:
        return 0


def write_plain(root: Path, final: dict[str, bytes]) -> None:
    for key, data in final.items():
        path = root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


def zstd_files_estimate(final: dict[str, bytes], block: int) -> int:
    """What each file compressed on its own with zstd -19 would take, rounded up to blocks."""
    total = 0
    for data in final.values():
        packed = _zstd.compress(data, 19)
        total += max(1, math.ceil(min(len(packed), len(data)) / block)) * block
    return total


# -- dictionaries off and on -----------------------------------------------------------------


def bucket_disk(store_dir: Path, name: str) -> int:
    bdir = store_dir / "buckets" / name
    return disk_usage(bdir)[1]


def measure_records(work: Path, n: int, seed: int, train_after: int) -> dict:
    """Store ``n`` small records without and with a dictionary; sizes and speeds."""
    rng = random.Random(seed + 101)
    records = [(f"laps/{i // 1000:03d}/{i:07d}.json", small_record(i, rng)) for i in range(n)]
    out: dict = {"records": n, "logical_bytes": sum(len(v) for _, v in records), "train_after": train_after}
    for mode in ("off", "on"):
        store_dir = work / f"records-{mode}"
        store = open_store(store_dir)
        b = store.create_bucket("records")
        put_s = train_s = 0.0
        for i, (key, data) in enumerate(records):
            if mode == "on" and i == train_after:
                t0 = time.perf_counter()
                report = b.train_dictionary()
                train_s = time.perf_counter() - t0
                out["train_report"] = {k: report[k] for k in ("kept", "dict_bytes", "samples", "saving", "recompressed")}
            t0 = time.perf_counter()
            b.put(key, data)
            put_s += time.perf_counter() - t0
        t0 = time.perf_counter()
        for key, data in records:
            if b.get(key) != data:
                raise SystemExit(f"read back of {key} differs")
        get_s = time.perf_counter() - t0
        st = b.stats()
        store.close()
        out[mode] = {
            "on_disk": bucket_disk(store_dir, "records"),
            "stored_bytes": st.stored_bytes,
            "dict_blobs": st.dict_blobs,
            "put_s": put_s,
            "get_s": get_s,
            "train_s": train_s,
        }
    return out


def measure_fitness_dict(store_dir: Path) -> dict:
    """Train a dictionary on the fitness bucket (recompressing it), gc, and measure again."""
    before = bucket_disk(store_dir, "fitness")
    store = open_store(store_dir)
    b = store.bucket("fitness")
    stored_before = b.stats().stored_bytes
    t0 = time.perf_counter()
    report = b.train_dictionary()
    train_s = time.perf_counter() - t0
    b.gc()
    st = b.stats()
    store.close()
    return {
        "on_disk_before": before,
        "on_disk_after": bucket_disk(store_dir, "fitness"),
        "stored_before": stored_before,
        "stored_after": st.stored_bytes,
        "train_s": train_s,
        "kept": report["kept"],
        "saving_held_out": report["saving"],
        "recompressed": report["recompressed"],
        "dict_bytes": report["dict_bytes"],
        "dict_blobs": st.dict_blobs,
    }


# -- streaming memory, measured in a child process ---------------------------------------


STREAM_DISTINCT_CHUNKS = 32  # a 32 MiB cycle: longer than zstd's level-9 window, so no free repeats


def _stream_source(seed: int) -> list[bytes]:
    """Distinct 1 MiB chunks of accelerometer-like JSON lines, built before measuring."""
    rng = random.Random(seed)
    chunks, t = [], 0
    for _ in range(STREAM_DISTINCT_CHUNKS):
        lines = []
        size = 0
        while size < MiB:
            t += 20
            line = b'{"t":%d,"ax":%d,"ay":%d,"az":%d}\n' % (
                t,
                rng.randint(-2000, 2000),
                rng.randint(-2000, 2000),
                rng.randint(-2000, 2000),
            )
            lines.append(line)
            size += len(line)
        chunks.append(b"".join(lines)[:MiB])
    return chunks


def _stream_chunks(total: int, source: list[bytes]):
    sent, n = 0, 0
    while sent < total:
        chunk = source[n % len(source)][: min(MiB, total - sent)]
        yield chunk
        sent += len(chunk)
        n += 1


def measure_stream(store_dir: str, mib: int) -> dict:
    def rss() -> int:
        r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return r if sys.platform == "darwin" else r * 1024  # macOS reports bytes, Linux KiB

    b = open_store(store_dir).bucket("stream", create=True)
    chunks = _stream_chunks(mib * MiB, _stream_source(3))
    rss_before = rss()
    tracemalloc.start()
    t0 = time.perf_counter()
    info = b.put("raw/accelerometer.jsonl", chunks)
    put_s = time.perf_counter() - t0
    _, put_peak = tracemalloc.get_traced_memory()
    rss_after_put = rss()
    tracemalloc.reset_peak()
    t0 = time.perf_counter()
    h = hashlib.sha256()
    with b.open("raw/accelerometer.jsonl") as s:
        while chunk := s.read(MiB):
            h.update(chunk)
    get_s = time.perf_counter() - t0
    _, get_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert h.hexdigest() == info.etag
    return {
        "bytes": info.size,
        "stored": info.stored_size,
        "put_s": put_s,
        "get_s": get_s,
        "put_peak_python": put_peak,
        "get_peak_python": get_peak,
        "rss_growth_put": rss_after_put - rss_before,
        "rss_growth_total": rss() - rss_before,
    }


# -- main -------------------------------------------------------------------------------------


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024 or unit == "GiB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024
    return str(n)


def run(args) -> dict:
    work = Path(args.keep) if args.keep else Path(tempfile.mkdtemp(prefix="mybucket-bench-"))
    work.mkdir(parents=True, exist_ok=True)
    try:
        t0 = time.perf_counter()
        ops, final = build_archive(args.days, args.seed)
        gen_s = time.perf_counter() - t0
        logical = sum(len(v) for v in final.values())

        plain = work / "plain"
        write_plain(plain, final)
        p_apparent, p_disk, p_files = disk_usage(plain)
        block = os.statvfs(work).f_frsize

        store_dir = work / "store"
        store = open_store(store_dir)
        bucket = store.create_bucket("fitness", trash_days=args.trash_days)
        statuses = {"created": 0, "updated": 0, "unchanged": 0}
        status_s = dict.fromkeys(statuses, 0.0)
        status_bytes = dict.fromkeys(statuses, 0)
        put_bytes = 0
        t0 = time.perf_counter()
        for key, data in ops:
            t1 = time.perf_counter()
            status = bucket.put(key, data).status
            status_s[status] += time.perf_counter() - t1
            status_bytes[status] += len(data)
            statuses[status] += 1
            put_bytes += len(data)
        put_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        got = 0
        for key, data in final.items():
            out = bucket.get(key)
            got += len(out)
            if out != data:
                raise SystemExit(f"read back of {key} differs")
        get_s = time.perf_counter() - t0
        stats = bucket.stats()
        store.close()  # last connection: SQLite checkpoints and removes the WAL
        s_apparent, s_disk, s_files = disk_usage(store_dir)
        bucket_dir = store_dir / "buckets" / "fitness"
        db_disk = sum(disk_usage_file(bucket_dir / name) for name in ("bucket.sqlite", "bucket.sqlite-wal"))
        blobs_disk = disk_usage(bucket_dir / "blobs")[1]

        zstd_est = zstd_files_estimate(final, block) if not args.skip_zstd_estimate else None

        child = subprocess.run(
            [sys.executable, __file__, "--measure-stream", str(work / "stream-store"), str(args.stream_mib)],
            capture_output=True,
            text=True,
            check=True,
        )
        stream = json.loads(child.stdout)
        fitness_dict = measure_fitness_dict(store_dir)
        records = measure_records(work, args.records, args.seed, args.train_after)
        return {
            "days": args.days,
            "seed": args.seed,
            "trash_days": args.trash_days,
            "keys": len(final),
            "puts": len(ops),
            "statuses": statuses,
            "generate_s": gen_s,
            "logical_bytes": logical,
            "plain": {"apparent": p_apparent, "on_disk": p_disk, "files": p_files, "block": block},
            "bucket": {
                "apparent": s_apparent,
                "on_disk": s_disk,
                "files": s_files,
                "db_on_disk": db_disk,
                "blobs_on_disk": blobs_disk,
                "stats": stats.to_dict(),
            },
            "zstd_files_estimate": zstd_est,
            "put": {
                "seconds": put_s,
                "bytes": put_bytes,
                "ops": len(ops),
                "by_status": {
                    k: {"ops": statuses[k], "seconds": status_s[k], "bytes": status_bytes[k]} for k in statuses
                },
            },
            "get": {"seconds": get_s, "bytes": got, "ops": len(final)},
            "stream": stream,
            "fitness_dict": fitness_dict,
            "small_records": records,
            "python": sys.version.split()[0],
            "zstd_backend": _zstd.BACKEND,
        }
    finally:
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)


def _rate(by_status: dict, names: tuple[str, ...]) -> str:
    ops = sum(by_status[n]["ops"] for n in names)
    secs = sum(by_status[n]["seconds"] for n in names) or 1e-9
    size = sum(by_status[n]["bytes"] for n in names)
    return f"{ops / secs:,.0f} puts/s, {size / MiB / secs:,.1f} MiB/s ({ops:,} puts, {secs:.1f} s)"


def print_table(r: dict) -> None:
    logical = r["logical_bytes"]
    rows = [
        (
            "Archive",
            f"{r['days']} days, {r['keys']:,} keys, {r['puts']:,} puts "
            f"({r['statuses']['created']:,} created, {r['statuses']['updated']:,} updated, "
            f"{r['statuses']['unchanged']:,} unchanged)",
        ),
        ("Logical bytes (final content)", human(logical)),
        ("Plain files: apparent size", human(r["plain"]["apparent"])),
        (
            "Plain files: on disk (st_blocks)",
            f"{human(r['plain']['on_disk'])}  ({r['plain']['files']:,} files, {r['plain']['block']} B blocks)",
        ),
    ]
    if r["zstd_files_estimate"] is not None:
        rows.append(("Plain files, each zstd -19 (computed)", human(r["zstd_files_estimate"])))
    st = r["bucket"]["stats"]
    rows += [
        ("Bucket: on disk (st_blocks)", f"{human(r['bucket']['on_disk'])}  ({r['bucket']['files']:,} files)"),
        (
            "Bucket: database / blob files on disk",
            f"{human(r['bucket']['db_on_disk'])} / "
            f"{human(r['bucket']['blobs_on_disk'])}  "
            f"({st['inline_blobs']:,} inline, {st['file_blobs']:,} file blobs)",
        ),
        ("Bucket: distinct contents", f"{st['blobs']:,} blobs, {st['trash_versions']:,} old versions in trash"),
        ("Ratio plain on disk / bucket on disk", f"{r['plain']['on_disk'] / r['bucket']['on_disk']:.2f}x"),
        ("Ratio logical / bucket on disk", f"{logical / r['bucket']['on_disk']:.2f}x"),
        (
            "Put throughput (all puts)",
            f"{r['put']['ops'] / r['put']['seconds']:,.0f} puts/s, "
            f"{r['put']['bytes'] / MiB / r['put']['seconds']:,.1f} MiB/s "
            f"({r['put']['seconds']:.1f} s)",
        ),
        (
            "  new or changed content",
            _rate(r["put"]["by_status"], ("created", "updated")),
        ),
        ("  unchanged re-puts", _rate(r["put"]["by_status"], ("unchanged",))),
        (
            "Get throughput (every key)",
            f"{r['get']['ops'] / r['get']['seconds']:,.0f} gets/s, "
            f"{r['get']['bytes'] / MiB / r['get']['seconds']:,.1f} MiB/s "
            f"({r['get']['seconds']:.1f} s)",
        ),
    ]
    s = r["stream"]
    rows += [
        (
            f"Streaming put of {human(s['bytes'])}",
            f"{s['bytes'] / MiB / s['put_s']:,.1f} MiB/s, stored as {human(s['stored'])}",
        ),
        (
            "  peak memory during put",
            f"{human(s['put_peak_python'])} Python heap, {human(s['rss_growth_put'])} process RSS growth",
        ),
        (
            "  streaming get",
            f"{s['bytes'] / MiB / s['get_s']:,.1f} MiB/s, {human(s['get_peak_python'])} Python heap peak",
        ),
    ]
    fd = r["fitness_dict"]
    rows += [
        (
            "Fitness bucket with a dictionary",
            f"{human(fd['on_disk_before'])} -> {human(fd['on_disk_after'])} on disk; stored bytes "
            f"{human(fd['stored_before'])} -> {human(fd['stored_after'])} "
            f"({1 - fd['stored_after'] / fd['stored_before']:.1%} less)",
        ),
        (
            "  training",
            f"{fd['train_s']:.1f} s incl. recompressing {fd['recompressed']:,} objects; "
            f"{human(fd['dict_bytes'])} dictionary, {fd['saving_held_out']:.1%} saved on held-out objects",
        ),
    ]
    sr = r["small_records"]
    off, on = sr["off"], sr["on"]
    rows += [
        (
            f"Small records ({sr['records']:,}, {human(sr['logical_bytes'])})",
            f"dictionary off {human(off['on_disk'])} on disk ({human(off['stored_bytes'])} stored), "
            f"on {human(on['on_disk'])} ({human(on['stored_bytes'])} stored)",
        ),
        (
            "  put / get, dictionary off",
            f"{sr['records'] / off['put_s']:,.0f} puts/s, {sr['records'] / off['get_s']:,.0f} gets/s",
        ),
        (
            "  put / get, dictionary on",
            f"{sr['records'] / on['put_s']:,.0f} puts/s, {sr['records'] / on['get_s']:,.0f} gets/s "
            f"(trained after {sr['train_after']:,} records in {on['train_s']:.1f} s)",
        ),
    ]
    width = max(len(k) for k, _ in rows)
    print(f"mybucket space benchmark (Python {r['python']}, {r['zstd_backend']}, trash_days={r['trash_days']})")
    print("-" * (width + 50))
    for k, v in rows:
        print(f"{k:<{width}}  {v}")


def main(argv=None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    if argv[:1] == ["--measure-stream"]:
        print(json.dumps(measure_stream(argv[1], int(argv[2]))))
        return
    parser = argparse.ArgumentParser(description="Space and speed benchmark on a synthetic fitness archive.")
    parser.add_argument("--days", type=int, default=365, help="days of data to generate (default 365)")
    parser.add_argument("--seed", type=int, default=1, help="random seed (default 1)")
    parser.add_argument("--stream-mib", type=int, default=256, help="size of the streaming put in MiB (default 256)")
    parser.add_argument("--trash-days", type=int, default=7, help="bucket trash_days (default 7, as a new bucket)")
    parser.add_argument("--records", type=int, default=20000, help="small records for the dictionary test (default 20000)")
    parser.add_argument("--train-after", type=int, default=2000, help="records stored before training (default 2000)")
    parser.add_argument("--skip-zstd-estimate", action="store_true", help="skip the per-file zstd -19 estimate")
    parser.add_argument("--keep", metavar="DIR", help="write into DIR and keep it, instead of a temp dir")
    parser.add_argument("--json", action="store_true", help="print the result as JSON")
    args = parser.parse_args(argv)
    result = run(args)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_table(result)


if __name__ == "__main__":
    main()
