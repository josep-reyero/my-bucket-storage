# Benchmarks

## bench_space.py: how much disk does a bucket save?

```
uv run python bench/bench_space.py                 # 365 days, 256 MiB streaming put
uv run python bench/bench_space.py --days 90 --stream-mib 64
uv run python bench/bench_space.py --json          # the same numbers as JSON
```

The script generates a synthetic fitness archive. It contains no real data:
everything comes from a seeded random generator (`--seed`, default 1).

- **One folder per day** (`daily/YYYY-MM-DD/`) with JSON documents shaped like
  wearable exports. Heart rate has a sample every 2 minutes and stress one every
  3 minutes. There are also sleep stages, steps in 15-minute buckets and a
  daily summary. Every document repeats the same field names thousands of times.
  About 4% of days are "not worn" and have almost empty documents.
- **`device.json` every day**: the same document until a firmware update. This
  is dedup across keys.
- **Unchanged re-puts**: every daily sync re-downloads the previous 6 days, as
  many sync tools do. Most puts therefore store content the bucket already has.
- **Revisions**: about 5% of days get a late sync that adds samples to
  yesterday's heart rate. That is an update, so the old version goes to the
  trash, which a new bucket keeps for 7 days.
- **Binary files**: FIT-like activity recordings (one packed record per
  second, roughly 20-110 KB), and a few incompressible photo-like files
  (0.8-2.5 MB).

The same final content is then stored in two ways.

1. As plain files in a directory. Disk use is counted from `st_blocks`, so
   block overhead (every small file takes at least one 4 KiB block on APFS
   and ext4) and the directories themselves are included.
2. In a mybucket bucket. Every file of the store directory is counted the
   same way, after the store is closed (SQLite then folds its WAL back into
   the database file).

It also reports:

- the size of every plain file compressed on its own with zstd -19
  (computed in memory, not written to disk), rounded up to blocks;
- put throughput over all puts (including the unchanged re-puts, which are
  cheap) and get throughput over every key, with every read verified;
- a streaming put and get of one large object (`--stream-mib`) in a separate
  process, with the peak Python heap (tracemalloc) and the growth of the
  process's peak RSS. This shows that memory does not grow with object size;
- dictionaries off and on: the fitness bucket after `train_dictionary()`
  and gc, and a second dataset of `--records` small JSON records (default
  20,000) stored once without a dictionary and once with one trained after
  the first `--train-after` (default 2,000).

Everything is written to a temporary directory and deleted afterwards
(`--keep DIR` keeps it).

### Results

Run on an Apple M1 Pro laptop (macOS, APFS, Python 3.12.8, backports.zstd)
with the defaults and mybucket 0.2: `uv run python bench/bench_space.py`. Read the speeds as
rough figures.

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
Put throughput (all puts)              705 puts/s, 10.3 MiB/s (21.8 s)
  new or changed content               114 puts/s, 2.2 MiB/s (2,359 puts, 20.6 s)
  unchanged re-puts                    12,073 puts/s, 167.6 MiB/s (12,994 puts, 1.1 s)
Get throughput (every key)             15,148 gets/s, 280.7 MiB/s (0.2 s)
Streaming put of 256.0 MiB             53.5 MiB/s, stored as 59.6 MiB
  peak memory during put               8.2 MiB Python heap, 16.4 MiB process RSS growth
  streaming get                        447.9 MiB/s, 6.4 MiB Python heap peak
Fitness bucket with a dictionary       12.1 MiB -> 12.1 MiB on disk; stored bytes 10.4 MiB -> 10.4 MiB (0.0% less)
  training                             3.4 s incl. recompressing 0 objects; 32.0 KiB dictionary, 1.1% saved on held-out objects
Small records (20,000, 4.5 MiB)        dictionary off 7.1 MiB on disk (3.4 MiB stored), on 5.1 MiB (1.0 MiB stored)
  put / get, dictionary off            220 puts/s, 32,570 gets/s
  put / get, dictionary on             204 puts/s, 37,437 gets/s (trained after 2,000 records in 0.3 s)
```

What the numbers say:

- **Space.** The bucket uses about a quarter of the disk the plain files use,
  and about 30% less than compressing each file on its own with zstd -19.
  Three things add up. Many small files no longer each round up to a 4 KiB
  block, since they live inline in one SQLite file. Identical documents
  (`device.json`, empty "not worn" days) are stored once. Level 19 works well
  on JSON with repeated field names. The JSON part alone (about 38 MiB)
  becomes the 7.1 MiB database. The incompressible photo-like files (5 MiB)
  are stored raw as 4 blob files, because compressing them would not save 5%.
  The 40 revised heart-rate files keep their previous version in the trash
  for 7 days. That space is included above, and gc frees it later.
- **Put speed.** A new or changed object costs about 8-9 ms on this machine:
  about 5 ms for the durable commit (`synchronous=FULL` plus `F_FULLFSYNC` on
  macOS) and about 4 ms for zstd -19 on a 15 KB document. Unchanged re-puts
  are hashed and compared only, with nothing compressed or written, so a sync
  tool that re-uploads a whole week every day pays almost nothing for it.
  `bucket.batch()` puts several objects under one commit when that matters.
- **Dictionaries.** On the fitness archive `train_dictionary()` measured a
  1.1 % saving on held-out documents and did not keep the dictionary (the
  threshold is 5 %): documents of 15-40 KB are already large enough for zstd
  -19. On 20,000 small records (150-400 bytes of JSON each; the first 2,000
  stored, then a dictionary trained and the existing ones recompressed, then
  the other 18,000 stored with it) stored bytes went from 3.4 MiB to 1.0 MiB
  (70 % less) and the bucket on disk from 7.1 MiB to 5.1 MiB; per-row SQLite
  overhead is most of what remains. Put and get speed hardly change.
- **Memory.** A 256 MiB streaming put peaks at about 6-8 MiB of Python heap,
  the same as a 64 MiB one (tests/test_stress.py checks that peak memory does
  not grow with size). A streaming get stays near 6 MiB.

## bench_incompressible.py: the worst case

```
uv run python bench/bench_incompressible.py
```

Stores the same random (incompressible) objects as plain files and in a
bucket for several object sizes and compares the disk used. See the README's
Space section for the measured table.
