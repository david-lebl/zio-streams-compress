# LZ4 Benchmark Results

**Environment:** JMH 1.37 · Java 17 (Azul JVM) · Scala 2.13 · ZIO 2.1.26 · `at.yawk.lz4:lz4-java:1.11.0`  
**Method:** 1 fork · 3 warmup iterations · 5 measurement iterations · 3 s per iteration · throughput mode

Two benchmark classes are compared:

- **`Lz4Benchmark`** — ZIO stream pipeline (`Lz4Compressor` / `Lz4Decompressor`), frame-format LZ4, default 256 KiB block size
- **`Lz4DirectBenchmark`** — `LZ4Factory` block API directly, no ZIO, no frame headers, pre-allocated byte arrays

---

## Part 1 — ZIO Stream Benchmark (`Lz4Benchmark`)

### Raw Results

```
Benchmark                (dataSize)    (dataType)   Mode  Cnt      Score      Error  Units
Lz4Benchmark.compress          1024  compressible  thrpt    5  10353.868 ± 4103.892  ops/s
Lz4Benchmark.compress          1024        random  thrpt    5  11362.036 ± 1345.099  ops/s
Lz4Benchmark.compress         65536  compressible  thrpt    5  10351.902 ± 2599.913  ops/s
Lz4Benchmark.compress         65536        random  thrpt    5   9323.903 ± 3789.062  ops/s
Lz4Benchmark.compress       1048576  compressible  thrpt    5   5345.523 ±  784.994  ops/s
Lz4Benchmark.compress       1048576        random  thrpt    5   3737.013 ±  931.621  ops/s

Lz4Benchmark.decompress        1024  compressible  thrpt    5  10799.508 ± 8338.710  ops/s
Lz4Benchmark.decompress        1024        random  thrpt    5  15156.113 ± 6097.986  ops/s
Lz4Benchmark.decompress       65536  compressible  thrpt    5  10351.560 ± 3876.641  ops/s
Lz4Benchmark.decompress       65536        random  thrpt    5   3865.884 ±  381.618  ops/s
Lz4Benchmark.decompress     1048576  compressible  thrpt    5   2066.990 ±  727.269  ops/s
Lz4Benchmark.decompress     1048576        random  thrpt    5    179.725 ±  131.933  ops/s

Lz4Benchmark.roundTrip         1024  compressible  thrpt    5   7019.698 ± 2092.596  ops/s
Lz4Benchmark.roundTrip         1024        random  thrpt    5   7434.145 ±  530.771  ops/s
Lz4Benchmark.roundTrip        65536  compressible  thrpt    5   6299.379 ±  275.701  ops/s
Lz4Benchmark.roundTrip        65536        random  thrpt    5   5447.002 ±  476.668  ops/s
Lz4Benchmark.roundTrip      1048576  compressible  thrpt    5   1653.812 ±   63.034  ops/s
Lz4Benchmark.roundTrip      1048576        random  thrpt    5   1464.956 ±  302.548  ops/s
```

### Data Throughput (ZIO stream)

Ops/s × payload size. Decompress is measured on uncompressed output size.

**Compress**

| Size  | Compressible | Random   |
|-------|-------------|----------|
| 1 KB  | 10 MB/s     | 11 MB/s  |
| 64 KB | 647 MB/s    | 583 MB/s |
| 1 MB  | 5.2 GB/s    | 3.6 GB/s |

**Decompress**

| Size  | Compressible | Random   |
|-------|-------------|----------|
| 1 KB  | 10 MB/s     | 15 MB/s  |
| 64 KB | 647 MB/s    | 242 MB/s |
| 1 MB  | 2.0 GB/s    | 178 MB/s |

**Round-trip**

| Size  | Compressible | Random   |
|-------|-------------|----------|
| 1 KB  | 7 MB/s      | 7 MB/s   |
| 64 KB | 394 MB/s    | 341 MB/s |
| 1 MB  | 1.6 GB/s    | 1.4 GB/s |

---

## Part 2 — Direct Block API Benchmark (`Lz4DirectBenchmark`)

Uses `LZ4Factory.fastestInstance()` with pre-allocated byte arrays. No ZIO, no frame format.

### Raw Results

```
Benchmark                      (compressorType)  (dataSize)    (dataType)   Mode  Cnt        Score         Error  Units
Lz4DirectBenchmark.compress                fast        1024  compressible  thrpt    5  2406564.730 ±  451029.067  ops/s
Lz4DirectBenchmark.compress                fast        1024        random  thrpt    5  1786859.708 ±  268315.186  ops/s
Lz4DirectBenchmark.compress                fast       65536  compressible  thrpt    5   361330.516 ±   48901.041  ops/s
Lz4DirectBenchmark.compress                fast       65536        random  thrpt    5   364172.444 ±   18110.668  ops/s
Lz4DirectBenchmark.compress                fast     1048576  compressible  thrpt    5    26284.950 ±    3884.139  ops/s
Lz4DirectBenchmark.compress                fast     1048576        random  thrpt    5    35182.606 ±    1679.400  ops/s
Lz4DirectBenchmark.compress                high        1024  compressible  thrpt    5   636474.079 ±  287351.177  ops/s
Lz4DirectBenchmark.compress                high        1024        random  thrpt    5   193520.935 ±   21711.217  ops/s
Lz4DirectBenchmark.compress                high       65536  compressible  thrpt    5   269201.805 ±   15027.285  ops/s
Lz4DirectBenchmark.compress                high       65536        random  thrpt    5     2372.340 ±     650.580  ops/s
Lz4DirectBenchmark.compress                high     1048576  compressible  thrpt    5    26419.373 ±     232.125  ops/s
Lz4DirectBenchmark.compress                high     1048576        random  thrpt    5       61.746 ±       2.926  ops/s

Lz4DirectBenchmark.decompress              fast        1024  compressible  thrpt    5  4735114.134 ± 2231602.602  ops/s
Lz4DirectBenchmark.decompress              fast        1024        random  thrpt    5  6330449.415 ± 1144454.987  ops/s
Lz4DirectBenchmark.decompress              fast       65536  compressible  thrpt    5   192745.144 ±   58944.020  ops/s
Lz4DirectBenchmark.decompress              fast       65536        random  thrpt    5  1049044.243 ±  126609.439  ops/s
Lz4DirectBenchmark.decompress              fast     1048576  compressible  thrpt    5     6831.753 ±    2927.303  ops/s
Lz4DirectBenchmark.decompress              fast     1048576        random  thrpt    5    63854.689 ±    6932.222  ops/s
Lz4DirectBenchmark.decompress              high        1024  compressible  thrpt    5  5297712.296 ±  103635.708  ops/s
Lz4DirectBenchmark.decompress              high        1024        random  thrpt    5  6581531.195 ±  327467.589  ops/s
Lz4DirectBenchmark.decompress              high       65536  compressible  thrpt    5   159698.215 ±   30020.576  ops/s
Lz4DirectBenchmark.decompress              high       65536        random  thrpt    5   661861.302 ±   62026.416  ops/s
Lz4DirectBenchmark.decompress              high     1048576  compressible  thrpt    5     8012.777 ±     367.974  ops/s
Lz4DirectBenchmark.decompress              high     1048576        random  thrpt    5    50665.838 ±    3276.073  ops/s
```

### Data Throughput (direct, fast compressor)

**Compress — fast**

| Size  | Compressible | Random    |
|-------|-------------|-----------|
| 1 KB  | 2.3 GB/s    | 1.7 GB/s  |
| 64 KB | 22.6 GB/s   | 22.8 GB/s |
| 1 MB  | 25.1 GB/s   | 33.6 GB/s |

**Decompress — fast**

| Size  | Compressible | Random    |
|-------|-------------|-----------|
| 1 KB  | 4.6 GB/s    | 6.2 GB/s  |
| 64 KB | 12.0 GB/s   | 65.6 GB/s |
| 1 MB  | 6.5 GB/s    | 61.0 GB/s |

**Compress — high (LZ4HC)**

| Size  | Compressible | Random     |
|-------|-------------|------------|
| 1 KB  | 621 MB/s    | 189 MB/s   |
| 64 KB | 16.8 GB/s   | 148 MB/s   |
| 1 MB  | 25.2 GB/s   | 59 MB/s    |

---

## Part 3 — ZIO Overhead

Comparing ZIO stream compress to direct fast compress. The frame-format overhead is also included in the ZIO column (frame headers, checksum), so the multiplier is an upper bound on pure ZIO cost.

**Compress overhead (compressible data)**

| Size  | Direct fast | ZIO stream | Overhead |
|-------|------------|------------|----------|
| 1 KB  | 2.3 GB/s   | 10 MB/s    | ~230×    |
| 64 KB | 22.6 GB/s  | 647 MB/s   | ~35×     |
| 1 MB  | 25.1 GB/s  | 5.2 GB/s   | ~4.8×    |

**Decompress overhead (random data, worst case)**

| Size  | Direct fast | ZIO stream | Overhead |
|-------|------------|------------|----------|
| 1 KB  | 6.2 GB/s   | 15 MB/s    | ~410×    |
| 64 KB | 65.6 GB/s  | 242 MB/s   | ~270×    |
| 1 MB  | 61.0 GB/s  | 178 MB/s   | ~340×    |

The **decompression overhead** is dramatically higher because the direct block API "decompressing" random (incompressible) data is essentially a memcpy (~61 GB/s), while the ZIO frame pipeline reads the same ~1 MB compressed stream through the `viaInputStreamByte` interop layer, incurring blocking thread-pool overhead per read regardless of chunk size.

---

## Part 4 — `chunkSize` Effect on ZIO Decompression

`Lz4Decompressor(chunkSize)` controls both the read buffer size passed to `BufferedInputStream`
and the output chunk size in `ZStream.fromInputStream`. Tested values: 4096 (default), 65536, 262144 (= LZ4 default block size).

### Raw Results

```
Benchmark                (chunkSize)  (dataSize)    (dataType)   Mode  Cnt      Score      Error  Units
Lz4Benchmark.decompress         4096       65536  compressible  thrpt    5  12696.576 ± 1111.923  ops/s
Lz4Benchmark.decompress         4096       65536        random  thrpt    5   3869.234 ±  342.883  ops/s
Lz4Benchmark.decompress         4096     1048576  compressible  thrpt    5   2182.337 ±  964.526  ops/s
Lz4Benchmark.decompress         4096     1048576        random  thrpt    5    196.293 ±   19.155  ops/s
Lz4Benchmark.decompress        65536       65536  compressible  thrpt    5  14566.214 ±  314.086  ops/s
Lz4Benchmark.decompress        65536       65536        random  thrpt    5   3269.009 ± 4640.032  ops/s
Lz4Benchmark.decompress        65536     1048576  compressible  thrpt    5   2876.177 ±  524.475  ops/s
Lz4Benchmark.decompress        65536     1048576        random  thrpt    5    206.961 ±    6.048  ops/s
Lz4Benchmark.decompress       262144       65536  compressible  thrpt    5  13454.811 ±  924.102  ops/s
Lz4Benchmark.decompress       262144       65536        random  thrpt    5   3720.427 ± 1467.828  ops/s
Lz4Benchmark.decompress       262144     1048576  compressible  thrpt    5   3212.021 ±  156.614  ops/s
Lz4Benchmark.decompress       262144     1048576        random  thrpt    5    194.267 ±   22.851  ops/s

Lz4Benchmark.roundTrip          4096       65536  compressible  thrpt    5   6392.071 ±  333.260  ops/s
Lz4Benchmark.roundTrip          4096       65536        random  thrpt    5   5458.470 ±  226.668  ops/s
Lz4Benchmark.roundTrip          4096     1048576  compressible  thrpt    5   1577.300 ±  336.852  ops/s
Lz4Benchmark.roundTrip          4096     1048576        random  thrpt    5   1484.211 ±   95.617  ops/s
Lz4Benchmark.roundTrip         65536       65536  compressible  thrpt    5   6093.325 ± 2320.210  ops/s
Lz4Benchmark.roundTrip         65536       65536        random  thrpt    5   5517.442 ±  228.381  ops/s
Lz4Benchmark.roundTrip         65536     1048576  compressible  thrpt    5   1710.119 ±  841.259  ops/s
Lz4Benchmark.roundTrip         65536     1048576        random  thrpt    5   1625.336 ± 1073.177  ops/s
Lz4Benchmark.roundTrip        262144       65536  compressible  thrpt    5   6460.389 ±  529.099  ops/s
Lz4Benchmark.roundTrip        262144       65536        random  thrpt    5   5425.275 ± 1101.936  ops/s
Lz4Benchmark.roundTrip        262144     1048576  compressible  thrpt    5   2053.022 ±  149.513  ops/s
Lz4Benchmark.roundTrip        262144     1048576        random  thrpt    5   1873.887 ±  509.447  ops/s
```

### Throughput vs chunkSize

**Decompress 1 MB — compressible**

| chunkSize | ops/s  | MB/s (output) | vs default |
|-----------|--------|---------------|------------|
| 4096      | 2,182  | 2.17 GB/s     | baseline   |
| 65536     | 2,876  | 2.87 GB/s     | +32%       |
| 262144    | 3,212  | 3.21 GB/s     | **+47%**   |

**Decompress 1 MB — random (incompressible)**

| chunkSize | ops/s | MB/s (output) | vs default |
|-----------|-------|---------------|------------|
| 4096      | 196   | 196 MB/s      | baseline   |
| 65536     | 207   | 207 MB/s      | +6% (noise)|
| 262144    | 194   | 194 MB/s      | flat       |

**Round-trip 1 MB (compress + decompress)**

| chunkSize | compressible | random  | vs default (compressible) |
|-----------|-------------|---------|--------------------------|
| 4096      | 1,577 ops/s | 1,484 ops/s | baseline               |
| 65536     | 1,710 ops/s | 1,625 ops/s | +8%                    |
| 262144    | 2,053 ops/s | 1,874 ops/s | **+30%**               |

### Finding: chunkSize helps compressible data but not incompressible data

**For compressible data**, increasing `chunkSize` to 262144 gives a consistent +47% at 1 MB and +15% at 64 KB.
The compressed form of highly compressible data is tiny (a few KB), so `LZ4FrameInputStream` reads it in very few input operations.
The bottleneck is on the output side: how many `ZIO.attemptBlockingInterrupt` calls `ZStream.fromInputStream` makes to read the expanded 1 MB.
Fewer, larger reads (larger `chunkSize`) means fewer blocking thread-pool submissions, directly improving throughput.

**For random (incompressible) data**, `chunkSize` has no measurable effect on standalone decompression.
With random data, the compressed frame is ~1 MB.
`LZ4FrameInputStream` must read that ~1 MB from the underlying `queueInputStream` (the ZIO-backed input stream) in its own internal read loop.
Those internal reads each invoke the ZIO stream-pull machinery, and their count is fixed by the LZ4 block size (4 × 256 KB blocks), not by `chunkSize`.
Each `ZIO.attemptBlockingInterrupt` on the output side costs approximately the same regardless of chunk size, and the blocking thread-pool submission cost per operation (~20 µs) accumulates to ~5 ms total for 256 output reads — equivalent to 256 reads × 20 µs regardless of whether reads are 4 KB or 256 KB.

**Round-trip improves for both data types** (+30% at 1 MB, compressible; +26% random) because in round-trip the compressor feeds the decompressor as a lazy stream in natural 64 KB compressed chunks. These smaller chunks reduce per-queue-item blocking overhead compared to the single pre-collected 1 MB chunk used in the standalone decompress benchmark.

### Recommendation

Set `chunkSize = 262144` (matching the LZ4 default block size) when decompressing large, compressible payloads: it is a no-cost change that recovers ~47% throughput at 1 MB.
For incompressible data the decompression bottleneck lies in the `viaInputStreamByte` architecture itself (blocking per-read fiber overhead) and cannot be addressed by tuning `chunkSize` alone.

---

## Key Findings

**Raw LZ4 fast compression runs at 22–34 GB/s.**
At 64 KB and above, the fast compressor saturates memory bandwidth. At 1 KB it drops to 1.7–2.3 GB/s due to fixed JNI call overhead amortised over fewer bytes.

**LZ4HC (high) on incompressible data collapses by 150×.**
At 64 KB of random data, LZ4HC achieves only 148 MB/s vs 22.8 GB/s for the fast compressor — a 154× penalty.
LZ4HC tries progressively harder to find matches; with random data there are none, so the search cost dominates.
For compressible data LZ4HC is only 1.3× slower, making the compressor choice data-dependent.

**Decompression of incompressible data is near-memcpy speed (61–66 GB/s) in the block API.**
With random data the block decompressor identifies there are no back-references and copies literal bytes directly, which is bounded only by memory bandwidth.
Compressible data (all-same byte) decompresses at 4.6–12 GB/s because resolving back-references is more complex than literal copy.

**ZIO stream overhead is 5–400× depending on payload size and data type.**
The overhead is smallest at large payloads with compressible data (~5×) and largest at small payloads or with incompressible data where the block API would otherwise be near memcpy speed.
The overhead shrinks as payload grows because the fixed per-operation ZIO cost (fiber scheduling, chunk management, InputStream interop) is amortised over more bytes.

**`chunkSize = 262144` recovers 47% of decompression throughput for compressible data.**
The default `chunkSize` of 4096 causes 256 blocking thread-pool submissions for 1 MB.
Raising it to 262144 reduces that to 4, cutting the ZIO scheduling overhead proportionally.
This has no effect for incompressible data where the blocking overhead originates from the input-side LZ4 frame reading loop, not the output chunk granularity.

**At 1 KB, ZIO overhead entirely dominates.**
All ZIO stream operations land at 7–15 MB/s regardless of data type.
The algorithm itself would run at 1.7–6.2 GB/s without the wrapper.
Callers processing many small payloads should batch them or use the block API directly.

---

## Caveats

- Error margins are wide for some rows (especially 1 KB results and random 64 KB decompress at chunkSize=65536). One fork over five short iterations is not enough to stabilise all results; treat high-variance rows as directional.
- ZIO vs direct comparison also includes frame-format overhead (magic numbers, block headers, optional checksums). Pure ZIO scheduler cost is somewhat lower than the multipliers shown.
- All ZIO compression used the default `BlockSize256KiB`. Larger blocks may improve throughput for large payloads.
- A production run should use at least two forks (`-f2`).

---

## Reproducing

```bash
# ZIO stream benchmark (all chunkSizes, all sizes)
sbt "benchmarks/Jmh/run -f1 -wi 3 -i 5 .*Lz4Benchmark.*"

# chunkSize effect only (decompress + roundTrip, two largest sizes)
sbt "benchmarks/Jmh/run -f1 -wi 3 -i 5 -p dataSize=65536,1048576 .*Lz4Benchmark\.(decompress|roundTrip)"

# Direct block API benchmark
sbt "benchmarks/Jmh/run -f1 -wi 3 -i 5 .*Lz4DirectBenchmark.*"

# Fast compressor only, quick run
sbt "benchmarks/Jmh/run -f1 -wi 1 -i 1 -p compressorType=fast .*Lz4DirectBenchmark.*"
```
