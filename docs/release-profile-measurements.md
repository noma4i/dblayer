# Release profile measurements

The workspace selects `opt-level=3`, `lto=false`, `codegen-units=1`, `strip="symbols"` and `panic="unwind"`.
The `none1` candidate keeps query CPU within the measured spread and minimizes the combined Android and iOS archive sizes.
Thin/Fat with one codegen unit saves less than 31 KB per Android library but adds over 36 MB to the iOS archive.
The `s` and `z` candidates lose query speed beyond the observed spread and are rejected.

## Method

Source revision: `a7615b6`. The core source stays unchanged. Measurements use Rust 1.99.0, LLVM 23.1.1, Android NDK 29.0.13599879, Apple M5 Max and macOS 27.0.1.
All timings below are OS `user+sys` CPU time. They are not wall time and were not measured in a quiet window.
Query samples had load averages above 10. External builds were present.
Each query has five samples, alternating all eight profiles before the next round.
The existing `rowbench` workload reads 10000 rows with scalar, list and object fields and a one-row cache.
`rowbench-cpu` changes only its timer to `getrusage(RUSAGE_SELF)`. The generated copy lives under `target/profile-measurements`.
Its dependency versions match the workspace lock file. There is no new benchmark framework.
The matrix tests all LTO/codegen-unit combinations at optimization level 3, then screens `s` and `z` against the baseline.
Panic unwinding stays enabled in every candidate.

| Candidate | Optimization | LTO | Codegen units |
| --- | --- | --- | ---: |
| base | 3 | false | 16 |
| s | s | false | 16 |
| z | z | false | 16 |
| thin16 | 3 | thin | 16 |
| fat16 | 3 | fat | 16 |
| none1 | 3 | false | 1 |
| thin1 | 3 | thin | 1 |
| fat1 | 3 | fat | 1 |

## Query CPU milliseconds

Cells contain median [minimum, maximum] from five runs. Fixture writes are outside these three timed operations.

| Candidate | read_ids | read_query | filter_plain |
| --- | ---: | ---: | ---: |
| base | 54.34 [52.01, 57.81] | 46.76 [45.64, 49.37] | 49.05 [48.25, 50.66] |
| s | 63.30 [60.50, 64.26] | 55.91 [54.52, 57.72] | 55.70 [55.52, 61.41] |
| z | 79.54 [78.38, 83.33] | 74.50 [72.16, 77.66] | 76.27 [73.13, 80.54] |
| thin16 | 54.93 [53.68, 68.49] | 46.64 [44.74, 51.89] | 50.60 [47.46, 55.82] |
| fat16 | 52.26 [51.47, 56.50] | 45.63 [42.54, 51.93] | 46.73 [45.93, 47.75] |
| none1 | 53.49 [52.51, 64.04] | 45.46 [44.03, 47.14] | 48.07 [46.36, 51.21] |
| thin1 | 51.42 [51.15, 55.83] | 46.02 [42.76, 57.11] | 47.16 [45.64, 52.39] |
| fat1 | 51.83 [51.00, 56.54] | 44.43 [43.81, 45.80] | 47.06 [46.93, 48.37] |

Every profile produced identical SQL counters at 1000 rows. Each query performed 12 statements.
The initial upsert performed 12007 statements and the one-field update performed 1013.

The sensitivity control alternated 10000 and 20000 rows five times. All three CPU intervals separated.

| Query | 10000 rows CPU ms [min, max] | 20000 rows CPU ms [min, max] |
| --- | ---: | ---: |
| read_ids | [54.64, 66.01] | [120.80, 128.13] |
| read_query | [47.16, 60.24] | [106.27, 119.16] |
| filter_plain | [49.93, 56.11] | [114.38, 119.02] |

## Android arm64-v8a bytes

| Candidate | strip none | strip debuginfo | strip symbols | .text |
| --- | ---: | ---: | ---: | ---: |
| base | 16364624 | 11920176 | 8654376 | 6775988 |
| s | 17011976 | 12490032 | 7386488 | 5171484 |
| z | 18969560 | 14031440 | 6481960 | 3934540 |
| thin16 | 16363480 | 11919048 | 8654312 | 6775964 |
| fat16 | 16363560 | 11919224 | 8654248 | 6775864 |
| none1 | 13576416 | 9394216 | 7450296 | 6008576 |
| thin1 | 13544440 | 9362176 | 7419624 | 5980496 |
| fat1 | 13545776 | 9363392 | 7419624 | 5980496 |

## Android armeabi-v7a bytes

| Candidate | strip none | strip debuginfo | strip symbols | .text |
| --- | ---: | ---: | ---: | ---: |
| base | 13222364 | 9213684 | 6409252 | 5510508 |
| s | 13666124 | 9601780 | 5161732 | 4154952 |
| z | 14707764 | 10581116 | 4616852 | 3590884 |
| thin16 | 13222464 | 9213756 | 6409236 | 5510556 |
| fat16 | 13220772 | 9212120 | 6409508 | 5510764 |
| none1 | 11346824 | 7575072 | 5856836 | 5115892 |
| thin1 | 11324128 | 7552540 | 5838660 | 5099956 |
| fat1 | 11329612 | 7558096 | 5839204 | 5100372 |

The strip comparison uses LLVM strip on copies of each built shared library.
All three strip modes preserve identical `.text` SHA256 values and dynamic export tables for each candidate and ABI.
Stripping removes metadata rather than query instructions, so these modes share the query CPU measurements.

## iOS archive size and cold compilation CPU

Each candidate starts with an empty target directory. Targets run in order: host benchmark, arm64 Android, armv7 Android, iOS device.
Host build scripts are reused by later targets in that sequence. Each CPU entry has one observation, without an estimated spread.
These are compilation CPU costs, not elapsed CI job times. Archive sizes here use `strip=none`.

| Candidate | iOS .a bytes | Host benchmark CPU s | ARM64 CPU s | ARMv7 CPU s | iOS CPU s |
| --- | ---: | ---: | ---: | ---: | ---: |
| base | 41002448 | 144.49 | 113.56 | 112.78 | 141.38 |
| s | 47787568 | 144.71 | 139.17 | 129.44 | 112.21 |
| z | 49110168 | 136.81 | 97.14 | 95.16 | 111.94 |
| thin16 | 94336240 | 273.11 | 171.05 | 144.82 | 161.32 |
| fat16 | 94334496 | 233.58 | 135.11 | 131.73 | 142.45 |
| none1 | 31137088 | 162.69 | 125.81 | 116.75 | 102.65 |
| thin1 | 67670248 | 147.65 | 101.17 | 93.61 | 95.47 |
| fat1 | 67665656 | 141.11 | 104.51 | 100.56 | 100.34 |

## Selected built artifacts

| Artifact | Before bytes | Selected bytes |
| --- | ---: | ---: |
| Android arm64-v8a .so | 12230856 | 7450936 |
| Android armeabi-v7a .so | 9500604 | 5856944 |
| iOS device .a | 41002448 | 31139208 |

The Android baseline comes from the previous default build script. The iOS baseline uses the explicit `base` candidate above.
Android reductions are 39.1% and 38.4%. The iOS archive reduction is 24.1%.
The selected Android `.text` sections occupy 6008576 and 5115332 bytes.
The Android source hash check passes, and archive-generated Kotlin has the same SHA256 as the host-generated bindings used by the JVM tests.

## Build result

The selected manifest produced a 31139208-byte iOS device archive with `strip=symbols`.
Generating Swift from that archive produced a nonempty 287719-byte source file.
Release tests passed: 1315 core tests and 3 bridge tests, with no failures or ignored tests.

The previous hash check failed with `Android SOURCE_HASH ignores the workspace release profile`.
The updated check passes with `Android SOURCE_HASH includes the workspace release profile`.
The hash now includes the root workspace manifest.

UniFFI 0.32.2 rejects a symbol-stripped shared library with `No UniFFI metadata found`.
Android binding generation now reads the existing static archive. Its generated Kotlin bytes match the host-generated bindings.
The host debug mode remains unchanged. The iOS generator already uses a static archive.

The default release build before this change produced 12230856 and 9500604 byte Android libraries.
Those default artifacts differ from explicit `strip=none`, which retains additional standard-library debug data.
The compiler describes stripping as a linking operation: <https://doc.rust-lang.org/rustc/codegen-options/index.html#strip>.
