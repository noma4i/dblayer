# Release profile and Android keep rules

The shared release profile selects optimization level 3, no LTO, one codegen unit, symbol stripping and panic unwinding.
The selected profile preserves query CPU performance within the measured spread and gives the smallest combined package size among the tested candidates.

| Artifact | Before bytes | After bytes | Reduction |
| --- | ---: | ---: | ---: |
| Android arm64-v8a .so | 12230856 | 7450936 | 39.1% |
| Android armeabi-v7a .so | 9500604 | 5856944 | 38.4% |
| iOS device .a | 41002448 | 31139208 | 24.1% |

Android baseline files came from the previous default build. The iOS baseline uses the explicit optimization-3, no-LTO, 16-unit candidate.
Thin/Fat with one unit adds over 36 MB to the iOS archive while saving less than 31 KB per Android library.
Optimization levels `s` and `z` lose query speed beyond the observed spread.

| Query CPU milliseconds | Baseline median [min, max] | Selected median [min, max] |
| --- | ---: | ---: |
| read_ids | 54.34 [52.01, 57.81] | 53.49 [52.51, 64.04] |
| read_query | 46.76 [45.64, 49.37] | 45.46 [44.03, 47.14] |
| filter_plain | 49.05 [48.25, 50.66] | 48.07 [46.36, 51.21] |

Five query samples alternate all eight profiles. Each profile has one cold compilation CPU observation and deterministic size measurements.
The timings use OS user+system CPU, without a quiet window. No wall-time performance claim is made.
SQL counts match across every profile. The CPU measurement distinguishes doubled work for all three queries.
See [the complete profile tables](release-profile-measurements.md) for strip variants, both ABIs, iOS archives and cold compilation CPU.

## Android consumer rules

```proguard
-dontwarn java.awt.**
-keep class com.sun.jna.* { *; }
-keepattributes RuntimeVisibleAnnotations
-keep class com.noma4i.dblayer.** extends com.sun.jna.Structure {
    public <fields>;
    public <init>(...);
}
-keep class com.noma4i.dblayer.** implements com.sun.jna.Callback {
    public <methods>;
}
-keepclassmembers class com.noma4i.dblayer.** {
    native <methods>;
}
```

Ordinary records, enums and errors use generated converters. Their names are free to change.
The generated native-declaring classes use `Native.register`, which receives `Class` objects. Only their native method names require preservation.

The Android check builds a release application with minification enabled and the actual generated bindings.
An R8-optimized JVM probe verifies native calls, structure layouts, callbacks, records, an enum and an error against the host library.
The old broad rule fails the `Config` obfuscation check. The narrow rule passes it.
Separate mutations removing fields, constructors, callbacks and native method names fail. Restored rules pass.
The minimal application's DEX decreases from 654640 to 371240 bytes. This does not measure another application's DEX.
See [Android R8 evidence](android-r8.md) for the red/green results and reproduction commands.

## Build checks

The complete command passed all 21 selected steps:

```sh
LC_ALL=C PATH=/bin:/usr/bin:$PATH ~/.claude/bin/build-slot -- bash scripts/check_dblayer.sh
```

| Check | Result |
| --- | --- |
| Rust release core and bridge | 1315 + 3 passed, zero failed or ignored |
| Default Rust core and generator | Passed |
| Swift tests | 247 passed |
| iOS Simulator compile | Passed |
| Kotlin JVM tests | 193 passed, zero failed or skipped |
| Android release minification and optimized JVM FFI | Passed |
| Android arm64-v8a and armeabi-v7a artifacts | Built and source hash matched |
| iOS device release archive | Built and Swift bindings generated from it |
| Coverage, invariants, build modes and docs | Passed |

The source hash now includes the workspace profile. Its regression check failed before that change and passes afterward.
Android bindgen reads the static archive because symbol-stripped shared libraries no longer expose metadata to UniFFI's ELF reader.
The resulting Kotlin file has the same SHA256 as the host-generated file used by the tests.
Android device execution was not performed. The checked Android artifacts, DEX mappings and JVM FFI results are the available evidence.
