# Getting started

## Requirements

Android 13 (API 33) or newer.

| Tool | Why |
| ---- | --- |
| Xcode (iOS 17+, macOS 14+) | Swift package and iOS simulator build |
| Homebrew `rustup` with targets `aarch64-apple-ios`, `aarch64-apple-ios-sim`, `aarch64-apple-darwin`, `aarch64-linux-android`, `x86_64-linux-android`, `armv7-linux-androideabi`, `i686-linux-android` | Rust core for every platform |
| `cargo-ndk` and Android NDK | Android libraries |
| JDK 17 and Gradle 9 | Kotlin module |

Use the `rustup` toolchain, not a plain `cargo` from Homebrew, because only `rustup` has the iOS and Android targets:

```sh
export PATH=/opt/homebrew/opt/rustup/bin:$PATH
```

## Build and check everything

```sh
bash scripts/check_dblayer.sh
```

It runs cheap checks first, including build modes, and stops on failure. Build commands run sequentially: Rust, Swift, Kotlin, then Android release libraries and an iOS device compile check. Android release builds contain arm64-v8a and armeabi-v7a. Swift tests use macOS and arm64 simulator slices. Every step has a time limit.

The workspace release profile uses optimization level 3, no LTO, one codegen unit, symbol stripping and panic unwinding.
It applies to Android and iOS builds. Panic unwinding remains enabled for UniFFI.
Android binding generation reads the static archive, which retains UniFFI metadata after the shared library loses its symbol table.
The Android source hash includes the workspace manifest, so changing the release profile invalidates older native artifacts.
See [release profile measurements](release-profile-measurements.md) for the size, query CPU and compilation CPU comparisons.

`specs/` is not in git: the invariants check reads `specs/invariants.md` of the main tree, or the file named by `SPEC=<file>`. The summary line counts CPU seconds of waited children plus the CPU of the processes below a java process (the single-use Gradle daemon and its test executors).

`bash scripts/check_dblayer.sh --fast` runs the cheap checks and only the steps of the touched directories (`core/`, `gen/`, `swift/`, `kotlin/`). `--list` prints the steps, `--only <lane>` runs one lane. Run the full check before a merge.

Single steps:

| Command | Does |
| ------- | ---- |
| `cargo test` | Rust core and generator tests |
| `bash scripts/build_ios.sh` | Builds macOS, iOS device and arm64 simulator slices, and generates Swift bindings. No x86_64 simulator slice |
| `bash scripts/build_ios.sh --device` | CI: builds only the iOS device release slice and Swift bindings |
| `bash scripts/build_ios.sh --check` | Builds macOS and arm64 simulator release slices for checks |
| `bash scripts/build_kotlin.sh --release` | CI: builds arm64-v8a and armeabi-v7a release libraries and Kotlin bindings. This is also the default |
| `bash scripts/build_kotlin.sh --abis` | Local development: explicitly builds all 4 Android ABIs and Kotlin bindings |
| `bash scripts/build_kotlin.sh --host` | Builds the host debug library and Kotlin bindings for JNA unit tests |
| `bash scripts/check_android_artifacts.sh --release` | Checks release ABI files and source hash. Use `--abis` to check all 4 ABIs |
| `bash scripts/check_zoo.sh` | Finds duplicate mechanisms and names (see [architecture](architecture.md)) |
| `bash scripts/check_docs.sh` | Checks that paths mentioned in README and docs exist |
| `bash scripts/reclaim_target.sh` | Removes the build output of `dblayer` and `dblayer-gen` from `target` and prints its size before and after. Refuses while `cargo` runs |

## Typical app setup

1. Write your models in `.dbl` files ([models](models.md)).
2. Run `dblayer-gen` in your build to produce typed models and the model specification.
3. Create a transport with your GraphQL endpoint and an auth header.
4. Open the database with the models and the transport.
5. Register your GraphQL operations. Every mutation a model action names must be registered, or the record fails with `operation <name> is not registered` ([sync state](sync-state.md)).
6. Use the generated model API: every read is live ([live data](live-data.md)).

## Schema changes

The open compares the database file with the build. A changed model clears the rows of that model, which the server sends again through sync. A file that does not fit the build is deleted and created empty: not a database, damaged, a newer or older format version, or a changed format of the internal tables. The files beside it (`.files`, `.cache`, `-wal`, `-shm`) go with it. Unsent writes in a discarded file are lost. Keep the session of the signed-in user outside the database file. `openReport` names what the open discarded, so the app can log it.

```swift
let report = DB.openReport()
if let reason = report.recreated { log("database recreated: \(reason)") }
if !report.resetModels.isEmpty { log("models cleared: \(report.resetModels)") }
```

```kotlin
val report = DB.openReport()
report.recreated?.let { log("database recreated: $it") }
if (report.resetModels.isNotEmpty()) log("models cleared: ${report.resetModels}")
```

## Where to go next

- [Describing models](models.md)
- [Live data and queries](live-data.md)
- [Writing data](writing-data.md)
- [Server sync](server-sync.md)
