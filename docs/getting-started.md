# Getting started

## Requirements

| Tool | Why |
| ---- | --- |
| Xcode (iOS 17+, macOS 14+) | Swift package and iOS simulator build |
| Homebrew `rustup` with targets `aarch64-apple-ios`, `aarch64-apple-ios-sim`, `x86_64-apple-ios`, `aarch64-apple-darwin`, `aarch64-linux-android`, `x86_64-linux-android`, `armv7-linux-androideabi`, `i686-linux-android` | Rust core for every platform |
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

It runs the cheap checks first (generated models, duplicate mechanisms, docs, invariants and their self test) and stops there on a failure. Then 4 lanes run one after another (never 2 build slot commands at once, the first red lane stops the run): Rust tests, Swift tests on the macOS and arm64 simulator slices with an iOS simulator compile, Kotlin unit tests, and the ship lane (Android libraries for 4 ABIs, compile check of the iOS device and x86_64 simulator targets). Every step has a time limit and its process group is killed past it.

`specs/` is not in git: the invariants check reads `specs/invariants.md` of the main tree, or the file named by `SPEC=<file>`. The summary line counts CPU seconds of waited children plus the CPU of the processes below a java process (the single-use Gradle daemon and its test executors).

`bash scripts/check_dblayer.sh --fast` runs the cheap checks and only the steps of the touched directories (`core/`, `gen/`, `swift/`, `kotlin/`). `--list` prints the steps, `--only <lane>` runs one lane. Run the full check before a merge.

Single steps:

| Command | Does |
| ------- | ---- |
| `cargo test` | Rust core and generator tests |
| `bash scripts/build_ios.sh` | Builds `swift/DBlayerFFI.xcframework` (the simulator slice holds arm64 and x86_64) and generates the Swift bindings |
| `bash scripts/build_kotlin.sh` | Builds `libdblayer.so` for 4 ABIs and generates the Kotlin bindings |
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

## Where to go next

- [Describing models](models.md)
- [Live data and queries](live-data.md)
- [Writing data](writing-data.md)
- [Server sync](server-sync.md)
