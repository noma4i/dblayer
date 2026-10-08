# Getting started

## Requirements

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

It runs, in order: Rust tests, the iOS framework build, Swift tests, an iOS simulator compile, Android libraries for 4 ABIs, Kotlin unit tests and the consistency check.

Single steps:

| Command | Does |
| ------- | ---- |
| `cargo test` | Rust core and generator tests |
| `bash scripts/build_ios.sh` | Builds `swift/DBlayerFFI.xcframework` and generates the Swift bindings |
| `bash scripts/build_kotlin.sh` | Builds `libdblayer.so` for 4 ABIs and generates the Kotlin bindings |
| `bash scripts/check_zoo.sh` | Finds duplicate mechanisms and names (see [architecture](architecture.md)) |
| `bash scripts/check_docs.sh` | Checks that paths mentioned in README and docs exist |

## Typical app setup

1. Write your models in `.dbl` files ([models](models.md)).
2. Run `dblayer-gen` in your build to produce typed models and the model specification.
3. Create a transport with your GraphQL endpoint and an auth header.
4. Open the database with the models and the transport.
5. Register your GraphQL operations.
6. Use the generated model API: every read is live ([live data](live-data.md)).

## Where to go next

- [Describing models](models.md)
- [Live data and queries](live-data.md)
- [Writing data](writing-data.md)
- [Server sync](server-sync.md)
