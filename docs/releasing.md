# Releasing

The owner chooses each version and authorizes publication. Use a minor version for a new feature or API change and a patch version for a fix. Only the owner can choose a major version.

## Steps

1. Replace `## Unreleased` in `CHANGELOG.md` with `## X.Y.Z - YYYY-MM-DD`. Keep only non-empty `### Added`, `### Changed`, `### Fixed`, `### Removed` sections, in that order, one sentence of at most 15 words per entry.
2. Set the version in `core/Cargo.toml` and the `dblayer` entry in `Cargo.lock`.
3. Commit those three files as `Release X.Y.Z` on `master`. Exactly one commit with that subject must exist since the previous tag. Keep the tree clean.
4. Run `make release VERSION=X.Y.Z` for a dry run. It builds the four release files, stages Maven artifacts, runs the sequence suites (and, when the release needs them, the app suites), writes `RECEIPT.json` into the stage, prints the generated `Package.swift`, and checks that manifest in a temporary copy.
5. After the owner agrees, run `make release-publish VERSION=X.Y.Z`. It runs the same checks, writes `Package.swift`, adds the generated `dblayer.swift`, and commits both as `Package release X.Y.Z` after the `Release` commit.
6. The release script tags the packaging commit, pushes `master` and the tag, publishes the staged Maven files to the orphan `maven` branch, then creates the GitHub Release with the four release files.

The root `Package.swift` pins the tag URL and the checksum of `DBlayerFFI.xcframework.zip`. Keep `swift/Package.swift` and `scripts/build_ios.sh` for local development.

Android coordinates are `com.noma4i:dblayer:X.Y.Z`. The Maven branch contains `com/noma4i/dblayer/X.Y.Z/` at its root. Consumers use the raw branch URL and dependency shown in [Getting started](getting-started.md#use-a-release). Publication uses a local staging directory and does not need credentials.

The packaging commit carries the staged `dblayer.swift`, and the script fails if the tag differs from the asset.

The release script checks for exactly one `Release X.Y.Z` commit since the previous tag. The additional `Package release X.Y.Z` commit follows it. A rerun after removing the tag reuses the packaging commit and existing Maven version.

## Checks before the tag

The script refuses the tag, in a dry run and in a publish run, when one of these checks is red or missing.

1. The selftest sequence manifest (`manifest.tsv` in the FFI selftest directory of the core) must exist. It lists the sequence tests of the three lanes. A missing manifest stops the release before any build.
2. `scripts/check_sequences.sh <stage>` runs the listed tests in the core, Swift and Kotlin lanes. It first compares the staged files with the built tree, so the tests run on the bytes that ship. Every listed test must run and pass, and a lane with no listed test is red. Logs and `sequences.tsv` go to the stage. Limits: `SEQ_CORE_LIMIT`, `SEQ_SWIFT_LIMIT`, `SEQ_KOTLIN_LIMIT` (seconds, default 900 each).
3. `scripts/check_consumers.sh` runs the iOS and Android app suites, in parallel, on a local package built from the stage. The script runs when the files changed since the previous tag touch `core/src/{lifecycle,ffi,session,sync,store}/`, `gen/`, `swift/`, `kotlin/`, the root `Cargo.toml` or the build and check scripts of the release. `RELEASE_CONSUMERS=always` forces it. No setting skips it.
4. `RECEIPT.json` in the stage records the source commit, the manifest hash, the hash of each release file, the sequence results and the app suite results.

`check_consumers.sh` makes a fresh worktree of the app repository from `CONSUMER_IOS_REF` (iOS lane) and `CONSUMER_ANDROID_REF` (Android lane) for each lane, both default `master` and uses its own library cache. It never reads or writes the shared library caches. It removes the trees and caches when it ends. Required environment:

| Variable | Meaning |
| --- | --- |
| `YUPI_V3_REPO` | App repository checkout |
| `CONSUMER_IOS_SIM` | Simulator name for `sim-run` (iOS lane) |
| `BRICK_HOST`, `BRICK_REMOTE_ROOT` | Android build host and its absolute library root (Android lane) |

Optional: `CONSUMER_IOS_REF`, `CONSUMER_ANDROID_REF` (app branch or ref each lane tests, default `master`; a missing ref fails with its name), `BRICK_LIBRARIES_CACHE` (default `.cache/yupi-v3/libraries-dblayer-check`), `CONSUMER_MEDIA_CACHE` (read-only source of the MediaLayer entries), `CONSUMER_WORK_ROOT`, `SIM_RUN`, `BUILD_SLOT`, `CONSUMER_IOS_BUILD_LIMIT`, `CONSUMER_IOS_TEST_LIMIT`, `BRICK_GRADLE_TIMEOUT`. An unset required variable is an error that lists every missing name.

The iOS lane generates the app project with `YUPI_DBLAYER_PATH` set to the local package, builds the `YupiLogicTests` scheme, then runs the whole bundle. Only exit code 0 of `ios/scripts/run-unit-tests.sh` is a pass. The Android lane resolves the staged Maven files from its own cache, runs `:app:testDebugUnitTest` and `:app:testDebugNativeSandboxUnitTest` on the build host through `android/scripts/brick.sh`, and requires `rc=0`, executed JVM tests, no failures, no cached result and an executed native sandbox task. `scripts/check_release_modes.sh` tests the refusals with stub commands.

## Release page

The title is the version. The text is the `CHANGELOG.md` section of that version and nothing else. The script writes it.

## Release files

| File | Contents | Where it goes |
| --- | --- | --- |
| `DBlayerFFI.xcframework.zip` | Apple device, simulator and macOS libraries | SwiftPM downloads it |
| `dblayer.swift` | Generated Swift bindings | Committed for the tagged release |
| `dblayer-android.zip` | Four Android ABI libraries and Kotlin bindings | Unzip into `kotlin/` |
| `dblayer-gen-macos-universal` | Model generator for macOS arm64 and x86_64 | Run as `dblayer-gen` |

Only these four files are release attachments.
