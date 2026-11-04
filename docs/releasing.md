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
4. Before the receipt and the tag, in the dry run too, `release.sh` requires fresh, non-empty `sequences.tsv` and `consumers.tsv` from this run. The sequence lanes are exactly `core`, `swift` and `kotlin`, each with rc 0, listed above 0 and equal to the manifest count of that lane, and passed equal to listed. When the app suites are required, the consumer lanes are exactly `ios` and `android`, each with rc 0. A missing file, a missing or extra lane or an incomplete result stops the release with no receipt and no tag. A failed write of either file fails the script that writes it.

`release.sh` records the source commit and a run id before the build and exports them as `RELEASE_SOURCE_SHA` and `RELEASE_RUN_ID`. Every row of `sequences.tsv` and `consumers.tsv` ends with that SHA and run id. Before the receipt, the package commit and the tag, `release.sh` refuses the release when HEAD differs from the recorded SHA (the Package release commit directly on top of it is the only exception), when the tree is dirty, or when any report row has another SHA or run id.
Package commit rule: before its own package commit, `release.sh` requires HEAD to equal the recorded source SHA exactly. It records the SHA of the `Package release X.Y.Z` commit it creates, checks that the parent of that commit is the source SHA and that its diff touches only `Package.swift` and `swift/Sources/DBlayer/dblayer.swift`, and tags that recorded SHA. A rerun that starts on an existing package commit checks the same composition at start and refuses an extra changed file, in a dry run too.

Single run rule: one release run per checkout. `release.sh` holds the lock directory `target/release.lock` for the whole run. A second run refuses at once and names the holder. The lock is released on exit and on the signals HUP, INT and TERM. A lock whose holder process is gone is replaced.

5. `RECEIPT.json` in the stage records the source commit, the manifest hash, the hash of each release file, the sequence results and the app suite results.

`check_consumers.sh` makes a fresh worktree of the app repository from the app ref of each lane (default `master`; a missing ref fails with its name) and uses its own library cache. It never reads or writes the shared library caches. It removes the trees and caches when it ends. Required environment:

The script lists every required variable in its header and names every missing one in its error. They set the app repository checkout, the simulator name for `sim-run` (iOS lane), and the Android build host with its absolute library root (Android lane).

Optional settings: the app ref of each lane, the library cache path of the Android lane, the read-only source of the MediaLayer entries, the work root, `SIM_RUN`, `BUILD_SLOT`, the iOS build and test limits and the Gradle timeout of the Android lane.

The iOS lane generates the app project against the local package, builds the logic test scheme, then runs the whole bundle. Only exit code 0 of the app unit test script is a pass. The Android lane resolves the staged Maven files from its own cache, runs the debug unit tests and the native sandbox unit tests on the build host, and requires `rc=0`, executed JVM tests, no failures, no cached result and an executed native sandbox task. `scripts/check_release_modes.sh` tests the refusals with stub commands.

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
