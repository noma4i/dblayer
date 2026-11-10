# Releasing

The owner chooses each version and authorizes publication. Use a minor version for a new feature or API change and a patch version for a fix. Only the owner can choose a major version.

## Steps

1. Replace `## Unreleased` in `CHANGELOG.md` with `## X.Y.Z - YYYY-MM-DD`. Keep only non-empty `### Added`, `### Changed`, `### Fixed`, `### Removed` sections, in that order, one sentence of at most 15 words per entry.
2. Set the version in `core/Cargo.toml` and the `dblayer` entry in `Cargo.lock`.
3. Commit those three files as `Release X.Y.Z` on `master`. Exactly one commit with that subject must exist since the previous tag. Keep the tree clean.
4. Run `make release VERSION=X.Y.Z` for a dry run. It builds the four release files, stages Maven artifacts, runs the sequence suites (and, when the release needs them, the app suites), writes `RECEIPT.json` into the stage, prints the generated `Package.swift`, and checks that manifest in a temporary copy. The stage `target/release-X.Y.Z` belongs to one source commit (`SOURCE` inside it): a run from another commit moves the old stage aside and builds the files again.
5. After the owner agrees, run `make release-publish VERSION=X.Y.Z`. It runs the same checks and creates `Package release X.Y.Z` with a temporary Git index. The commit has `Release X.Y.Z` as its parent and contains the release manifest and generated binding.
6. The release script tags the packaging commit and checks the tagged `dblayer.swift`. It pushes the unchanged `Release X.Y.Z` commit as `master` and the packaging commit as the tag. It publishes the staged Maven files to the orphan `maven` branch, then creates the GitHub Release with the four release files.

The root `Package.swift` on `master` retains the manifest from the last release made on the branch. Its binding is ignored, so a root build needs locally generated bindings. The tagged manifest pins that release's URL and the checksum of `DBlayerFFI.xcframework.zip`. Use `swift/Package.swift` and `scripts/build_ios.sh` for local development.

Android coordinates are `com.noma4i:dblayer:X.Y.Z`. The Maven branch contains `com/noma4i/dblayer/X.Y.Z/` at its root. Consumers use the raw branch URL and dependency shown in [Getting started](getting-started.md#use-a-release). Publication uses a local staging directory and does not need credentials.

The packaging commit carries the staged `dblayer.swift`, and the script fails if the tag differs from the asset.

The release script checks for exactly one `Release X.Y.Z` commit since the previous tag and requires it at HEAD. The packaging commit is outside `master` and is referenced by the release tag. Its SHA is saved in the local stage so a rerun after removing the tag reuses the same commit. A rerun checks the commit and the existing Maven version against the staged files.

## Checks before the tag

The script refuses the tag, in a dry run and in a publish run, when one of these checks is red or missing.

1. The selftest sequence manifest (`manifest.tsv` in the FFI selftest directory of the core) must exist. It lists the sequence tests of the three lanes. A missing manifest stops the release before any build.
2. `scripts/check_sequences.sh <stage>` runs the listed tests in the core, Swift and Kotlin lanes. It first compares the staged files with the built tree, so the tests run on the bytes that ship. Every listed test must run and pass, and a lane with no listed test is red. Logs and `sequences.tsv` go to the stage. Limits: `SEQ_CORE_LIMIT`, `SEQ_SWIFT_LIMIT`, `SEQ_KOTLIN_LIMIT` (seconds, default 900 each).
3. `scripts/check_consumers.sh` runs the selected iOS and Android consumer suites in parallel on a local package built from the stage. The script runs when changed files match the release script's consumer check. `RELEASE_CONSUMERS=always` forces it. `CONSUMER_LANES=none` skips it only when the owner waives the app suites for one release; the receipt then records `consumers` as `waived-by-owner`.
4. Before the receipt and the tag, in the dry run too, `release.sh` requires a fresh, non-empty `sequences.tsv` from this run. The sequence lanes are exactly `core`, `swift` and `kotlin`, each with rc 0, listed above 0 and equal to the manifest count of that lane, and passed equal to listed. When app suites are required, `consumers.tsv` must contain exactly the lanes selected by `CONSUMER_LANES`, each with rc 0. A missing required file, a missing or extra lane or an incomplete result stops the release with no receipt and no tag. A failed write of either file fails the script that writes it.

`release.sh` records the source commit and a run id before the build and exports them as `RELEASE_SOURCE_SHA` and `RELEASE_RUN_ID`. Every row of `sequences.tsv` and `consumers.tsv` ends with that SHA and run id. Before the receipt, the package commit and the tag, `release.sh` refuses the release when HEAD differs from the recorded SHA, when the tree is dirty, or when any report row has another SHA or run id.
The package commit must have the source SHA as its only parent and change exactly `Package.swift` and `swift/Sources/DBlayer/dblayer.swift`. The temporary index leaves the working index unchanged. The script checks both files against the stage before tagging, including when it reuses a commit.

Single run rule: one release run per checkout. `release.sh` holds the lock directory `target/release.lock` for the whole run. A second run refuses at once and names the holder. The lock is released on exit and on the signals HUP, INT and TERM. An existing lock always refuses the run, whether its holder is alive or dead. The message names the lock path, the holder pid and whether it is alive. Remove a lock of a dead holder by hand (`rm -rf target/release.lock`) after you check that no release run is alive.

5. `RECEIPT.json` in the stage records the source commit, the manifest hash, the hash of each release file, the sequence results and the app suite results.

`check_consumers.sh` makes a fresh worktree of the consumer repository from the ref of each lane (default `master`; a missing ref fails with its name) and runs the lane command in it. The commands live in the consumer repository. The script never reads or writes shared library caches and removes its trees when it ends.

Required environment: `CONSUMER_REPO` (the consumer checkout), `CONSUMER_IOS_COMMAND` (iOS lane) and `CONSUMER_ANDROID_COMMAND` (Android lane). The script names every missing variable in its error. `CONSUMER_LANES` (default `ios android`) names the app lanes a release requires; the receipt records the required lanes under `consumer_lanes_required`, so a release checked on one lane says so. Optional: `CONSUMER_IOS_REF`, `CONSUMER_ANDROID_REF`, `CONSUMER_WORK_ROOT`, `BUILD_SLOT`, `CONSUMER_LANE_LIMIT` (seconds per lane, default 300). A lane past the limit is killed, exits 124 and its log ends with `TIMEOUT <limit> lane <name>`.

A lane command receives `DBLAYER_PACKAGE` (local Swift package), `DBLAYER_MAVEN` (staged Maven directory), `DBLAYER_GEN` (staged generator), `DBLAYER_VERSION` and `DBLAYER_STAGE`. Only exit code 0 is a pass. `scripts/check_release_modes.sh` tests the refusals with stub commands.

## Release page

The title is the version. The text is the `CHANGELOG.md` section of that version and nothing else. The script writes it.

## Release files

| File                          | Contents                                       | Where it goes                    |
| ----------------------------- | ---------------------------------------------- | -------------------------------- |
| `DBlayerFFI.xcframework.zip`  | Apple device, simulator and macOS libraries    | SwiftPM downloads it             |
| `dblayer.swift`               | Generated Swift bindings                       | Committed for the tagged release |
| `dblayer-android.zip`         | Four Android ABI libraries and Kotlin bindings | Unzip into `kotlin/`             |
| `dblayer-gen-macos-universal` | Model generator for macOS arm64 and x86_64     | Run as `dblayer-gen`             |

Only these four files are release attachments.
