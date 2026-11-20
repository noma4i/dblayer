# Releasing

The owner chooses each version and authorizes publication. Use a minor version for a new feature or API change and a patch version for a fix. Only the owner can choose a major version.

## Steps

1. Replace `## Unreleased` in `CHANGELOG.md` with `## X.Y.Z - YYYY-MM-DD`. Keep only non-empty `### Added`, `### Changed`, `### Fixed`, `### Removed` sections, in that order, one sentence of at most 15 words per entry.
2. Set the version in `core/Cargo.toml` and the `dblayer` entry in `Cargo.lock`.
3. Commit those three files as `Release X.Y.Z` on `master`. Exactly one commit with that subject must exist since the previous tag. Keep the tree clean. The root `Package.swift` must equal the manifest of the previous tag, or the release script refuses.
4. Run `make release VERSION=X.Y.Z` for a dry run. It builds the four release files, stages Maven artifacts, runs the sequence suites (and, when the release needs them, the app suites), writes `RECEIPT.json` into the stage, prints the generated `Package.swift`, and checks that manifest in a temporary copy. The stage `target/release-X.Y.Z` belongs to one source commit (`SOURCE` inside it): a run from another commit moves the old stage aside and builds the files again.
5. After the owner agrees, run `make release-publish VERSION=X.Y.Z`. It reuses successful checks with unchanged inputs and creates `Package release X.Y.Z` with a temporary Git index. The commit has `Release X.Y.Z` as its parent and contains the release manifest and generated binding.
6. The release script tags the packaging commit and checks the tagged `dblayer.swift`. It pushes the unchanged `Release X.Y.Z` commit as `master` and the packaging commit as the tag. It also puts the tag `release-X.Y.Z` on the `Release X.Y.Z` commit, so the history of `master` shows each release. It publishes the staged Maven files to the orphan `maven` branch, then creates the GitHub Release with the four release files.
7. After the GitHub Release is verified, the script commits `Point the package manifest at X.Y.Z` on `master` and pushes `master`. The commit sets the root `Package.swift` equal to the manifest of the packaging commit and changes nothing else. The generated binding stays untracked on `master`. A rerun after a partial publish reuses this commit and never creates a second one.

The root `Package.swift` on `master` is always the manifest of the last published release. Its binding is ignored, so a root build needs locally generated bindings. The tagged manifest pins that release's URL and the checksum of `DBlayerFFI.xcframework.zip`. The next packaging commit therefore differs from `master` in the URL and checksum lines only. Use `swift/Package.swift` and `scripts/build_ios.sh` for local development.

Android coordinates are `com.noma4i:dblayer:X.Y.Z`. The Maven branch contains `com/noma4i/dblayer/X.Y.Z/` at its root. Consumers use the raw branch URL and dependency shown in [Getting started](getting-started.md#use-a-release). Publication uses a local staging directory and does not need credentials.

The stage keeps checked Maven files under `maven/com/noma4i/dblayer/X.Y.Z/` for app checks before publication. The test fixtures must contain `DBlayerTestingApi` and `MockTransport`. Use these staged files instead of running another Gradle publication in the same checkout.

The packaging commit carries the staged `dblayer.swift`, and the script fails if the tag differs from the asset.

The release script checks for exactly one `Release X.Y.Z` commit since the previous tag and requires it at HEAD, or directly below the `Point the package manifest at X.Y.Z` commit of a rerun. The `Release X.Y.Z` commit must carry the same `Package.swift` as the previous tag. The packaging commit is outside `master` and is referenced by the release tag. Its SHA is saved in the local stage so a rerun after removing the tag reuses the same commit. A rerun checks the commit and the existing Maven version against the staged files.

## Checks before the tag

The script refuses the tag, in a dry run and in a publish run, when one of these checks is red or missing.

1. The selftest sequence manifest (`manifest.tsv` in the FFI selftest directory of the core) must exist. It lists the sequence tests of the three lanes. A missing manifest stops the release before any build.
2. `scripts/check_sequences.sh <stage>` runs the listed tests in the core, Swift and Kotlin lanes. It first compares the staged files with the built tree, so the tests run on the bytes that ship. Every listed test must run and pass, and a lane with no listed test is red. Logs and `sequences.tsv` go to the stage. Limits: `SEQ_CORE_LIMIT`, `SEQ_SWIFT_LIMIT`, `SEQ_KOTLIN_LIMIT` (seconds, default 900 each).
   The core lane runs each exact test name through Corgi. Each run must report exactly 1 passed test.

3. `scripts/check_consumers.sh` runs the selected iOS and Android consumer suites in parallel on a local package built from the stage. The script runs when changed files match the release script's consumer check. `RELEASE_CONSUMERS=always` forces it. `CONSUMER_LANES=none` skips it only when the owner waives the app suites for one release; the receipt then records `consumers` as `waived-by-owner`.
4. Each required lane must have a complete successful result. Sequence counts must match the manifest and stay above zero. The script reuses a result only when its source inputs, artifacts and declared external inputs match. Changed inputs run only the affected lanes. Missing or malformed reports from a new check prevent the tag. A damaged receipt prevents publication without repeating checks.

`release.sh` records the source commit and a run id before the build and exports them as `RELEASE_SOURCE_SHA` and `RELEASE_RUN_ID`. New report rows must match that source and run. Reused results retain their original source and run in `lane_origin`. The receipt records fingerprints in `lane_inputs`. Before publication, HEAD and the checked inputs must remain unchanged. A failed check leaves the previous successful receipt intact.
The package commit must have the source SHA as its only parent and change exactly `Package.swift` and `swift/Sources/DBlayer/dblayer.swift`. The temporary index leaves the working index unchanged. The script checks both files against the stage before tagging, including when it reuses a commit.

Single run rule: one release run per checkout. `release.sh` holds the lock directory `target/release.lock` for the whole run. A second run refuses at once and names the holder. The lock is released on exit and on the signals HUP, INT and TERM. An existing lock always refuses the run, whether its holder is alive or dead. The message names the lock path, the holder pid and whether it is alive. Remove a lock of a dead holder by hand (`rm -rf target/release.lock`) after you check that no release run is alive.

5. `RECEIPT.json` in the stage records the source commit, the manifest hash, the hash of each release file, the sequence results and the app suite results.

`check_consumers.sh` makes a fresh worktree of the consumer repository from the ref of each lane (default `master`; a missing ref fails with its name) and runs the lane command in it. The commands live in the consumer repository. The script never reads or writes shared library caches and removes its trees when it ends.

Required environment: `CONSUMER_REPO` (the consumer checkout), `CONSUMER_IOS_COMMAND` (iOS lane) and `CONSUMER_ANDROID_COMMAND` (Android lane). Release checks also require `CONSUMER_INPUTS_IOS` and `CONSUMER_INPUTS_ANDROID`: colon-separated paths to the external files each lane consumes. The script names missing inputs in its error. `CONSUMER_LANES` (default `ios android`) names the app lanes a release requires; the receipt records them under `consumer_lanes_required`. Optional: `CONSUMER_IOS_REF`, `CONSUMER_ANDROID_REF`, `CONSUMER_WORK_ROOT`, `BUILD_SLOT`, `CONSUMER_LANE_LIMIT` (seconds per lane, default 300). A lane past the limit is killed, exits 124 and its log ends with `TIMEOUT <limit> lane <name>`.

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
