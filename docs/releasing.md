# Releasing

The owner chooses each version and authorizes publication. Use a minor version for a new feature or API change and a patch version for a fix. Only the owner can choose a major version.

## Steps

1. Replace `## Unreleased` in `CHANGELOG.md` with `## X.Y.Z - YYYY-MM-DD`. Keep only non-empty `### Added`, `### Changed`, `### Fixed`, `### Removed` sections, in that order, one sentence of at most 15 words per entry.
2. Set the version in `core/Cargo.toml` and the `dblayer` entry in `Cargo.lock`.
3. Commit those three files as `Release X.Y.Z` on `master`. Exactly one commit with that subject must exist since the previous tag. Keep the tree clean.
4. Run `make release VERSION=X.Y.Z` for a dry run. It builds the four release files, stages Maven artifacts, prints the generated `Package.swift`, and checks that manifest in a temporary copy.
5. After the owner agrees, run `make release-publish VERSION=X.Y.Z`. It writes `Package.swift`, adds the generated `dblayer.swift`, and commits both as `Package release X.Y.Z` after the `Release` commit.
6. The release script tags the packaging commit, pushes `master` and the tag, publishes the staged Maven files to the orphan `maven` branch, then creates the GitHub Release with the four release files.

The root `Package.swift` pins the tag URL and the checksum of `DBlayerFFI.xcframework.zip`. Keep `swift/Package.swift` and `scripts/build_ios.sh` for local development.

Android coordinates are `com.noma4i:dblayer:X.Y.Z`. The Maven branch contains `com/noma4i/dblayer/X.Y.Z/` at its root. Consumers use the raw branch URL and dependency shown in [Getting started](getting-started.md#use-a-release). Publication uses a local staging directory and does not need credentials.

The packaging commit carries the staged `dblayer.swift`, and the script fails if the tag differs from the asset.

The release script checks for exactly one `Release X.Y.Z` commit since the previous tag. The additional `Package release X.Y.Z` commit follows it. A rerun after removing the tag reuses the packaging commit and existing Maven version.

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
