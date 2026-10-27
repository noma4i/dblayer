# Releasing

The owner chooses each version and authorizes publication. Use a minor version for a new feature or API change and a patch version for a fix. Only the owner can choose a major version.

## Steps

1. Replace `## Unreleased` in `CHANGELOG.md` with `## X.Y.Z - YYYY-MM-DD`. Keep only non-empty `### Added`, `### Changed`, `### Fixed`, `### Removed` sections, in that order, one sentence of at most 15 words per entry.
2. Set the version in `core/Cargo.toml` and the `dblayer` entry in `Cargo.lock`.
3. Commit those three files as `Release X.Y.Z` on `master`. Exactly one commit with that subject must exist since the previous tag. Commit any other pending work before publishing: the tree must be clean.
4. Run `make release VERSION=X.Y.Z`. It checks the version, changelog, documentation, invariants and consistency, builds Apple, Android, JVM and generator outputs, packages them into `target/release-X.Y.Z`, and checks architectures, Android ABIs and checksums. It publishes nothing. Remove `target/release-X.Y.Z` before a rerun.
5. After the owner agrees, run `make release-publish VERSION=X.Y.Z`. It tags the current commit, pushes `master` and the tag, creates the GitHub Release marked latest, then downloads the files and checks their checksums.

If a step fails before the tag, fix the cause and rerun. After the tag, publish a new patch version instead of moving the tag.

## Release page

The title is the version. The text is the `CHANGELOG.md` section of that version, followed by a `Compiled parts` list that says what each file is and where it goes, with the SwiftPM checksum of the Apple archive. The script writes it.

## Files on the release page

| File | Contents | Where it goes |
| --- | --- | --- |
| `DBlayerFFI.xcframework.zip` | Apple device, simulator and macOS libraries | Unzip into `swift/` |
| `dblayer.swift` | Generated Swift bindings | `swift/Sources/DBlayer/` |
| `dblayer-android.zip` | Four Android ABI libraries and Kotlin bindings | Unzip into `kotlin/` |
| `dblayer-gen-macos-universal` | Model generator for macOS arm64 and x86_64 | Run as `dblayer-gen` |
| `libdblayer-macos-universal.dylib` | JVM library for macOS arm64 and x86_64 | JVM native library path |

GitHub shows a SHA-256 digest for every file. Nothing else is attached.

## Check a publication

1. `gh release view X.Y.Z --repo noma4i/dblayer --json assets` lists the five files.
2. `git ls-remote --tags origin X.Y.Z` shows the tag.
