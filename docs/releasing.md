# Releasing

The owner chooses each version and authorizes publication. Use a minor version for a new feature or API change and a patch version for a fix. Only the owner can choose a major version.

## Prepare

1. Replace `## Unreleased` in `CHANGELOG.md` with `## X.Y.Z - YYYY-MM-DD`.
2. Keep only non-empty `### Added`, `### Changed`, `### Fixed`, and `### Removed` sections, in that order.
3. Write one sentence of at most 15 words per entry. Start with a verb or noun and end with a period.
4. Set the version in `core/Cargo.toml` and the `dblayer` entry in `Cargo.lock`.
5. Commit only those three files as `Release X.Y.Z` on `master`. Since the previous tag, exactly one commit must have that subject.

## Build and publish

1. Run `make release VERSION=X.Y.Z`. It checks the version, clean tree, `master` branch, previous tag, release commit, package version, changelog heading, documentation, invariants, and consistency. It builds Apple, Android, JVM, and generator outputs, packages them, and checks archive contents, architectures, Android ABIs, and SHA-256 sums. It publishes nothing.
2. The build can fail if any prerequisite check fails, a build tool or target is missing, output already exists, a package is malformed, an expected file or architecture is missing.
3. After the owner authorizes publication, run `make release-publish VERSION=X.Y.Z`. This creates and pushes the tag, pushes `master`, creates the GitHub Release, checks its assets, downloads them, and verifies their checksums.
4. A failure before the tag can be fixed and rerun. After the tag, fix forward with a new patch version. Never move a published tag.

## Release text

Use the version's `CHANGELOG.md` section without its heading. Follow it with a `## Compiled parts` block listing each file, its contents, its destination, the SwiftPM checksum for the Apple archive, and the checksum command. The script generates the release notes and checksum data.

## Attachments

| File | Contents | Consumer destination |
| --- | --- | --- |
| `DBlayerFFI.xcframework.zip` | Apple device, simulator, and macOS libraries | `swift/DBlayerFFI.xcframework` |
| `dblayer.swift` | Generated Swift bindings | swift/Sources/DBlayer/dblayer.swift |
| `dblayer-android.zip` | Four Android ABI libraries and Kotlin bindings | Extract into the Kotlin module, preserving archive paths |
| `dblayer-gen-macos-universal` | macOS arm64 and x86_64 model generator | Run to generate models |
| `libdblayer-macos-universal.dylib` | macOS arm64 and x86_64 JVM library | JVM native library path |
| `MANIFEST-X.Y.Z.txt` | Tag, commit, sizes, and hashes for attachments and archive contents | Keep as the release inventory |
| `SHA256SUMS` | Hashes for every attachment | Use to verify downloaded files |

## Verify publication

1. Inspect the release and asset names with `gh release view X.Y.Z --repo noma4i/dblayer --json assets,isLatest`.
2. Download into an empty directory with `gh release download X.Y.Z --repo noma4i/dblayer --dir <directory>`.
3. In that directory, check the downloaded files with `shasum -a 256 -c SHA256SUMS`.

`MANIFEST-X.Y.Z.txt` records the tag, commit, sizes, and hashes of every attachment and each file inside the archives. `SHA256SUMS` records attachment hashes.
