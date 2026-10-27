.PHONY: build-ios build-kotlin build-jvm build-gen check check-docs check-invariants check-zoo release release-publish

build-ios:
	bash scripts/build_ios.sh
build-kotlin:
	bash scripts/build_kotlin.sh --abis
build-jvm:
	bash scripts/build_kotlin.sh --jvm
build-gen:
	cargo build --manifest-path Cargo.toml -p dblayer-gen
check:
	bash scripts/check_dblayer.sh
check-docs:
	bash scripts/check_docs.sh
check-invariants:
	bash scripts/check_invariants.sh
check-zoo:
	bash scripts/check_zoo.sh
release:
	bash scripts/release.sh "$(VERSION)"
release-publish:
	bash scripts/release.sh "$(VERSION)" --publish
