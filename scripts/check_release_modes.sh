#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python3 - "$root" <<'PY'
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

source = Path(sys.argv[1])
release_script = source / "scripts/release.sh"
version = "1.2.3"
attachments = ["DBlayerFFI.xcframework.zip", "dblayer.swift", "dblayer-android.zip", "dblayer-gen-macos-universal"]
real_git = shutil.which("git")


def run(command, cwd, env, expected=0, contains=None):
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True)
    output = result.stdout + result.stderr
    if result.returncode != expected:
        raise AssertionError(f"{command}: rc={result.returncode}, output={output}")
    if contains and contains not in output:
        raise AssertionError(f"{command}: expected {contains!r} in {output!r}")
    return result


def fixture(base, touch=None, manifest=True):
    repo = base / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy2(release_script, repo / "scripts/release.sh")
    for name in ("check_docs.sh", "check_zoo.sh"):
        (repo / "scripts" / name).write_text("#!/bin/sh\nexit 0\n")
    shutil.copy2(source / "scripts/check_legacy.sh", repo / "scripts/check_legacy.sh")
    (repo / "scripts/check_sequences.sh").write_text("""#!/bin/sh
echo "check_sequences $*" >> "$TEST_EVENTS"
S="${STUB_SHA:-$RELEASE_SOURCE_SHA}"; R="${STUB_RUN:-$RELEASE_RUN_ID}"
case "${STUB_REPORT:-green}" in
  missing-file) ;;
  missing-lane) printf 'core\\t3\\t3\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nswift\\t2\\t2\\t0\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/sequences.tsv" ;;
  partial) printf 'core\\t3\\t3\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nswift\\t2\\t1\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nkotlin\\t2\\t2\\t0\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/sequences.tsv" ;;
  zero) printf 'core\\t3\\t3\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nswift\\t2\\t2\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nkotlin\\t0\\t0\\t0\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/sequences.tsv" ;;
  rc) printf 'core\\t3\\t3\\t1\\tlog\\t'"$S"'\\t'"$R"'\\nswift\\t2\\t2\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nkotlin\\t2\\t2\\t0\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/sequences.tsv" ;;
  *) printf 'core\\t3\\t3\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nswift\\t2\\t2\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nkotlin\\t2\\t2\\t0\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/sequences.tsv" ;;
esac
[ "${STUB_SEQUENCES:-green}" = green ] || { echo "sequences: lane core red" >&2; exit 1; }
""")
    (repo / "scripts/check_consumers.sh").write_text("""#!/bin/sh
echo "check_consumers $*" >> "$TEST_EVENTS"
S="${STUB_SHA:-$RELEASE_SOURCE_SHA}"; R="${STUB_RUN:-$RELEASE_RUN_ID}"
[ -z "${STUB_MOVE_HEAD:-}" ] || git commit -q --allow-empty -m "${STUB_MOVE_SUBJECT:-Moved during the run}"
case "${STUB_CONSUMER_REPORT:-green}" in
  missing-file) ;;
  missing-lane) printf 'ios\\t0\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/consumers.tsv" ;;
  rc) printf 'ios\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nandroid\\t1\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/consumers.tsv" ;;
  *) printf 'ios\\t0\\tlog\\t'"$S"'\\t'"$R"'\\nandroid\\t0\\tlog\\t'"$S"'\\t'"$R"'\\n' > "$1/consumers.tsv" ;;
esac
[ "${STUB_CONSUMERS:-green}" = green ] || { echo "consumers: lane ios red" >&2; exit 1; }
""")
    if manifest:
        (repo / "core/src/ffi/selftest").mkdir(parents=True)
        (repo / "core/src/ffi/selftest/manifest.tsv").write_text("version\t1\nid\taliases\tlane\ttest\tstate\ts1\tprovenance\n" + "".join(f"{n}\t\t{lane}\tT.{lane}{n}\tx\tx\tx\n" for lane, count in (("core", 3), ("swift", 2), ("kotlin", 2)) for n in range(count)))
    (repo / "scripts/build_ios.sh").write_text("""#!/bin/bash
mkdir -p swift/DBlayerFFI.xcframework/{macos-arm64_x86_64,ios-arm64_x86_64-simulator,ios-arm64} swift/Sources/DBlayer
printf generated-bindings > swift/Sources/DBlayer/dblayer.swift
printf info > swift/DBlayerFFI.xcframework/Info.plist
printf binary > swift/DBlayerFFI.xcframework/macos-arm64_x86_64/libdblayer.a
printf binary > swift/DBlayerFFI.xcframework/ios-arm64_x86_64-simulator/libdblayer.a
printf binary > swift/DBlayerFFI.xcframework/ios-arm64/libdblayer.a
""")
    (repo / "scripts/build_kotlin.sh").write_text("""#!/bin/bash
mkdir -p kotlin/src/main/jniLibs/{arm64-v8a,armeabi-v7a,x86_64,x86} kotlin/src/main/kotlin/com/noma4i/dblayer
for abi in arm64-v8a armeabi-v7a x86_64 x86; do printf so > "kotlin/src/main/jniLibs/$abi/libdblayer.so"; done
printf bindings > kotlin/src/main/kotlin/com/noma4i/dblayer/dblayer.kt
""")
    for path in (repo / "scripts/build_ios.sh", repo / "scripts/build_kotlin.sh"):
        path.chmod(0o755)
    (repo / "swift/Sources/DBlayer").mkdir(parents=True)
    (repo / "swift/Sources/DBlayerTesting").mkdir(parents=True)
    (repo / "swift/Sources/DBlayerTesting/Test.swift").write_text("public struct Test {}\n")
    (repo / "swift/Sources/DBlayer/dblayer.swift").write_text("bindings")
    (repo / "core").mkdir(exist_ok=True)
    (repo / "core/Cargo.toml").write_text('[package]\nname = "dblayer"\nversion = "1.2.2"\n')
    (repo / "Cargo.toml").write_text('[workspace]\nmembers = ["core"]\n')
    (repo / ".gitignore").write_text("target/\nkotlin/src/main/\nswift/DBlayerFFI.xcframework/\n")
    (repo / "CHANGELOG.md").write_text("## 1.2.2 - 2026-10-28\n\n## Unreleased\n")
    (repo / "Package.swift").write_text("old package\n")
    subprocess.run([real_git, "init", "-b", "master", str(repo)], check=True, capture_output=True)
    subprocess.run([real_git, "-C", str(repo), "config", "user.name", "Release Test"], check=True)
    subprocess.run([real_git, "-C", str(repo), "config", "user.email", "release@example.invalid"], check=True)
    subprocess.run([real_git, "-C", str(repo), "remote", "add", "origin", "https://example.invalid/dblayer.git"], check=True)
    subprocess.run([real_git, "-C", str(repo), "add", "."], check=True)
    subprocess.run([real_git, "-C", str(repo), "commit", "-m", "Source baseline"], check=True, capture_output=True)
    subprocess.run([real_git, "-C", str(repo), "tag", "1.2.2"], check=True)
    (repo / "core/Cargo.toml").write_text(f'[package]\nname = "dblayer"\nversion = "{version}"\n')
    (repo / "CHANGELOG.md").write_text(f"## {version} - 2026-10-29\n\n### Fixed\n- Release selftest.\n\n## Unreleased\n")
    if touch:
        (repo / touch).parent.mkdir(parents=True, exist_ok=True)
        (repo / touch).write_text("changed\n")
        subprocess.run([real_git, "-C", str(repo), "add", touch], check=True)
    subprocess.run([real_git, "-C", str(repo), "add", "core/Cargo.toml", "CHANGELOG.md"], check=True)
    subprocess.run([real_git, "-C", str(repo), "commit", "-m", f"Release {version}"], check=True, capture_output=True)
    bin_dir = base / "bin"
    bin_dir.mkdir()
    events = base / "events"
    stubs = {
        "git": '''#!/usr/bin/env python3
import os, subprocess, sys
a=sys.argv[1:]
with open(os.environ["TEST_EVENTS"],"a") as f: f.write("git " + " ".join(a) + "\\n")
command = a[2:] if a and a[0] == "-C" else a
if command and command[0] == "commit" and os.environ.get("STUB_COMMIT_EXTRA"):
    open(os.environ["STUB_COMMIT_EXTRA"], "w").write("extra\\n")
    subprocess.run([os.environ["REAL_GIT"], "add", "-f", os.environ["STUB_COMMIT_EXTRA"]], check=True)
if command and command[0] in ("clone", "fetch", "pull", "push"): sys.exit(0)
if command[:1] == ["tag"] and len(command) == 3 and os.environ.get("STUB_AFTER_TAG"):
    rc = subprocess.run([os.environ["REAL_GIT"], *a]).returncode
    subprocess.run([os.environ["REAL_GIT"], *(a[:2] if a[0] == "-C" else []), "commit", "-q", "--allow-empty", "-m", "Foreign commit after the tag"], check=True)
    sys.exit(rc)
if a[:2] == ["ls-remote", "--heads"]: sys.exit(0)
if a[:2] == ["remote", "get-url"]: print("https://example.invalid/dblayer.git"); sys.exit(0)
rc = subprocess.run([os.environ["REAL_GIT"], *a]).returncode
if rc == 0 and command and command[0] == "commit" and os.environ.get("STUB_REPARENT"):
    g = [os.environ["REAL_GIT"]] + (a[:2] if a[0] == "-C" else [])
    out = lambda *x: subprocess.check_output([*g, *x], text=True).strip()
    new = out("commit-tree", "HEAD^{tree}", "-p", out("rev-list", "--max-parents=0", "HEAD"), "-m", out("log", "-1", "--format=%s"))
    subprocess.run([*g, "update-ref", "HEAD", new], check=True)
sys.exit(rc)
''',
        "cargo": '''#!/usr/bin/env python3
import pathlib, sys
a=sys.argv[1:]
for i, arg in enumerate(a):
    if arg == "--target":
        p=pathlib.Path("target")/a[i+1]/"release/dblayer-gen"; p.parent.mkdir(parents=True,exist_ok=True); p.write_text("generator")
''',
        "lipo": '''#!/usr/bin/env python3
import pathlib, sys
a=sys.argv[1:]
if a[0] == "-create": pathlib.Path(a[a.index("-output")+1]).write_text("generator")
elif a[0] == "-info": print("Architectures in the fat file: arm64 x86_64")
elif a[0] == "-archs": print("arm64" if "/ios-arm64/" in a[-1] else "arm64 x86_64")
''',
        "gradle": '''#!/usr/bin/env python3
import io, pathlib, sys, zipfile
a=sys.argv[1:]; stage=pathlib.Path(next(x.split("=",1)[1] for x in a if x.startswith("-PstagingRepo=")))
d=stage/"com/noma4i/dblayer/1.2.3"; d.mkdir(parents=True,exist_ok=True)
(d/"dblayer-1.2.3.pom").write_text("pom"); (d/"dblayer-1.2.3.module").write_text("{}")
jar=io.BytesIO()
with zipfile.ZipFile(jar,"w") as classes: classes.writestr("com/noma4i/dblayer/Database.class","class")
with zipfile.ZipFile(d/"dblayer-1.2.3.aar","w") as archive:
 archive.writestr("classes.jar",jar.getvalue())
 for abi in ("arm64-v8a","armeabi-v7a","x86_64","x86"): archive.writestr(f"jni/{abi}/libdblayer.so","so")
''',
        "swift": '''#!/usr/bin/env python3
import hashlib, pathlib, sys
a=sys.argv[1:]
if a[:2] == ["package", "compute-checksum"]: print(hashlib.sha256(pathlib.Path(a[-1]).read_bytes()).hexdigest())
else: print("{}")
''',
        "xcodebuild": '''#!/bin/sh
exit 97
''',
        "gh": '''#!/usr/bin/env python3
import os, pathlib, shutil, sys
a=sys.argv[1:]; root=pathlib.Path(os.environ["TEST_ROOT"]); stage=root/"target/release-1.2.3"
with open(os.environ["TEST_EVENTS"],"a") as f: f.write("gh " + " ".join(a) + "\\n")
if a[:2] == ["release", "view"] and "--json" not in a: sys.exit(1)
if a[:2] == ["release", "view"]: print("\\n".join(["DBlayerFFI.xcframework.zip","dblayer.swift","dblayer-android.zip","dblayer-gen-macos-universal"]))
if a[:2] == ["release", "download"]:
 d=pathlib.Path(a[a.index("--dir")+1]); d.mkdir(exist_ok=True)
 for n in ("DBlayerFFI.xcframework.zip","dblayer.swift","dblayer-android.zip","dblayer-gen-macos-universal"): shutil.copy2(stage/n,d/n)
''',
    }
    for name, text in stubs.items():
        path = bin_dir / name
        path.write_text(text)
        path.chmod(0o755)
    bash_env = base / "bash_env"
    bash_env.write_text("cargo() { while [ \"$#\" -gt 0 ]; do if [ \"$1\" = --target ]; then mkdir -p \"target/$2/release\"; printf generator > \"target/$2/release/dblayer-gen\"; fi; shift; done; }\nxcodebuild() { return 97; }\nexport -f cargo xcodebuild\n")
    env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", BASH_ENV=str(bash_env), REAL_GIT=real_git, TEST_ROOT=str(repo), TEST_EVENTS=str(events), GRADLE=str(bin_dir / "gradle"))
    return repo, env


with tempfile.TemporaryDirectory(prefix="dblayer-release-modes-") as directory:
    base = Path(directory)
    repo, env = fixture(base)
    run(["bash", "scripts/release.sh", version], repo, env, contains="Package.swift to be written:")
    stage = repo / "target/release-1.2.3"
    assert sorted(path.name for path in stage.iterdir()) == sorted(attachments + ["SHA256SUMS", "NOTES.md", "RECEIPT.json", "sequences.tsv"])
    receipt = json.loads((stage / "RECEIPT.json").read_text())
    assert receipt["sequences"] == {"core": {"listed": 3, "passed": 3, "rc": "0"}, "swift": {"listed": 2, "passed": 2, "rc": "0"}, "kotlin": {"listed": 2, "passed": 2, "rc": "0"}}, receipt
    assert receipt["consumers"] == "not-required" and receipt["consumer_lanes"] == {}, receipt
    assert sorted(receipt["artifacts_sha256"]) == sorted(attachments), receipt
    assert len(receipt["manifest_sha256"]) == 64
    assert not any(line.startswith("check_consumers ") for line in (base / "events").read_text().splitlines()), "consumers ran without a boundary change"
    assert subprocess.check_output([real_git, "-C", str(repo), "log", "-1", "--format=%s"], text=True).strip() == f"Release {version}"
    dry_events = (base / "events").read_text().splitlines()
    assert not any(line.startswith("gh ") or line.startswith("git push ") for line in dry_events), dry_events
    print("ok dry run stages four release files without publishing")
    (stage / "dblayer.swift").write_bytes(b"staged binding")
    sums_file = stage / "SHA256SUMS"
    sums_file.write_text(subprocess.check_output(["shasum", "-a", "256", *attachments], cwd=stage, text=True))
    (repo / "swift/Sources/DBlayer/dblayer.swift").write_bytes(b"dirty binding")
    run(["bash", "scripts/release.sh", version, "--publish"], repo, env)
    staged_binding = (stage / "dblayer.swift").read_bytes()
    for revision in ("HEAD", version):
        assert subprocess.check_output([real_git, "-C", str(repo), "show", f"{revision}:swift/Sources/DBlayer/dblayer.swift"]) == staged_binding
    sums = [line.split("  ", 1)[1] for line in (stage / "SHA256SUMS").read_text().splitlines()]
    assert sorted(sums) == sorted(attachments), sums
    assert (stage / "NOTES.md").read_text() == "\n### Fixed\n- Release selftest.\n\n"
    assert "https://github.com/noma4i/dblayer/releases/download/1.2.3/DBlayerFFI.xcframework.zip" in (repo / "Package.swift").read_text()
    first_package = (repo / "Package.swift").read_bytes()
    run(["bash", "scripts/release.sh", version, "--publish"], repo, env)
    assert (repo / "Package.swift").read_bytes() == first_package
    subjects = subprocess.check_output([real_git, "-C", str(repo), "log", "--format=%s", "1.2.2..HEAD"], text=True)
    assert subjects.splitlines().count(f"Package release {version}") == 1
    assert subprocess.check_output([real_git, "-C", str(repo), "rev-parse", version + "^{commit}"], text=True).strip() == subprocess.check_output([real_git, "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    assert subprocess.check_output([real_git, "-C", str(repo), "cat-file", "-t", version], text=True).strip() == "commit"
    events = (base / "events").read_text().splitlines()
    assert any(line.startswith("git -C ") and line.endswith("push -u origin maven") for line in events), events
    assert any(line.startswith("gh release create 1.2.3 ") and all(name in line for name in attachments) for line in events), events
    assert any(line.startswith("gh release download 1.2.3 ") for line in events), events
    print("ok publish uses four checksummed files and repeat reuses the tagged package commit")
    print("ok release tag is a lightweight tag")
    package_head = subprocess.check_output([real_git, "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    subprocess.run([real_git, "-C", str(repo), "tag", "-d", version], check=True, capture_output=True)
    run(["bash", "scripts/release.sh", version, "--publish"], repo, env)
    assert subprocess.check_output([real_git, "-C", str(repo), "rev-parse", version + "^{commit}"], text=True).strip() == package_head
    assert subprocess.check_output([real_git, "-C", str(repo), "rev-parse", "HEAD"], text=True).strip() == package_head
    print("ok a rerun after the tag was deleted tags the same package commit")

    wrong, wrong_env = fixture(base / "wrong")
    subprocess.run([real_git, "-C", str(wrong), "commit", "--amend", "-m", "Release 9.9.9"], check=True, capture_output=True)
    run(["bash", "scripts/release.sh", "9.9.9"], wrong, wrong_env, expected=1, contains="core version does not match")
    print("ok wrong version is rejected")
    dirty, dirty_env = fixture(base / "dirty")
    (dirty / "Package.swift").write_text("dirty")
    run(["bash", "scripts/release.sh", version], dirty, dirty_env, expected=1, contains="working tree is not clean")
    assert not (dirty / "target/release.lock").exists(), "the lock stayed after a refusal"
    print("ok dirty tracked file is rejected and releases the lock")
    def amend_with(repo, name, text):
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(text)
        subprocess.run([real_git, "-C", str(repo), "add", name], check=True)
        subprocess.run([real_git, "-C", str(repo), "commit", "-q", "--amend", "--no-edit"], check=True, capture_output=True)

    for label, name, text, message in (
        ("a legacy line", "core/src/extra.rs", "// one legacy line\n", "1 legacy lines or files, 0 development-file references or files"),
        ("a legacy file name", "core/src/shim.rs", "pub fn a() {}\n", "1 legacy lines or files, 0 development-file references or files"),
        ("a development file", "CLAUDE.md", "notes\n", "0 legacy lines or files, 1 development-file references or files"),
        ("a plans reference", "core/src/p.rs", "// see .claude/plans/a.md\n", "0 legacy lines or files, 1 development-file references or files"),
        ("COMPAT", "core/src/c.rs", "// COMPAT\n", "1 legacy lines or files, 0 development-file references or files"),
        ("a spaced flush", "core/src/f.rs", "pub fn flush () {}\n", "1 legacy lines or files, 0 development-file references or files"),
        ("a sync root import", "core/src/s.rs", "use crate::sync;\n", "1 legacy lines or files, 0 development-file references or files"),
    ):
        repo_red, env_red = fixture(base / ("red-" + name.replace("/", "-")))
        amend_with(repo_red, name, text)
        run(["bash", "scripts/release.sh", version], repo_red, env_red, expected=1, contains=message)
        assert not (repo_red / "target/release-1.2.3").exists(), f"{label}: the stage was built after a red check_legacy"
        print(f"ok release refuses {label} before any build")
    compatible, compatible_env = fixture(base / "compatible")
    amend_with(compatible, "core/src/ok.rs", "// Compatible with the server\n")
    run(["bash", "scripts/release.sh", version], compatible, compatible_env, contains="check_legacy: ok")
    print("ok release continues on a clean tree and Compatible is allowed")
    broken, broken_env = fixture(base / "broken")
    run(["bash", "scripts/check_legacy.sh", "count", str(broken), "no-such-ref"], broken, broken_env, expected=2, contains="git grep failed")
    print("ok a git failure is not a zero count")
    incomplete, incomplete_env = fixture(base / "incomplete")
    stage = incomplete / "target/release-1.2.3"
    stage.mkdir(parents=True)
    (stage / "DBlayerFFI.xcframework.zip").write_text("partial")
    (stage / "SHA256SUMS").write_text("partial")
    run(["bash", "scripts/release.sh", version], incomplete, incomplete_env, expected=1, contains="incomplete existing output")
    print("ok incomplete stage is rejected")

    def assert_no_release(repo, env, events_file, foreign=0):
        events = events_file.read_text().splitlines() if events_file.exists() else []
        assert not any(line.startswith("gh ") or line.startswith("git push ") for line in events), events
        tags = subprocess.check_output([real_git, "-C", str(repo), "tag", "--list", version], text=True).strip()
        assert tags == "", f"tag {version} exists after a refusal"
        subjects = subprocess.check_output([real_git, "-C", str(repo), "log", "--format=%s"], text=True).splitlines()
        assert subjects.count(f"Package release {version}") == foreign, subjects

    red, red_env = fixture(base / "red")
    run(["bash", "scripts/release.sh", version, "--publish"], red, dict(red_env, STUB_SEQUENCES="red"), expected=1, contains="sequence suites are red")
    assert_no_release(red, red_env, base / "red/events")
    assert not (red / "target/release-1.2.3/RECEIPT.json").exists()
    print("ok a red sequence lane refuses the tag without a receipt")
    absent, absent_env = fixture(base / "absent", manifest=False)
    run(["bash", "scripts/release.sh", version, "--publish"], absent, absent_env, expected=1, contains="sequence manifest missing")
    assert_no_release(absent, absent_env, base / "absent/events")
    assert not (absent / "target/release-1.2.3").exists(), "the stage was built before the manifest check"
    print("ok a missing manifest refuses the release before any build")
    boundary = "core/src/lifecycle/state.rs"
    app_red, app_red_env = fixture(base / "app-red", touch=boundary)
    run(["bash", "scripts/release.sh", version, "--publish"], app_red, dict(app_red_env, STUB_CONSUMERS="red"), expected=1, contains="app suites are red")
    assert_no_release(app_red, app_red_env, base / "app-red/events")
    assert any(line.startswith("check_consumers ") for line in (base / "app-red/events").read_text().splitlines())
    assert not (app_red / "target/release-1.2.3/RECEIPT.json").exists()
    print("ok a red app suite refuses the tag after a lifecycle change")
    forced, forced_env = fixture(base / "forced")
    run(["bash", "scripts/release.sh", version, "--publish"], forced, dict(forced_env, RELEASE_CONSUMERS="always", STUB_CONSUMERS="red"), expected=1, contains="app suites are red")
    assert_no_release(forced, forced_env, base / "forced/events")
    print("ok RELEASE_CONSUMERS=always runs the app suites without a boundary change")
    green, green_env = fixture(base / "green", touch=boundary)
    run(["bash", "scripts/release.sh", version, "--publish"], green, green_env)
    receipt = json.loads((green / "target/release-1.2.3/RECEIPT.json").read_text())
    assert receipt["consumers"] == "required" and receipt["consumer_lanes"] == {"ios": {"rc": "0"}, "android": {"rc": "0"}}, receipt
    green_events = (base / "green/events").read_text().splitlines()
    assert any(line.startswith("check_consumers ") and "--maven " in line and "--package-swift " in line for line in green_events), green_events
    assert subprocess.check_output([real_git, "-C", str(green), "cat-file", "-t", version], text=True).strip() == "commit"
    print("ok green sequences and app suites tag the release and record the receipt")
    for kind, variable, modes, touch in (
        ("sequences", "STUB_REPORT", ("missing-file", "missing-lane", "partial", "zero", "rc"), None),
        ("consumers", "STUB_CONSUMER_REPORT", ("missing-file", "missing-lane", "rc"), boundary),
    ):
        for mode in modes:
            bad, bad_env = fixture(base / f"report-{kind}-{mode}", touch=touch)
            text = f"release: {kind}.tsv is missing or empty" if mode == "missing-file" else "lane reports are missing or incomplete"
            run(["bash", "scripts/release.sh", version, "--publish"], bad, dict(bad_env, **{variable: mode}), expected=1, contains=text)
            assert_no_release(bad, bad_env, base / f"report-{kind}-{mode}/events")
            assert not (bad / "target/release-1.2.3/RECEIPT.json").exists()
            print(f"ok {kind} report {mode} refuses the tag")
    for mode, variables, text in (
        ("another-sha", {"STUB_SHA": "0" * 40}, "lane reports are missing or incomplete"),
        ("another-run", {"STUB_RUN": "another-run"}, "lane reports are missing or incomplete"),
        ("head-moved", {"STUB_MOVE_HEAD": "1"}, "source commit changed during the run"),
        ("foreign-package-commit", {"STUB_MOVE_HEAD": "1", "STUB_MOVE_SUBJECT": f"Package release {version}"}, "source commit changed during the run"),
    ):
        bad, bad_env = fixture(base / f"identity-{mode}", touch=boundary)
        run(["bash", "scripts/release.sh", version, "--publish"], bad, dict(bad_env, **variables), expected=1, contains=text)
        assert_no_release(bad, bad_env, base / f"identity-{mode}/events", foreign=int(mode == "foreign-package-commit"))
        assert not (bad / "target/release-1.2.3/RECEIPT.json").exists()
        print(f"ok {mode} refuses the receipt, the package commit and the tag")
    dry, dry_env = fixture(base / "identity-foreign-package-commit-dry", touch=boundary)
    run(["bash", "scripts/release.sh", version], dry, dict(dry_env, STUB_MOVE_HEAD="1", STUB_MOVE_SUBJECT=f"Package release {version}"), expected=1, contains="source commit changed during the run")
    assert_no_release(dry, dry_env, base / "identity-foreign-package-commit-dry/events", foreign=1)
    assert not (dry / "target/release-1.2.3/RECEIPT.json").exists()
    print("ok foreign-package-commit refuses the receipt in the dry run")
    late, late_env = fixture(base / "foreign-after-tag")
    run(["bash", "scripts/release.sh", version, "--publish"], late, dict(late_env, STUB_AFTER_TAG="1"))
    tagged = subprocess.check_output([real_git, "-C", str(late), "rev-parse", version + "^{commit}"], text=True).strip()
    head = subprocess.check_output([real_git, "-C", str(late), "rev-parse", "HEAD"], text=True).strip()
    assert tagged != head, "the foreign commit did not land after the tag"
    late_events = (base / "foreign-after-tag/events").read_text().splitlines()
    assert f"git push origin {tagged}:refs/heads/master" in late_events, late_events
    assert f"git push origin {tagged}:refs/tags/{version}" in late_events, late_events
    assert not any(head in line or line == "git push origin master" for line in late_events if line.startswith("git push ")), late_events
    print("ok a foreign commit landing after the tag is not pushed")
    package_cases = (
        ("extra", ("Package.swift", "swift/Sources/DBlayer/dblayer.swift", "core/src/extra.rs")),
        ("only-binding", ("swift/Sources/DBlayer/dblayer.swift",)),
    )
    for case, files in package_cases:
        for mode_args, label in (([], "dry run"), (["--publish"], "publish")):
            name = f"package-{case}-{label.replace(' ', '-')}"
            bad, bad_env = fixture(base / name)
            for file in files:
                (bad / file).parent.mkdir(parents=True, exist_ok=True)
                (bad / file).write_text("changed\n")
            subprocess.run([real_git, "-C", str(bad), "add", "-f", *files], check=True)
            subprocess.run([real_git, "-C", str(bad), "commit", "-m", f"Package release {version}"], check=True, capture_output=True)
            run(["bash", "scripts/release.sh", version, *mode_args], bad, bad_env, expected=1, contains="must change")
            bad_events = (base / name / "events").read_text().splitlines() if (base / name / "events").exists() else []
            assert not any(line.startswith("gh ") or line.startswith("git push ") for line in bad_events), bad_events
            assert subprocess.check_output([real_git, "-C", str(bad), "tag", "--list", version], text=True).strip() == ""
            assert not (bad / "target/release-1.2.3").exists()
            assert not (bad / "target/release.lock").exists()
            print(f"ok a Package release commit with {case} files is refused in the {label}")
    unchanged, unchanged_env = fixture(base / "package-unchanged-binding")
    (unchanged / "Package.swift").write_text("changed\n")
    subprocess.run([real_git, "-C", str(unchanged), "add", "Package.swift"], check=True)
    subprocess.run([real_git, "-C", str(unchanged), "commit", "-m", f"Package release {version}"], check=True, capture_output=True)
    run(["bash", "scripts/release.sh", version], unchanged, unchanged_env, contains="Package.swift to be written:")
    print("ok a Package release commit with only Package.swift (binding unchanged) is accepted")
    own, own_env = fixture(base / "own-extra")
    run(["bash", "scripts/release.sh", version, "--publish"], own, dict(own_env, STUB_COMMIT_EXTRA="core/extra.rs"), expected=1, contains="must change")
    assert subprocess.check_output([real_git, "-C", str(own), "tag", "--list", version], text=True).strip() == ""
    assert not any(line.startswith("gh ") or line.startswith("git push ") for line in (base / "own-extra/events").read_text().splitlines())
    print("ok an extra staged file in the own package commit refuses the tag")
    wrong, wrong_env = fixture(base / "own-wrong-parent")
    run(["bash", "scripts/release.sh", version, "--publish"], wrong, dict(wrong_env, STUB_REPARENT="1"), expected=1, contains="is not directly on source commit")
    assert subprocess.check_output([real_git, "-C", str(wrong), "tag", "--list", version], text=True).strip() == ""
    assert not any(line.startswith("gh ") or line.startswith("git push ") for line in (base / "own-wrong-parent/events").read_text().splitlines())
    print("ok the own package commit with a wrong parent refuses the tag")

    def lock_fixture(name):
        repo, env = fixture(base / name)
        (repo / "target/release.lock").mkdir(parents=True)
        return repo, env, repo / "target/release.lock"

    locked, locked_env, lock = lock_fixture("locked")
    holder = subprocess.Popen(["sleep", "60"])
    try:
        (lock / "holder").write_text(f"{holder.pid} {version}\n")
        run(["bash", "scripts/release.sh", version], locked, locked_env, expected=1, contains=f"holder pid {holder.pid}, alive")
        assert (lock / "holder").read_text().startswith(str(holder.pid)), "the second run removed the lock of a live holder"
        assert not (locked / "target/release-1.2.3").exists()
    finally:
        holder.kill()
        holder.wait()
    run(["bash", "scripts/release.sh", version], locked, locked_env, expected=1, contains=f"holder pid {holder.pid}, dead")
    assert (lock / "holder").read_text().startswith(str(holder.pid)), "the lock of a dead holder was replaced"
    assert not (locked / "target/release-1.2.3").exists()
    unwritten, unwritten_env, unwritten_lock = lock_fixture("lock-no-holder")
    run(["bash", "scripts/release.sh", version], unwritten, unwritten_env, expected=1, contains="holder pid none, unknown")
    assert unwritten_lock.is_dir(), "the lock without a holder was replaced"
    assert str(unwritten_lock) in run(["bash", "scripts/release.sh", version], unwritten, unwritten_env, expected=1).stderr
    shutil.rmtree(lock)
    run(["bash", "scripts/release.sh", version], locked, locked_env, contains="Package.swift to be written:")
    assert not lock.exists(), "the lock stayed after the run ended"
    print("ok an existing lock refuses the run, is never replaced, and a finished run releases it")
    import signal
    import time
    for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        sig_repo, sig_env = fixture(base / f"signal-{sig.name}")
        (sig_repo / "scripts/check_docs.sh").write_text("#!/bin/sh\nmkdir -p \"$TEST_ROOT/target\"; touch \"$TEST_ROOT/target/started\"\nsleep 2\n")
        subprocess.run([real_git, "-C", str(sig_repo), "commit", "-aq", "--amend", "--no-edit"], check=True, capture_output=True)
        proc = subprocess.Popen(["bash", "scripts/release.sh", version], cwd=sig_repo, env=sig_env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 30
        while not (sig_repo / "target/started").exists():
            assert time.time() < deadline and proc.poll() is None, f"the release run did not reach its first check: {proc.poll()} {proc.stderr.read() if proc.poll() is not None else ''}"
            time.sleep(0.05)
        assert (sig_repo / "target/release.lock").is_dir()
        proc.send_signal(sig)
        assert proc.wait(timeout=60) == 128 + sig.value, proc.returncode
        assert not (sig_repo / "target/release.lock").exists(), f"the lock stayed after {sig.name}"
        run(["bash", "scripts/release.sh", version], sig_repo, sig_env, contains="Package.swift to be written:")
        print(f"ok {sig.name} releases the lock")
    dry, dry_env = fixture(base / "report-dry")
    run(["bash", "scripts/release.sh", version], dry, dict(dry_env, STUB_REPORT="partial"), expected=1, contains="lane reports are missing or incomplete")
    assert not (dry / "target/release-1.2.3/RECEIPT.json").exists()
    print("ok the dry run refuses an incomplete report")
    sequences_script = release_script.parent / "check_sequences.sh"

    def swift_lane(name, results):
        root = base / name
        for script in ("check_sequences.sh", "check_bounded.sh", "check_steps.sh"):
            (root / "scripts").mkdir(parents=True, exist_ok=True)
            shutil.copy2(release_script.parent / script, root / "scripts" / script)
        (root / "core/src/ffi/selftest").mkdir(parents=True)
        (root / "core/src/ffi/selftest/manifest.tsv").write_text("version\t1\nid\taliases\tlane\ttest\tstate\ts1\tprovenance\n" + "".join(f"{n}\t\tswift\tSuite.{n}\tx\tx\tx\n" for n, _ in results))
        (root / "swift/Sources/DBlayer").mkdir(parents=True)
        (root / "swift/Sources/DBlayer/dblayer.swift").write_text("bindings")
        (root / "swift/DBlayerFFI.xcframework").mkdir()
        (root / "swift/DBlayerFFI.xcframework/Info.plist").write_text("info")
        for path, text in (("src/main/kotlin/com/noma4i/dblayer/dblayer.kt", "kt"), *((f"src/main/jniLibs/{abi}/libdblayer.so", abi) for abi in ("arm64-v8a", "armeabi-v7a", "x86_64", "x86"))):
            (root / "kotlin" / path).parent.mkdir(parents=True, exist_ok=True)
            (root / "kotlin" / path).write_text(text)
        stage = root / "stage"
        stage.mkdir()
        (stage / "dblayer.swift").write_text("bindings")
        with zipfile.ZipFile(stage / "DBlayerFFI.xcframework.zip", "w") as archive:
            archive.writestr("DBlayerFFI.xcframework/Info.plist", "info")
        with zipfile.ZipFile(stage / "dblayer-android.zip", "w") as archive:
            archive.writestr("src/main/kotlin/com/noma4i/dblayer/dblayer.kt", "kt")
            for abi in ("arm64-v8a", "armeabi-v7a", "x86_64", "x86"):
                archive.writestr(f"src/main/jniLibs/{abi}/libdblayer.so", abi)
        bin_dir = root / "bin"
        bin_dir.mkdir()
        failure = '<failure message="x"/>'
        cases = "".join(f'<testcase classname="DBlayerTests.Suite" name="{n}()">{"" if good else failure}</testcase>' for n, good in results)
        swift = bin_dir / "swift"
        swift.write_text(f"""#!/bin/sh
out=""
while [ "$#" -gt 0 ]; do [ "$1" = --xunit-output ] && out="$2"; shift; done
printf '<testsuites><testsuite name="s">{cases}</testsuite></testsuites>' > "${{out%.xml}}-swift-testing.xml"
echo 'Test run passed'
""")
        swift.chmod(0o755)
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", BUILD_SLOT=str(root / "none"))
        return root, stage, env

    root, stage, env = swift_lane("swift-green", [("a", True), ("b", True)])
    run(["bash", "scripts/check_sequences.sh", str(stage), "--lane", "swift"], root, env, contains="")
    assert (stage / "sequences.tsv").read_text().split("\t")[:3] == ["swift", "2", "2"], (stage / "sequences.tsv").read_text()
    root, stage, env = swift_lane("swift-red", [("a", True), ("b", False)])
    run(["bash", "scripts/check_sequences.sh", str(stage), "--lane", "swift"], root, env, expected=1, contains="1 of 2 listed tests passed")
    print("ok the Swift lane counts passed tests from the xunit report and fails a failed one")
PY
