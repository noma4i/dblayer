#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python3 - "$root" <<'PY'
import io
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


def fixture(base):
    repo = base / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy2(release_script, repo / "scripts/release.sh")
    for name in ("check_docs.sh", "check_invariants.sh", "check_zoo.sh"):
        (repo / "scripts" / name).write_text("#!/bin/sh\nexit 0\n")
    (repo / "scripts/build_ios.sh").write_text("""#!/bin/bash
mkdir -p swift/DBlayerFFI.xcframework/{macos-arm64_x86_64,ios-arm64_x86_64-simulator,ios-arm64} swift/Sources/DBlayer
printf bindings > swift/Sources/DBlayer/dblayer.swift
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
    (repo / "core").mkdir()
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
if command and command[0] in ("clone", "fetch", "pull", "push"): sys.exit(0)
if a[:2] == ["ls-remote", "--heads"]: sys.exit(0)
if a[:2] == ["remote", "get-url"]: print("https://example.invalid/dblayer.git"); sys.exit(0)
sys.exit(subprocess.run([os.environ["REAL_GIT"], *a]).returncode)
''',
        "cargo": '''#!/usr/bin/env python3
import pathlib, sys
a=sys.argv[1:]; target=a[a.index("--target")+1]
p=pathlib.Path("target")/target/"release/dblayer-gen"; p.parent.mkdir(parents=True,exist_ok=True); p.write_text("generator")
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
    bash_env.write_text("cargo() { local target=; while [ \"$#\" -gt 0 ]; do if [ \"$1\" = --target ]; then target=$2; break; fi; shift; done; mkdir -p \"target/$target/release\"; printf generator > \"target/$target/release/dblayer-gen\"; }\nxcodebuild() { return 97; }\nexport -f cargo xcodebuild\n")
    env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", BASH_ENV=str(bash_env), REAL_GIT=real_git, TEST_ROOT=str(repo), TEST_EVENTS=str(events), GRADLE=str(bin_dir / "gradle"))
    return repo, env


with tempfile.TemporaryDirectory(prefix="dblayer-release-modes-") as directory:
    base = Path(directory)
    repo, env = fixture(base)
    run(["bash", "scripts/release.sh", version], repo, env, contains="Package.swift to be written:")
    stage = repo / "target/release-1.2.3"
    assert sorted(path.name for path in stage.iterdir()) == sorted(attachments + ["SHA256SUMS", "NOTES.md"])
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

    wrong, wrong_env = fixture(base / "wrong")
    subprocess.run([real_git, "-C", str(wrong), "commit", "--amend", "-m", "Release 9.9.9"], check=True, capture_output=True)
    run(["bash", "scripts/release.sh", "9.9.9"], wrong, wrong_env, expected=1, contains="core version does not match")
    print("ok wrong version is rejected")
    dirty, dirty_env = fixture(base / "dirty")
    (dirty / "Package.swift").write_text("dirty")
    run(["bash", "scripts/release.sh", version], dirty, dirty_env, expected=1, contains="working tree is not clean")
    print("ok dirty tracked file is rejected")
    incomplete, incomplete_env = fixture(base / "incomplete")
    stage = incomplete / "target/release-1.2.3"
    stage.mkdir(parents=True)
    (stage / "DBlayerFFI.xcframework.zip").write_text("partial")
    (stage / "SHA256SUMS").write_text("partial")
    run(["bash", "scripts/release.sh", version], incomplete, incomplete_env, expected=1, contains="incomplete existing output")
    print("ok incomplete stage is rejected")
PY
