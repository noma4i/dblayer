import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

source = Path(__file__).resolve().parent / "check_consumers.sh"
VCS = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"]
PACKAGE = 'targets: [.binaryTarget(name: "F", url: "https://x/y.zip", checksum: "abc")]\n'
PROBE = 'test "$DBLAYER_VERSION" = 1.2.3 && grep -q \'path: "DBlayerFFI.xcframework"\' "$DBLAYER_PACKAGE/Package.swift" && test -s "$DBLAYER_MAVEN/dblayer-1.2.3.aar" && test -s "$DBLAYER_GEN"'


def vcs(args, cwd):
    subprocess.run(VCS + args, cwd=cwd, check=True, capture_output=True)


def build(base):
    root = base / "root"
    (root / "scripts").mkdir(parents=True)
    shutil.copy2(source, root / "scripts/check_consumers.sh")
    for name, text in {"core/a.rs": "x", "gen/b.rs": "x", "Cargo.toml": "x", "Cargo.lock": "x", "swift/Sources/DBlayer/c.swift": "x"}.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    vcs(["init", "-q"], root)
    vcs(["add", "-A"], root)
    vcs(["commit", "-q", "-m", "root"], root)
    repo = base / "consumer"
    repo.mkdir()
    vcs(["init", "-q", "-b", "master"], repo)
    (repo / "app.txt").write_text("app")
    vcs(["add", "-A"], repo)
    vcs(["commit", "-q", "-m", "app"], repo)
    stage = base / "stage"
    stage.mkdir()
    with zipfile.ZipFile(stage / "DBlayerFFI.xcframework.zip", "w") as archive:
        archive.writestr("DBlayerFFI.xcframework/Info.plist", "plist")
    for name in ("dblayer.swift", "dblayer-gen-macos-universal"):
        (stage / name).write_text("x")
    maven = base / "maven"
    maven.mkdir()
    for ext in ("aar", "pom", "module"):
        (maven / f"dblayer-1.2.3.{ext}").write_text("x")
    package_swift = base / "Package.swift"
    package_swift.write_text(PACKAGE)
    return root, repo, stage, maven, package_swift


def run(mutate=None, args=None, env=None, expect=0, contains=None):
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        root, repo, stage, maven, package_swift = build(base)
        if mutate:
            mutate(stage, maven, package_swift)
        given = [str(stage), "--version", "1.2.3", "--maven", str(maven), "--package-swift", str(package_swift)]
        full = dict(os.environ, CONSUMER_REPO=str(repo), CONSUMER_WORK_ROOT=str(base), CONSUMER_IOS_COMMAND=PROBE, CONSUMER_ANDROID_COMMAND=PROBE)
        for key in ("BUILD_SLOT", "CONSUMER_IOS_REF", "CONSUMER_ANDROID_REF"):
            full.pop(key, None)
        full.update(env or {})
        full = {key: value for key, value in full.items() if value is not None}
        result = subprocess.run(["bash", str(root / "scripts/check_consumers.sh"), *(args(given) if args else given)], env=full, capture_output=True, text=True)
        output = result.stdout + result.stderr
        assert result.returncode == expect, f"rc={result.returncode} output={output}"
        if contains:
            assert contains in output, f"expected {contains!r} in {output!r}"
        return (stage / "consumers.tsv").read_text() if (stage / "consumers.tsv").exists() else None


def drop(path_of):
    return lambda stage, maven, package_swift: path_of(stage, maven, package_swift).unlink()


tsv = run(contains="consumers: lane android green")
assert [line.split("\t")[:2] for line in tsv.splitlines()] == [["ios", "0"], ["android", "0"]], tsv
tsv = run(args=lambda c: c + ["--lane", "ios"], env={"CONSUMER_ANDROID_COMMAND": None})
assert tsv.count("\n") == 1 and tsv.startswith("ios\t0\t"), tsv
tsv = run(env={"CONSUMER_ANDROID_COMMAND": "exit 3"}, expect=1, contains="consumers: lane android red: rc=3")
assert [line.split("\t")[:2] for line in tsv.splitlines()] == [["ios", "0"], ["android", "3"]], tsv
tsv = run(env={"CONSUMER_ANDROID_COMMAND": "tail -f /dev/null", "CONSUMER_LANE_LIMIT": "1"}, expect=1, contains="consumers: lane android red: rc=124")
assert "android\t124\t" in tsv, tsv
tsv = run(env={"CONSUMER_IOS_REF": "no-such-ref"}, expect=1, contains="consumers: lane ios red")
assert "ios\t1\t" in tsv, tsv
run(args=lambda c: [], expect=1, contains="usage: check_consumers.sh")
run(args=lambda c: c + ["--bogus", "x"], expect=1, contains="unknown argument: --bogus")
run(args=lambda c: c + ["--lane"], expect=1, contains="--lane needs a value")
run(args=lambda c: c + ["--lane", "tv"], expect=1, contains="unknown lane: tv")
run(args=lambda c: [c[0], "--version", "1.2", *c[3:]], expect=1, contains="--version x.y.z is required")
run(args=lambda c: ["/no/such/stage", *c[1:]], expect=1, contains="stage missing")
run(args=lambda c: [*c[:3], "--maven", "/no/such/maven", *c[5:]], expect=1, contains="--maven directory missing")
run(drop(lambda s, m, p: p), expect=1, contains="--package-swift file missing")
run(drop(lambda s, m, p: s / "dblayer.swift"), expect=1, contains="stage lacks dblayer.swift")
run(drop(lambda s, m, p: m / "dblayer-1.2.3.pom"), expect=1, contains="Maven directory lacks dblayer-1.2.3.pom")
run(env={"CONSUMER_REPO": "", "CONSUMER_IOS_COMMAND": "", "CONSUMER_ANDROID_COMMAND": ""}, expect=1, contains="unset inputs: CONSUMER_REPO (consumer repository checkout) CONSUMER_IOS_COMMAND (command of the iOS lane) CONSUMER_ANDROID_COMMAND")
run(env={"CONSUMER_REPO": "/no/such/repo"}, expect=1, contains="CONSUMER_REPO is not a git checkout")
print("check_consumers_test: ok")
