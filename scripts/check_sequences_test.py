import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

scripts = Path(__file__).resolve().parent
ABIS = ["arm64-v8a", "armeabi-v7a", "x86_64", "x86"]
HEADER = "version\t1\nid\taliases\tlane\ttest\tstate\ts1\tprovenance\n"
ROWS = (
    "S1\ta\tcore\talpha\tgreen\ts\tp\n"
    "S1\ta\tswift\tBridgeProbeTests.one\tgreen\ts\tp\n"
    "S1\ta\tkotlin\tcom.x.KTest.one\tgreen\ts\tp\n"
)
CARGO = """#!/bin/bash
mode=${STUB_CORE:-green}
after_lib=0
for arg in "$@"; do
  if [ "$after_lib" = 1 ] && [ "$mode" != skip ]; then echo "test ffi::selftest::$arg ... ok"; fi
  [ "$arg" = --lib ] && after_lib=1
done
[ "$mode" = red ] && exit 1
exit 0
"""
SWIFT = """#!/bin/bash
out=""; names=()
while [ $# -gt 0 ]; do case $1 in --xunit-output) out=$2; shift 2;; --filter) names+=("$2"); shift 2;; *) shift;; esac; done
mode=${STUB_SWIFT:-green}
{
  echo '<testsuites><testsuite>'
  for n in "${names[@]}"; do
    n=${n//\\\\/}
    printf '<testcase classname="Pkg.%s" name="%s()">\\n' "${n%%/*}" "${n#*/}"
    [ "$mode" = skip ] && echo '<skipped/>'
    echo '</testcase>'
  done
  echo '</testsuite></testsuites>'
} > "$out"
[ "$mode" = red ] && exit 1
exit 0
"""
GRADLE = """#!/bin/bash
dir=""; names=()
while [ $# -gt 0 ]; do case $1 in -p) dir=$2; shift 2;; --tests) names+=("$2"); shift 2;; *) shift;; esac; done
mode=${STUB_KOTLIN:-green}
mkdir -p "$dir/build/test-results/testDebugUnitTest"
{
  echo '<testsuite>'
  for n in "${names[@]}"; do
    printf '<testcase classname="%s" name="%s()">\\n' "${n%.*}" "${n##*.}"
    [ "$mode" = failure ] && echo '<failure/>'
    echo '</testcase>'
  done
  echo '</testsuite>'
} > "$dir/build/test-results/testDebugUnitTest/TEST-k.xml"
exit 0
"""


def write(path, text, executable=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    if executable:
        path.chmod(0o755)


def build(base):
    root = base / "root"
    (root / "scripts").mkdir(parents=True)
    for name in ("check_sequences.sh", "check_bounded.sh", "check_steps.sh"):
        shutil.copy2(scripts / name, root / "scripts" / name)
    write(root / "scripts/build_kotlin.sh", "#!/bin/sh\nexit 0\n")
    write(root / "swift/Sources/DBlayer/dblayer.swift", "swift bindings")
    write(root / "swift/DBlayerFFI.xcframework/Info.plist", "plist")
    write(root / "kotlin/src/main/kotlin/com/noma4i/dblayer/dblayer.kt", "kotlin bindings")
    for abi in ABIS:
        write(root / f"kotlin/src/main/jniLibs/{abi}/libdblayer.so", f"so {abi}")
    stage = base / "stage"
    stage.mkdir()
    write(stage / "dblayer.swift", "swift bindings")
    with zipfile.ZipFile(stage / "DBlayerFFI.xcframework.zip", "w") as archive:
        archive.writestr("DBlayerFFI.xcframework/Info.plist", "plist")
    with zipfile.ZipFile(stage / "dblayer-android.zip", "w") as archive:
        archive.writestr("src/main/kotlin/com/noma4i/dblayer/dblayer.kt", "kotlin bindings")
        for abi in ABIS:
            archive.writestr(f"src/main/jniLibs/{abi}/libdblayer.so", f"so {abi}")
    stubs = base / "stubs"
    write(stubs / "cargo", CARGO, True)
    write(stubs / "swift", SWIFT, True)
    write(stubs / "gradle", GRADLE, True)
    write(base / "env.sh", f'export PATH="{stubs}:$PATH"\n')
    manifest = base / "manifest.tsv"
    manifest.write_text(HEADER + ROWS)
    return root, stage, manifest


def run(mutate=None, args=None, env=None, expect=0, contains=None, stage_arg=None):
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        root, stage, manifest = build(base)
        if mutate:
            mutate(root, stage, manifest)
        given = [str(stage if stage_arg is None else stage_arg), "--manifest", str(manifest)]
        full = dict(os.environ, BASH_ENV=str(base / "env.sh"), GRADLE=str(base / "stubs/gradle"), RELEASE_SOURCE_SHA="abc", RELEASE_RUN_ID="run1")
        full.pop("BUILD_SLOT", None)
        full.update(env or {})
        result = subprocess.run(["bash", str(root / "scripts/check_sequences.sh"), *(args(given) if args else given)], env=full, capture_output=True, text=True)
        output = result.stdout + result.stderr
        if result.returncode != expect:
            logs = {item.name: item.read_text() for item in sorted((stage / "sequence-logs").glob("*"))}
            raise AssertionError(f"rc={result.returncode} output={output} logs={logs}")
        if contains:
            assert contains in output, f"expected {contains!r} in {output!r}"
        report = stage / "sequences.tsv"
        return report.read_text() if report.exists() else None


def edit(path_of, text):
    return lambda root, stage, manifest: path_of(root, stage, manifest).write_text(text)


tsv = run(contains="sequences: lane kotlin green: 1 of 1 listed tests passed")
assert all(line.split("\t")[5:] == ["abc", "run1"] for line in tsv.splitlines()), tsv
assert [line.split("\t")[:4] for line in tsv.splitlines()] == [["core", "1", "1", "0"], ["swift", "1", "1", "0"], ["kotlin", "1", "1", "0"]], tsv
run(args=lambda c: c + ["--lane", "core"], contains="lane core green")
run(args=lambda c: [], expect=1, contains="usage: check_sequences.sh")
run(args=lambda c: c + ["--bogus"], expect=1, contains="unknown argument: --bogus")
run(args=lambda c: [c[0], "--manifest"], expect=1, contains="--manifest needs a file")
run(args=lambda c: c + ["--lane"], expect=1, contains="--lane needs a name")
run(args=lambda c: [c[0], "--manifest", "/no/such/manifest"], expect=1, contains="sequence manifest missing")
run(stage_arg="/no/such/stage", expect=1, contains="stage missing")
run(lambda r, s, m: (s / "dblayer-android.zip").unlink(), expect=1, contains="stage lacks dblayer-android.zip")
run(edit(lambda r, s, m: m, "version\t2\n" + HEADER.split("\n", 1)[1] + ROWS), expect=1, contains='manifest version line must be "version<TAB>1"')
run(edit(lambda r, s, m: m, "version\t1\nid\ttest\n" + ROWS), expect=1, contains="manifest header line is not the expected column list")
run(edit(lambda r, s, m: s / "dblayer.swift", "other"), expect=1, contains="staged dblayer.swift differs")
run(edit(lambda r, s, m: r / "swift/DBlayerFFI.xcframework/Info.plist", "other"), expect=1, contains="staged XCFramework differs")
run(edit(lambda r, s, m: r / "kotlin/src/main/kotlin/com/noma4i/dblayer/dblayer.kt", "other"), expect=1, contains="staged dblayer.kt differs")
run(edit(lambda r, s, m: r / "kotlin/src/main/jniLibs/x86/libdblayer.so", "other"), expect=1, contains="staged x86 library differs")
run(args=lambda c: c + ["--lane", "bogus"], expect=1, contains="unknown lane: bogus")
tsv = run(edit(lambda r, s, m: m, HEADER + ROWS.splitlines(True)[0]), args=lambda c: c + ["--lane", "swift"], expect=1, contains="lane swift lists no test in the manifest")
assert tsv.startswith("swift\t0\t0\tmissing\t"), tsv
tsv = run(env={"STUB_CORE": "red"}, expect=1, contains="sequences: lane core red: rc=1, 1 of 1 listed tests passed")
assert tsv.splitlines()[0].split("\t")[:4] == ["core", "1", "1", "1"], tsv
tsv = run(env={"STUB_CORE": "skip"}, expect=1, contains="lane core red: rc=0, 0 of 1 listed tests passed")
run(env={"STUB_SWIFT": "skip"}, expect=1, contains="lane swift red: rc=0, 0 of 1 listed tests passed")
run(env={"STUB_KOTLIN": "failure"}, expect=1, contains="lane kotlin red: rc=0, 0 of 1 listed tests passed")
print("check_sequences_test: ok")
