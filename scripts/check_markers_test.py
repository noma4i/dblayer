import shutil
import subprocess
import tempfile
from pathlib import Path

WORD = "leg" + "acy"
SCRIPT = f"check_{WORD}.sh"
source = Path(__file__).resolve().parent / SCRIPT
VCS = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"]


def sh(command, cwd=None, expect=0, contains=None):
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    output = result.stdout + result.stderr
    assert result.returncode == expect, f"{command}: rc={result.returncode} output={output}"
    if contains:
        assert contains in output, f"expected {contains!r} in {output!r}"


def check(files, expect, contains, after=None):
    with tempfile.TemporaryDirectory() as base:
        root = Path(base)
        (root / "scripts").mkdir()
        shutil.copy2(source, root / "scripts" / SCRIPT)
        (root / "core/src").mkdir(parents=True)
        (root / "core/src/lib.rs").write_text("fn main() {}\n")
        for name, text in files.items():
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            (root / name).write_text(text)
        sh(VCS + ["init", "-q"], cwd=root)
        sh(VCS + ["add", "-f", "."], cwd=root)
        sh(VCS + ["commit", "-q", "-m", "fixture"], cwd=root)
        sh(["bash", str(root / "scripts" / SCRIPT), "check"], expect=expect, contains=contains)
        if after:
            after(root)


def count_modes(root):
    sh(["bash", str(source), "count", str(root), "HEAD"], contains="0 0")
    sh(["bash", str(source), "count", str(root), "no-such-ref"], expect=2, contains="git grep failed")
    sh(["bash", str(source), "count", str(root)], expect=2, contains=f"usage: {SCRIPT} count")
    sh(["bash", str(source)], expect=2, contains=f"usage: {SCRIPT} count")


check({}, 0, f"check_{WORD}: ok", after=count_modes)
check({"core/src/a.rs": f"// {WORD} path\n"}, 1, f"1 {WORD} lines or files, 0 development-file")
check({"core/src/old.rs": "fn x() {}\n"}, 1, f"1 {WORD} lines or files, 0 development-file")
check({"docs/a.md": "see CLAUDE.md\n"}, 1, f"0 {WORD} lines or files, 1 development-file")
check({"CLAUDE.md": "notes\n"}, 1, "development-file references or files")
print("check_markers_test: ok")
