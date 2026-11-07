import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

source = Path(__file__).resolve().parent / "check_docs.sh"
PAIR = "```swift\nlet a = 1\n```\n\n```kotlin\nval a = 1\n```\n"


def run(readme, extra=None, expect=0, contains=None):
    with tempfile.TemporaryDirectory() as base:
        root = Path(base)
        (root / "scripts").mkdir()
        shutil.copy2(source, root / "scripts/check_docs.sh")
        (root / "core/src").mkdir(parents=True)
        (root / "docs").mkdir()
        (root / "README.md").write_text(readme)
        (root / "docs/guide.md").write_text(extra or "Plain guide.\n")
        result = subprocess.run(["bash", str(root / "scripts/check_docs.sh")], capture_output=True, text=True)
    output = result.stdout + result.stderr
    assert result.returncode == expect, f"rc={result.returncode} output={output}"
    if contains:
        assert contains in output, f"expected {contains!r} in {output!r}"


run("Intro. See `core/src` and [guide](docs/guide.md).\n\n" + PAIR, contains="docs check: clean")
run("```swift\nlet a = 1\n```\n", expect=1, contains="1 swift examples but 0 kotlin examples")
run("Privet \u043c\u0438\u0440\n", expect=1, contains="README.md:1: non-English text")
run("A helper here.\n", expect=1, contains="banned word 'helper'")
run("This is planned.\n", expect=1, contains="status wording 'planned'")
run("Path `core/missing.rs` is named.\n", expect=1, contains="path does not exist: core/missing.rs")
run("Link [guide](docs/absent.md).\n", expect=1, contains="broken link: docs/absent.md")
run("fine\n", extra="A wrapper word.\n", expect=1, contains="docs/guide.md: banned word 'wrapper'")
print("check_docs_test: ok")
