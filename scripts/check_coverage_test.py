import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class CoverageTests(unittest.TestCase):
    def test_reports_account_for_current_transport_sources(self):
        script = Path(__file__).with_name("check_coverage.sh")
        cases = [
            ("covered", None, None, 0),
            ("covered continuation with an untaken branch", "swift", "continuation", 0),
            ("missing Swift", "swift", "missing", 1),
            ("missing Kotlin", "kotlin", "missing", 1),
            ("empty Swift", "swift", "empty", 1),
            ("empty Kotlin", "kotlin", "empty", 1),
            ("uncovered Swift", "swift", "uncovered", 1),
            ("uncovered Kotlin", "kotlin", "uncovered", 1),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            shutil.copyfile(script, root / "scripts/check_coverage.sh")
            paths = {
                "swift": ["swift/Sources/DBlayer/Network/TransportFailure.swift",
                          "swift/Sources/DBlayer/UrlSessionTransport.swift"],
                "kotlin": ["kotlin/src/main/kotlin/com/noma4i/dblayer/OkHttpEncryptedSocket.kt",
                           "kotlin/src/main/kotlin/com/noma4i/dblayer/TransportFailure.kt"],
            }
            for names in paths.values():
                for name in names:
                    target = root / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text("sendFrame()\n")
            (root / "swift/Sources/DBlayer/Network/GraphQLOperationKind.swift").write_text(
                "enum GraphQLOperationKind { case query, mutation }\n")
            allowlist = root / "allowlist"
            allowlist.write_text("")
            for label, platform, defect, expected in cases:
                with self.subTest(case=label):
                    reports = {}
                    for language, names in paths.items():
                        reports[language] = [(name, 1) for name in names]
                        if platform == language:
                            if defect == "missing":
                                reports[language].pop()
                            elif defect == "empty":
                                reports[language] = []
                            elif defect == "uncovered":
                                reports[language][0] = (names[0], 0)
                    swift = root / "swift.json"
                    swift.write_text(json.dumps({"data": [{"files": [
                        {"filename": str(root / name),
                         "segments": [[1, 1, count, True, True, False],
                                      [2, 1, 0, False, False, False]]}
                        for name, count in reports["swift"]]}]}))
                    if defect == "continuation":
                        target = root / paths["swift"][0]
                        target.write_text("sendFrame()\nreturn response ?? empty\n")
                        report = json.loads(swift.read_text())
                        report["data"][0]["files"][0]["segments"] = [
                            [1, 1, 1, True, True, False],
                            [2, 20, 0, True, True, False],
                            [2, 25, 1, True, False, False],
                            [3, 1, 0, False, False, False],
                        ]
                        swift.write_text(json.dumps(report))
                    kotlin = root / "kotlin.xml"
                    kotlin.write_text('<report><package name="com/noma4i/dblayer">' + ''.join(
                        f'<sourcefile name="{Path(name).name}"><line nr="1" ci="{count}" mi="{1 - count}"/></sourcefile>'
                        for name, count in reports["kotlin"]) + '</package></report>')
                    result = subprocess.run(
                        ["bash", str(root / "scripts/check_coverage.sh")],
                        env={**os.environ, "COVERAGE_SWIFT_JSON": str(swift),
                             "COVERAGE_KOTLIN_XML": str(kotlin),
                             "COVERAGE_ALLOWLIST": str(allowlist)},
                        capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, expected, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
