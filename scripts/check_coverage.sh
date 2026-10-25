#!/bin/bash
# INV-WAIT-05: a production line of the cable code that no test runs is a defect or dead code.
# Reads the coverage of the test runs the lane already did: swift test --enable-code-coverage with atomic counters (-Xswiftc -Xllvm -Xswiftc -instrprof-atomic-counter-update-all), and the AGP unit test coverage report of testDebugUnitTest.
# Lines that stay at 0 executions are listed in scripts/coverage_allowlist.txt (file, count, line text, owner, reason).
# A new never-executed line fails. An allowed line that a test now runs fails too, so the list only shrinks.
# check_coverage.sh --write-baseline prints the allowlist for the current coverage. The owner and the reason of an entry come
# from the first rule of scripts/coverage_reasons.txt (file, Type.function pattern, owner, reason) that matches the scope of the line:
# a scope without a rule stops the write. Use it only in a reviewed change.
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
mode="${1:-check}"
swift_json="${COVERAGE_SWIFT_JSON:-$(swift test --package-path "$root/swift" --show-codecov-path)}"
kotlin_xml="${COVERAGE_KOTLIN_XML:-$root/kotlin/build/reports/coverage/test/debug/report.xml}"
allowlist="${COVERAGE_ALLOWLIST:-$root/scripts/coverage_allowlist.txt}"
reasons="${COVERAGE_REASONS:-$root/scripts/coverage_reasons.txt}"

for report in "$swift_json" "$kotlin_xml"; do
  if [ ! -f "$report" ]; then
    echo "check_coverage: $report is missing, run the swift and the gradle test lanes with coverage first" >&2
    exit 2
  fi
done

exec python3 -I - "$mode" "$root" "$swift_json" "$kotlin_xml" "$allowlist" "$reasons" <<'PY'
import collections, json, re, sys
import xml.etree.ElementTree as ET
from pathlib import Path

mode, root, swift_json, kotlin_xml, allowlist, reasons = sys.argv[1:7]
kotlin_dir = f"{root}/kotlin/src/main/kotlin/com/noma4i/dblayer"
missed = collections.defaultdict(list)
swift_dir = Path(root) / "swift/Sources/DBlayer"
swift_sources = set((swift_dir / "Network").glob("*.swift")) | {swift_dir / "UrlSessionTransport.swift", swift_dir / "UrlSessionCable.swift"}
swift_sources.discard(swift_dir / "Network/GraphQLOperationKind.swift")
kotlin_sources = set(Path(kotlin_dir).glob("*Encrypted*.kt")) | {Path(kotlin_dir) / "TransportFailure.kt", Path(kotlin_dir) / "Transports.kt"}
seen = set()

TYPE = re.compile(r"^(?:[a-z@A-Z]+ )*(?:class|struct|enum|actor|extension|object|interface)\s+([A-Za-z0-9_.]+)")
FUNCTION = re.compile(r"^\s+(?:[a-z@A-Z()]+ )*(?:func|fun)\s+([A-Za-z0-9_`]+)|^\s+(?:public |private |internal )?(init)\(")

def source_lines(path):
    return open(path).read().split("\n")

def scope_of(lines, number):
    function = type_name = "?"
    for index in range(number - 1, -1, -1):
        if function == "?" and (m := FUNCTION.match(lines[index])):
            function = m.group(1) or m.group(2)
        if m := TYPE.match(lines[index]):
            type_name = m.group(1)
            break
    return f"{type_name}.{function}"

def add(short, path, number):
    lines = source_lines(path)
    text = lines[number - 1].strip()
    if text not in ("", "{", "}", "})"):
        missed[(short, text)].append((number, scope_of(lines, number)))

report = json.load(open(swift_json))["data"][0]
for entry in report["files"]:
    name = str(Path(entry["filename"]).resolve())
    if Path(name) not in swift_sources:
        continue
    if any(segment[3] for segment in entry["segments"]):
        seen.add(Path(name))
    starting = collections.defaultdict(list)
    for line, _, count, has_count, is_entry, is_gap in entry["segments"]:
        starting[line].append((count, has_count, is_entry, is_gap))
    wrapped = None
    for number in range(1, max(starting, default=0) + 1):
        entries = [count for count, has_count, is_entry, is_gap in starting.get(number, []) if has_count and is_entry and not is_gap]
        counts = entries + ([wrapped] if wrapped is not None else [])
        line_count = max(counts) if counts else None
        if line_count == 0:
            add(name.split("/Sources/DBlayer/")[1], name, number)
        if starting.get(number):
            last = starting[number][-1]
            wrapped = last[0] if last[1] else None

for package in ET.parse(kotlin_xml).getroot().iter("package"):
    if package.get("name") != "com/noma4i/dblayer":
        continue
    for source in package.iter("sourcefile"):
        path = Path(kotlin_dir) / source.get("name")
        if path not in kotlin_sources:
            continue
        if any(int(line.get("ci")) + int(line.get("mi")) > 0 for line in source.iter("line")):
            seen.add(path)
        for line in source.iter("line"):
            if int(line.get("ci")) == 0 and int(line.get("mi")) > 0:
                add(source.get("name"), f"{kotlin_dir}/{source.get('name')}", int(line.get("nr")))

missing = (swift_sources | kotlin_sources) - seen
if missing:
    for path in sorted(missing):
        print(f"check_coverage: no executable coverage: {path.relative_to(root)}", file=sys.stderr)
    sys.exit(1)

if mode == "--write-baseline":
    rules = []
    for rule in open(reasons):
        if rule.startswith("#") or not rule.strip():
            continue
        name, pattern, owner, reason = rule.rstrip("\n").split("\t")
        rules.append((name, re.compile(pattern), owner, reason))
    print("# file<TAB>count<TAB>line text<TAB>owner<TAB>reason. Generated by check_coverage.sh --write-baseline from coverage_reasons.txt.")
    unmatched = set()
    for (name, text), places in sorted(missed.items()):
        found = None
        for _, scope in places:
            found = next(((o, r) for n, p, o, r in rules if n == name and p.search(scope)), None)
            if found:
                break
            unmatched.add((name, scope))
        if found:
            print(f"{name}\t{len(places)}\t{text}\t{found[0]}\t{found[1]} ({places[0][1]})")
    for name, scope in sorted(unmatched):
        print(f"check_coverage: no reason rule for {name} {scope}", file=sys.stderr)
    sys.exit(1 if unmatched else 0)

allowed = {}
for entry in open(allowlist):
    if entry.startswith("#") or not entry.strip():
        continue
    name, count, text = entry.rstrip("\n").split("\t")[:3]
    allowed[(name, text)] = int(count)

failed = False
for key, places in sorted(missed.items()):
    extra = len(places) - allowed.get(key, 0)
    if extra > 0:
        failed = True
        for number, _ in sorted(places)[-extra:]:
            print(f"check_coverage: never executed: {key[0]}:{number}: {key[1]}", file=sys.stderr)
for key, count in sorted(allowed.items()):
    if len(missed.get(key, [])) < count:
        failed = True
        print(f"check_coverage: a test runs it now, remove it from the allowlist: {key[0]}: {key[1]}", file=sys.stderr)
if failed:
    sys.exit(1)
print("check_coverage: ok")
PY
