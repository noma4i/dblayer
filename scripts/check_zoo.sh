#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python3 - "$root" <<'PY'
import re
import sys
from collections import defaultdict
from pathlib import Path

root = Path(sys.argv[1])
source = root / "core" / "src"
allowlist_path = root / "scripts" / "zoo_allowlist.txt"
allowed = set()
if allowlist_path.exists():
    for line in allowlist_path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            allowed.add(line)

files = [path for path in source.rglob("*.rs") if not path.name.endswith("_tests.rs")]
contents = {path: path.read_text() for path in files}
findings = []

definitions = defaultdict(list)
pattern = re.compile(r"^\s*pub (?:struct|enum|trait|type) ([A-Z][A-Za-z0-9]*)", re.M)
for path in files:
    for name in pattern.findall(contents[path]):
        definitions[name].append(str(path.relative_to(root)))
for name, places in sorted(definitions.items()):
    if len(places) > 1 and f"type:{name}" not in allowed:
        findings.append(f"Z2 duplicate public type {name}: {', '.join(sorted(places))}")

clocks = [str(path.relative_to(root)) for path in files if re.search(r"^\s*pub fn set_clock\b", contents[path], re.M)]
if len(clocks) > 1 and "set_clock" not in allowed:
    findings.append(f"Z1 several set_clock methods: {', '.join(sorted(clocks))}")

supports = sorted(str(path.relative_to(root)) for path in source.rglob("support.rs"))
if len(supports) > 1 and "support" not in allowed:
    findings.append(f"Z6 several support.rs files: {', '.join(supports)}")

diagnostics = []
for path in files:
    for name in re.findall(r"^\s*pub struct ([A-Za-z]*(?:Counters|Diagnostics|Stats))\b", contents[path], re.M):
        diagnostics.append(f"{name} ({path.relative_to(root)})")
if len(diagnostics) > 1 and "diagnostics" not in allowed:
    findings.append("Z1 several diagnostics structs: " + ", ".join(sorted(diagnostics)))

for path in files:
    relative = str(path.relative_to(root))
    if "/store/" in relative or relative.endswith(("db/commit.rs", "fixtures.rs")):
        continue
    if re.search(r"\.commit_with\(|\bstore\s*\.commit\(", contents[path]) and f"commit:{relative}" not in allowed:
        findings.append(f"Z7 commit outside db/commit.rs skips the parent pointer: {relative}")

for path in files:
    relative = str(path.relative_to(root))
    if relative.endswith("db/merge.rs") or "/schema/" in relative:
        continue
    if re.search(r"(?<!\bdb)(?<!\bself)\.merged\(", contents[path]):
        findings.append(f"Z8 server rows merge through Db::merged only: {relative}")

for path in files:
    relative = str(path.relative_to(root))
    if relative.endswith(("db/swap.rs", "commit/mod.rs")):
        continue
    if re.search(r"Replaced \{ by:", contents[path]):
        findings.append(f"Z9 a temp row is replaced in db/swap.rs only: {relative}")

for path in files:
    relative = str(path.relative_to(root))
    if "/sync/invalidate/" in relative:
        continue
    if re.search(r"\binvalidate_model\(", contents[path]):
        findings.append(f"Z10 lists are marked stale and reloaded in sync/invalidate only: {relative}")

effect = re.compile(r"\.\s*(notify_one|notify_waiters|send|send_replace|send_modify|send_if_modified|abort)\s*\(")
non_effects = {
    ("ffi/entity_machine.rs", "self", "send"),
    ("db/upload_round.rs", "self", "send"),
    ("sync/send/flush.rs", "self", "send"),
    ("sync/resume.rs", "inner.resume", "abort"),
    ("ffi/transport.rs", "self", "send"),
    ("transport/encrypted/cable/wire.rs", "socket", "send"),
}

def code_only(text):
    return re.sub(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"', lambda match: re.sub(r"[^\n]", " ", match.group()), text, flags=re.S)

def end_of(code, start, opening, closing):
    depth = 1
    for end in range(start + 1, len(code)):
        depth += (code[end] == opening) - (code[end] == closing)
        if depth == 0:
            return end
    return len(code)

foreign_traits = set()
for text in contents.values():
    foreign_traits.update(re.findall(r"#\[uniffi::export\([^\]]*foreign[^\]]*\)\](?:\s*#\[[^\]]*\])*\s*pub trait (\w+)", text))

for path in files:
    relative = str(path.relative_to(source))
    if relative.startswith("sync/stress/") or relative in ("transport/mock.rs", "test_wait.rs"):
        continue
    text = contents[path]
    code = code_only(text)
    checked = [(match.end(), end_of(code, match.end() - 1, "(", ")")) for match in re.finditer(r"\b(?:crate::db::|super::)?lock::effect\s*\(", code)]
    for match in effect.finditer(code):
        receiver = re.search(r"\w+(?:\s*\.\s*\w+)*\s*$", code[:match.start()])
        if receiver and (relative, re.sub(r"\s+", "", receiver.group()), match[1]) in non_effects:
            continue
        number = text.count("\n", 0, match.start()) + 1
        line = text.splitlines()[number - 1]
        if any(start <= match.start() < end for start, end in checked) or re.search(r"lock-ok:\s*\S", line):
            continue
        findings.append(f"Z17 unchecked effect: {path.relative_to(root)}:{number}: {line.strip()}")
    for match in re.finditer(r"\bstruct\s+(\w+)(?:<[^;{]*>)?\s*([({])", code):
        end = end_of(code, match.end() - 1, match[2], ")" if match[2] == "(" else "}")
        traits = set(re.findall(r"\b(?:Arc|Box)\s*<\s*dyn\s+(\w+)\b(?!\s*::)", code[match.end():end]))
        if relative in ("sync/mod.rs", "transport/encrypted/graphql.rs"):
            traits.discard("Transport")
        if not traits.intersection(foreign_traits):
            continue
        destructor = re.search(r"impl Drop for " + match[1] + r"\s*\{", code)
        body = code[destructor.end():end_of(code, destructor.end() - 1, "{", "}")] if destructor else ""
        if not re.search(r"\block::assert_no_library_lock\s*\(", body):
            number = text.count("\n", 0, match.start()) + 1
            findings.append(f"Z17 unchecked foreign destruction: {path.relative_to(root)}:{number}: {match[1]}")
    seen = False
    for number, line in enumerate(text.splitlines(), 1):
        if re.search(r"\bfn\s+\w+", line):
            seen = False
        if re.search(r"\b(?:locked|held)\(|\.lock\(", line):
            seen = True
        if seen and re.search(r"\.wake\(|\blistener\.changed\(", line) and not re.search(r"lock-ok:\s*\S", line):
            findings.append(f"Z17 review callback after guard: {path.relative_to(root)}:{number}: {line.strip()}")

test_only = ("/fixtures.rs", "/spec_fixtures.rs", "/rig.rs", "/stress/", "/transport/mock.rs", "/invariants/", "/bin/", "/test_wait.rs")
panic_call = re.compile(r"\b(?:expect|unwrap)\(|\b(?:panic|unreachable|todo|unimplemented)!\(")
panic_allowed = [entry[len("panic:"):] for entry in allowed if entry.startswith("panic:")]
for path in files:
    relative = str(path.relative_to(root))
    if any(marker in "/" + relative for marker in test_only):
        continue
    for number, line in enumerate(contents[path].splitlines(), 1):
        if relative == "core/src/db/lock.rs" and number < contents[path].splitlines().index("pub(crate) use debug::{assert_no_library_lock, holds, lock, lock_as, try_lock, wait, wait_timeout, wait_timeout_while, Guard};") + 1:
            continue
        calls = panic_call.findall(line)
        if not calls:
            continue
        covering = {fragment for line_file, fragment in map(lambda entry: entry.split(":", 1), panic_allowed) if line_file == relative and fragment in line}
        if len(calls) <= sum(len(panic_call.findall(fragment)) for fragment in covering):
            continue
        findings.append(f"Z11 a phone library never panics on the environment, return an error or a degraded answer: {relative}:{number}: {line.strip()}")

for directory, suffix, generated in [("swift/Sources/DBlayer", "*.swift", "dblayer.swift"), ("kotlin/src/main/kotlin/com/noma4i/dblayer", "*.kt", "dblayer.kt")]:
    for path in (root / directory).rglob(suffix):
        if path.name in (generated, "MockTransport.kt"):
            continue
        relative = str(path.relative_to(root))
        text = path.read_text()
        rules = [
            ("Z12 platform retries belong in core", r"\b(?:pow|sleep|delay)\s*\(|\b(?:TransportRetryPolicy|CableReconnect|SubscriptionRetry)\b"),
            ("Z13 removed platform transport", r"\b(?:CableClient|CableConnection|OperationRun|NetworkTransport|DatabaseTransport|GraphQLTransport|GraphQLTransportError|SubscriptionFailureKind|SubscriptionFailure|SubscriptionHandle|GraphQLError|PayloadCipher|PayloadEnvelope|RequestSigner|CanonicalJson|CanonicalJSON|EnvelopeWire|PublicOperations|UnauthorizedDecision)\b"),
            ("Z14 opening arguments are compared in core", r"\b(?:OpenArguments|AnyHashable)\b|(?:previous|opened|arguments)\s*==\s*arguments|\.matches\(arguments\)"),
            ("Z15 application names do not belong in the library", r'"(?:myProfile|AuthSignIn|AuthConsumeLoginLink|Yupi|V3|Message|chat|gift)"'),
            ("Z16 change streams use the bounded core queue", r"buffer\(Channel\.UNLIMITED\)|buffered:\s*true"),
        ]
        for label, expression in rules:
            for match in re.finditer(expression, text):
                number = text.count("\n", 0, match.start()) + 1
                findings.append(f"{label}: {relative}:{number}: {match.group()}")

if findings:
    print("zoo check found", len(findings), "problem(s):")
    for finding in findings:
        print(" -", finding)
    sys.exit(1)
print("zoo check: clean")
PY
