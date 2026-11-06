#!/usr/bin/env bash
set -euo pipefail

root="${CHECK_ZOO_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)}"
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

lib_text = (source / "lib.rs").read_text() if (source / "lib.rs").exists() else ""
test_modules = set(re.findall(r"#\[cfg\(test\)\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;", lib_text))

def in_test_module(path):
    parts = path.relative_to(source).parts
    return parts[0] in test_modules or (len(parts) == 1 and parts[0][:-3] in test_modules)

files = [path for path in source.rglob("*.rs") if not path.name.endswith("_tests.rs") and not in_test_module(path)]
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

task_support = ("sync/stress/", "test_wait.rs", "transport/mock.rs", "/fixtures.rs", "/rig.rs", "/spec_fixtures.rs", "/invariants/")
spawn_forms = [
    re.compile(r"(?<![\w:.])(?:tokio::(?:task::)?)?spawn(?:_blocking|_local)?\s*\("),
    re.compile(r"\.\s*(?:spawn|spawn_blocking|spawn_local|spawn_on|spawn_blocking_on)\s*\((?!\s*(?:crate::task::)?Carry\b|\s*carry\b|\s*stream\.carry\(\))"),
    re.compile(r"(?<![\w])spawn_named\s*\((?!\s*(?:crate::task::)?Carry\b|\s*carry\b)"),
    re.compile(r"\bJoinSet\b|\bstd::thread::(?:Builder|spawn)\b|(?<![\w:])thread::(?:Builder|spawn)\b"),
]
for path in files:
    relative = str(path.relative_to(source))
    if relative == "task.rs" or any(marker in "/" + relative for marker in task_support):
        continue
    text = contents[path]
    code = code_only(text)
    for form in spawn_forms:
        for match in form.finditer(code):
            number = text.count("\n", 0, match.start()) + 1
            findings.append(f"Z19 a task boundary goes through crate::task, which carries the epoch: {path.relative_to(root)}:{number}: {match.group().strip()}")

writer_files = ("store/write.rs", "store/apply.rs", "store/reset.rs", "store/boot.rs", "store/open.rs")
write_methods = r"(?:execute|execute_batch|unchecked_transaction|transaction)\b"
connection_write = re.compile(r"\.connection\s*\.\s*" + write_methods + r"|\.connection\s*\.\s*prepare(?:_cached)?\([^;]*?\)\??\s*\.\s*execute\b")
connection_alias = re.compile(r"\blet\s+(?:mut\s+)?(\w+)\s*=\s*&(?:mut\s+)?[\w.]*\.connection\b")
read_callback = re.compile(r"\bread_locked\s*\(\s*(?:move\s*)?\|\s*(\w+)")
through_authority = re.compile(r"\bwrite_store\(|\bstore\s*\.\s*write\(|\bself\s*\.\s*write\(|\bcommit_as\(")
function_head = re.compile(r"\bfn\s+(\w+)\s*(?:<[^>]*>)?\(([^)]*)\)")
connection_param = re.compile(r"(\w+)\s*:\s*&(?:mut\s+)?(?:rusqlite::)?(?:Connection|Transaction)\b")

def chunks_of(code):
    offset = 0
    for chunk in re.split(r"(?=\bfn\s+\w+)", code):
        yield offset, chunk
        offset += len(chunk)

def authority_spans(chunk):
    spans = []
    for match in through_authority.finditer(chunk):
        spans.append((match.start(), end_of(chunk, match.end() - 1, "(", ")")))
    return spans

def receiver_write(name):
    return re.compile(r"(?<![\w.])" + name + r"\s*\.\s*" + write_methods + r"|(?<![\w.])" + name + r"\s*\.\s*prepare(?:_cached)?\([^;]*?\)\??\s*\.\s*execute\b")

z20_exempt = ("/fixtures.rs", "/spec_fixtures.rs", "/rig.rs")
writing_helpers = set()
for path in files:
    for _, chunk in chunks_of(code_only(contents[path])):
        head = function_head.search(chunk)
        if not head:
            continue
        for parameter in connection_param.findall(head.group(2)):
            if receiver_write(parameter).search(chunk[head.end():]):
                writing_helpers.add(head.group(1))
for path in files:
    relative = str(path.relative_to(source))
    if relative in writer_files or any(marker in "/" + relative for marker in z20_exempt):
        continue
    code = code_only(contents[path])
    for offset, chunk in chunks_of(code):
        head = function_head.search(chunk)
        name = head.group(1) if head else ""
        if f"z20:{relative}:{name}" in allowed:
            continue
        spans = authority_spans(chunk)
        aliases = set(connection_alias.findall(chunk)) | set(read_callback.findall(chunk))
        while True:
            copies = {copy for alias in aliases for copy in re.findall(r"\blet\s+(?:mut\s+)?(\w+)\s*=\s*&?(?:mut\s+)?(?:\*\s*)?" + alias + r"\s*;", chunk)} - aliases
            if not copies:
                break
            aliases |= copies
        writes = [match.start() for match in connection_write.finditer(chunk)]
        for alias in aliases:
            writes.extend(match.start() for match in receiver_write(alias).finditer(chunk))
        for helper in writing_helpers:
            if helper == name:
                continue
            argument = r"&(?:mut\s+)?(?:[\w.]*\.connection|" + "|".join(sorted(aliases) or ["(?!)"]) + r")\b"
            writes.extend(match.start() for match in re.finditer(r"(?<![\w.])" + helper + r"\s*\([^;]*?" + argument, chunk))
        for position in sorted(writes):
            if any(begin < position < finish for begin, finish in spans):
                continue
            number = code.count("\n", 0, offset + position) + 1
            findings.append(f"Z20 a SQL write goes through the Authority commit (Store::write or a commit envelope), inside its call: {path.relative_to(root)}:{number}")
            break

for path in [root / "core" / "Cargo.toml", root / "gen" / "Cargo.toml"]:
    if re.search(r"test-support", path.read_text()):
        findings.append(f"Z22 a Cargo feature that changes who may write is not allowed, tests use a real session or an Epoch: {path.relative_to(root)}")
for path in files:
    for match in re.finditer(r'feature\s*=\s*"test-support"', contents[path]):
        findings.append(f"Z22 a Cargo feature that changes who may write is not allowed: {path.relative_to(root)}:{contents[path].count(chr(10), 0, match.start()) + 1}")

dedicated_threads = ("db/lock.rs", "db/reads.rs", "db/listeners.rs", "db/ephemeral.rs", "db/dispatch.rs")
blocking_call = re.compile(r"\.(?:wait|wait_timeout|wait_while|wait_timeout_while|recv_timeout|blocking_recv)\(|\.recv\(\)|\bthread::sleep\(")

def spans_of(code, opener):
    return [(match.start(), end_of(code, match.end() - 1, "(", ")")) for match in re.finditer(opener, code)]

def test_regions(code):
    regions = []
    for match in re.finditer(r"#\[cfg\((?:any\()?test\b[^\]]*\]", code):
        cursor = match.end()
        while code[cursor:].lstrip().startswith("#["):
            cursor = code.index("]", code.index("#[", cursor)) + 1
        brace, semicolon = code.find("{", cursor), code.find(";", cursor)
        regions.append((match.start(), end_of(code, brace, "{", "}") if brace != -1 and (semicolon == -1 or brace < semicolon) else semicolon))
    return regions

for path in files:
    relative = str(path.relative_to(source))
    if relative in dedicated_threads or any(marker in "/" + relative for marker in task_support):
        continue
    code = code_only(contents[path])
    inside = spans_of(code, r"\bblocking_unit\s*\(") + test_regions(code)
    for match in blocking_call.finditer(code):
        if any(begin < match.start() < finish for begin, finish in inside):
            continue
        call_end = end_of(code, match.end() - 1, "(", ")") + 1 if match.group().endswith("(") else match.end()
        if code[call_end:].lstrip().startswith((".await", "=>")):
            continue
        number = code.count("\n", 0, match.start()) + 1
        findings.append(f"Z21 a blocking wait runs inside blocking_unit, so it never parks a worker of the shared executor: {path.relative_to(root)}:{number}: {contents[path].splitlines()[number - 1].strip()}")

test_only = ("/fixtures.rs", "/spec_fixtures.rs", "/rig.rs", "/stress/", "/transport/mock.rs", "/invariants/", "/bin/", "/test_wait.rs", "/ffi/selftest/")
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

glue = {line[len("wrapper-glue:"):] for line in allowed if line.startswith("wrapper-glue:")}
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
            ("Z15 application names do not belong in the library", r'"(?:myProfile|AuthSignIn|AuthConsumeLoginLink|Yup[i]|V[3]|Message|chat|gift)"'),
            ("Z16 change streams use the bounded core queue", r"buffer\(Channel\.UNLIMITED\)|buffered:\s*true"),
        ]
        if path.name not in glue:
            rules.append(("Z18 a wrapper holds no lock and no static registry: the core owns the state", r"\b(?:NSLock|NSRecursiveLock|OSAllocatedUnfairLock|Mutex|DispatchSemaphore|ReentrantLock|ReentrantReadWriteLock|synchronized|ConcurrentHashMap)\b|\.withLock\b|\bstatic\s+(?:let|var)\s+shared\b|nonisolated\(unsafe\)"))
        for label, expression in rules:
            for match in re.finditer(expression, text):
                number = text.count("\n", 0, match.start()) + 1
                findings.append(f"{label}: {relative}:{number}: {match.group()}")

admitted_argument = re.compile(r"\(\s*&?\s*(?:self\s*\.\s*)?(?:admitted|admission)\b")
network_call = re.compile(r"\b(?:transport|http|client)\s*\.\s*(?:query|mutate|subscribe|put)\s*\(")
for path in files:
    relative = str(path.relative_to(source))
    if any(marker in "/" + relative for marker in task_support) or relative.startswith("transport/encrypted/"):
        continue
    text = contents[path]
    code = code_only(text)
    for match in network_call.finditer(code):
        if admitted_argument.match(code[match.end() - 1:]) or f"z23:{relative}" in allowed:
            continue
        number = text.count("\n", 0, match.start()) + 1
        findings.append(f"Z23 a request goes to the network with an Admitted epoch permit: {path.relative_to(root)}:{number}: {text.splitlines()[number - 1].strip()}")

for path in files:
    relative = str(path.relative_to(source))
    if not relative.startswith("owners/") or any(marker in "/" + relative for marker in task_support):
        continue
    code = code_only(contents[path])
    hits = [match for match in re.finditer(r"\b(?:Mutex|RwLock)\b|\bparking_lot\b", code)]
    if hits and f"z24:{relative}" not in allowed:
        number = contents[path].count("\n", 0, hits[0].start()) + 1
        findings.append(f"Z24 an owner changes its state on its executor, no Mutex or RwLock: {path.relative_to(root)}:{number}: {len(hits)} use(s)")

json_parse = re.compile(r"\bserde_json::from_(?:str|slice)\b")
for relative in sorted(str(path.relative_to(source)) for path in files if str(path.relative_to(source)).startswith("ffi/")):
    path = source / relative
    if any(marker in "/" + relative for marker in test_only) or f"z25:{relative}" in allowed:
        continue
    code = code_only(contents[path])
    for offset, chunk in chunks_of(code):
        head = re.match(r"\s*fn\s+(\w+)", chunk)
        if not head:
            continue
        before = code[:offset]
        if not re.search(r"\bpub\s+(?:async\s+)?$", before[-20:]) and not re.search(r"#\[uniffi::export", before[-300:]):
            continue
        if f"z25:{relative}:{head.group(1)}" in allowed:
            continue
        call = json_parse.search(chunk)
        if call:
            number = code.count("\n", 0, offset + call.start()) + 1
            findings.append(f"Z25 a public call hands the string to its owner, it parses no JSON on the caller thread: {path.relative_to(root)}:{number}: {head.group(1)}")

clock_forms = re.compile(r"\btokio::time\b|\bInstant::now\b|\bSystemTime::now\b|\bUNIX_EPOCH\b")
for path in files:
    relative = str(path.relative_to(source))
    if relative.startswith(("pacing/", "bin/")) or any(marker in "/" + relative for marker in task_support):
        continue
    code = code_only(contents[path])
    inside = test_regions(code)
    for match in clock_forms.finditer(code):
        if any(begin < match.start() < finish for begin, finish in inside):
            continue
        number = contents[path].count("\n", 0, match.start()) + 1
        if f"z26:{relative}:{number}" in allowed or f"z26:{relative}" in allowed:
            continue
        findings.append(f"Z26 time goes through Clock: {path.relative_to(root)}:{number}: {match.group()}")

design = root / "specs" / "machines" / "design.md"
if design.exists():
    table = design.read_text()
    start = table.find("## 1a.")
    end = table.find("\n## ", start + 5)
    section = table[start:end if end != -1 else len(table)]
    constant = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?const\s+([A-Z][A-Z0-9_]*)\s*:\s*(?:u8|u16|u32|u64|usize|i32|i64|f64|Duration)\b", re.M)
    for path in files:
        relative = str(path.relative_to(source))
        if not (relative.startswith("owners/") or relative == "upload/config.rs") or any(marker in "/" + relative for marker in task_support):
            continue
        for name in constant.findall(contents[path]):
            if name not in section and f"z27:{name}" not in allowed:
                findings.append(f"Z27 an owner constant is listed in design.md section 1a with its file: {path.relative_to(root)}: {name}")

if findings:
    print("zoo check found", len(findings), "problem(s):")
    for finding in findings:
        print(" -", finding)
    sys.exit(1)
print("zoo check: clean")
PY
