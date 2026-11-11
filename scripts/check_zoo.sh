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
    if parts == ("ffi", "tape.rs") or "tests" in parts[:-1] or any(part.endswith(("_tests", "_tests.rs")) for part in parts):
        return True
    return parts[0] in test_modules or (len(parts) == 1 and parts[0][:-3] in test_modules)

files = [path for path in source.rglob("*.rs") if not in_test_module(path)]
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
    if relative.endswith("store/merge.rs") or "/schema/" in relative:
        continue
    if re.search(r"(?<!\bdb)(?<!\bself)\.merged\(", contents[path]):
        findings.append(f"Z8 server rows merge through store/merge.rs only: {relative}")

for path in files:
    relative = str(path.relative_to(root))
    if relative.endswith(("store/swap.rs", "store/batch_plan.rs")):
        continue
    if re.search(r"Replaced \{ by:", contents[path]):
        findings.append(f"Z9 a temp row is replaced in core/src/store/swap.rs only: {relative}")

effect = re.compile(r"\.\s*(notify_one|notify_waiters|send|send_replace|send_modify|send_if_modified|abort)\s*\(")
non_effects = {
    ("ffi/transport/mod.rs", "self", "send"),
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
    if relative in ("transport/mock.rs", "test_wait.rs"):
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
        if relative == "transport/encrypted/graphql.rs":
            traits.discard("Transport")
        if not traits.intersection(foreign_traits):
            continue
        if re.search(r"#\[derive\([^\]]*uniffi::Record[^\]]*\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?$", code[:match.start()]):
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

task_support = ("test_wait.rs", "transport/mock.rs", "/fixtures.rs", "/rig.rs", "/spec_fixtures.rs", "/invariants/")
spawn_forms = [
    re.compile(r"(?<![\w:.])(?:tokio::(?:task::)?)?spawn(?:_blocking|_local)?\s*\("),
    re.compile(r"\.\s*(?:spawn|spawn_blocking|spawn_local|spawn_on|spawn_blocking_on)\s*\("),
    re.compile(r"(?<![\w])spawn_named\s*\("),
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

uncounted_blocking = re.compile(r"\btask::spawn_blocking(?:_on)?\s*\(\s*(?:[^,()]*(?:\([^()]*\))?[^,()]*,\s*)?None\b")
for path in files:
    relative = str(path.relative_to(source))
    if relative == "task.rs" or f"z19:{relative}" in allowed:
        continue
    code = code_only(contents[path])
    for match in uncounted_blocking.finditer(code):
        number = code.count("\n", 0, match.start()) + 1
        findings.append(f"Z19 a blocking job is counted in the Inflight of the owners until it ends (a launch goes to crate::task::spawn_blocking*): {path.relative_to(root)}:{number}")

write_methods = r"(?:execute|execute_batch|unchecked_transaction|transaction)\b"
connection_write = re.compile(r"\.connection\s*\.\s*" + write_methods + r"|\.connection\s*\.\s*prepare(?:_cached)?\([^;]*?\)\??\s*\.\s*execute\b")
connection_alias = re.compile(r"\blet\s+(?:mut\s+)?(\w+)\s*=\s*&(?:mut\s+)?[\w.]*\.connection\b")
read_callback = re.compile(r"\bread_locked\s*\(\s*(?:move\s*)?\|\s*(\w+)")
through_authority = re.compile(r"\bstore\s*\.\s*write\(|\bself\s*\.\s*write\(|\.commit\(")
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
    if relative.startswith("store/") or any(marker in "/" + relative for marker in z20_exempt):
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
    if any(marker in "/" + relative for marker in task_support):
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

test_only = ("/fixtures.rs", "/spec_fixtures.rs", "/rig.rs", "/transport/mock.rs", "/invariants/", "/bin/", "/test_wait.rs", "/test_matrix.rs", "/ffi/selftest/")
panic_call = re.compile(r"\b(?:expect|unwrap)\(|\b(?:panic|unreachable|todo|unimplemented)!\(")
panic_allowed = [entry[len("panic:"):] for entry in allowed if entry.startswith("panic:")]
for path in files:
    relative = str(path.relative_to(root))
    if any(marker in "/" + relative for marker in test_only):
        continue
    for number, line in enumerate(contents[path].splitlines(), 1):
        calls = panic_call.findall(line)
        if not calls:
            continue
        covering = {fragment for line_file, fragment in map(lambda entry: entry.split(":", 1), panic_allowed) if line_file == relative and fragment in line}
        if len(calls) <= sum(len(panic_call.findall(fragment)) for fragment in covering):
            continue
        findings.append(f"Z11 a phone library never panics on the environment, return an error or a degraded answer: {relative}:{number}: {line.strip()}")

z18_entries = [entry[len("z18:"):].split(":", 1) for entry in allowed if entry.startswith("z18:")]

def z18_allowed(relative, line):
    return any(len(entry) == 2 and entry[0] == relative and entry[1] in line for entry in z18_entries)

for directory, suffix, generated in [("swift/Sources/DBlayer", "*.swift", "dblayer.swift"), ("kotlin/src/main/kotlin/com/noma4i/dblayer", "*.kt", "dblayer.kt")]:
    for path in (root / directory).rglob(suffix):
        if path.name == generated:
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
        rules.append(("Z18 a wrapper holds no lock and no static registry: the core owns the state", r"\b(?:NSLock|NSRecursiveLock|OSAllocatedUnfairLock|Mutex|DispatchSemaphore|ReentrantLock|ReentrantReadWriteLock|synchronized|ConcurrentHashMap)\b|\.withLock\b|\bstatic\s+(?:let|var)\s+shared\b|nonisolated\(unsafe\)"))
        global_state = "Z18 a wrapper holds no global state: the core owns it"
        if suffix == "*.swift":
            rules.append((global_state, r"\bstatic\s+(?:nonisolated\(unsafe\)\s+)?var\s+\w+\s*(?::[^={\n]*)?(?:=|$)"))
            rules.append((global_state, r"\bstatic\s+let\s+\w+[^=\n]*=\s*(?:\[|Dictionary\b|Array\b|Set\b|NSMutable\w*|Atomic\b|ManagedAtomic\b)"))
        else:
            rules.append((global_state, r"@Volatile\b"))
            for body in re.finditer(r"^( *)(?:\w+ +)*object\s+\w+[^{\n]*\{", text, re.M):
                inside = text[body.end():end_of(text, body.end() - 1, "{", "}")]
                for member in re.finditer(r"^" + body[1] + r" {4}(?:(?:private|internal|public|lateinit)\s+)*var\b[^\n]*", inside, re.M):
                    number = text.count("\n", 0, body.end()) + inside.count("\n", 0, member.start()) + 1
                    if not z18_allowed(relative, member.group()):
                        findings.append(f"{global_state}: {relative}:{number}: {member.group().strip()}")
        for label, expression in rules:
            for match in re.finditer(expression, text, re.M):
                number = text.count("\n", 0, match.start()) + 1
                if label == global_state and z18_allowed(relative, text.splitlines()[number - 1]):
                    continue
                findings.append(f"{label}: {relative}:{number}: {match.group()}")

network_call = re.compile(r"\b(?:transport|http|client|lines)\s*\.\s*(?:query|mutate|subscribe|put|connect)\s*\(")
sending = re.compile(r"\bsend_in_epoch\s*\(")
kind_from_text = re.compile(r'starts_with\(\s*"(?:mutation|query|subscription)\b')
for path in files:
    relative = str(path.relative_to(source))
    if any(marker in "/" + relative for marker in task_support) or relative.startswith("runtime/gate"):
        continue
    text = contents[path]
    code = code_only(text)
    inside = test_regions(code)
    spans = []
    for match in sending.finditer(code):
        finish = end_of(code, match.end() - 1, "(", ")")
        argument = code[match.end():finish]
        if not re.search(r"\|\s*\|\s*[^,]*$", argument.split(",", 2)[-1] if argument.count(",") >= 2 else argument) and not re.search(r",\s*\|\s*\|", argument):
            number = text.count("\n", 0, match.start()) + 1
            findings.append(f"Z23 the request of send_in_epoch is a closure that calls the transport after the gate check: {path.relative_to(root)}:{number}")
        spans.append((match.end(), finish))
    for match in kind_from_text.finditer(text):
        if code[match.start():match.start() + 11] != "starts_with" or any(begin < match.start() < finish for begin, finish in inside) or f"z23:{relative}" in allowed:
            continue
        number = text.count("\n", 0, match.start()) + 1
        findings.append(f"Z23 the kind of a request comes from the input of its owner, not from the text of the document: {path.relative_to(root)}:{number}")
    for match in network_call.finditer(code):
        if any(begin <= match.start() < finish for begin, finish in spans) or any(begin < match.start() < finish for begin, finish in inside) or f"z23:{relative}" in allowed:
            continue
        number = text.count("\n", 0, match.start()) + 1
        findings.append(f"Z23 a request goes to the network through send_in_epoch: {path.relative_to(root)}:{number}: {text.splitlines()[number - 1].strip()}")

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
    if relative in ("runtime/clock.rs", "runtime/test_clock.rs") or relative.startswith("bin/") or any(marker in "/" + relative for marker in task_support):
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
        if not ((relative.startswith("owners/") and not relative.startswith("owners/write/upload/")) or relative == "owners/write/upload/config.rs") or any(marker in "/" + relative for marker in task_support):
            continue
        for name in constant.findall(contents[path]):
            if name not in section and f"z27:{name}" not in allowed:
                findings.append(f"Z27 an owner constant is listed in design.md section 1a with its file: {path.relative_to(root)}: {name}")

for path in files:
    relative = str(path.relative_to(source))
    if relative == "task.rs":
        continue
    for number, line in enumerate(contents[path].splitlines(), 1):
        suppressed = re.search(r"#!?\[allow\(([^)]*)\)\]", line)
        if suppressed and f"z29:{relative}:{suppressed.group(1)}" not in allowed:
            findings.append(f"Z29 a lint is fixed at its cause, not suppressed (spawn lives in task.rs): {path.relative_to(root)}:{number}: {suppressed.group(0)}")

generated_tool = [path for path in (root / "gen" / "src").rglob("*.rs") if "tests" not in path.relative_to(root / "gen" / "src").parts and not path.name.endswith("_tests.rs")]
for path in generated_tool:
    for number, line in enumerate(path.read_text().splitlines(), 1):
        suppressed = re.search(r"#!?\[allow\(([^)]*)\)\]", line)
        if suppressed and f"z29:{path.relative_to(root)}:{suppressed.group(1)}" not in allowed:
            findings.append(f"Z29 a lint is fixed at its cause, not suppressed (gen): {path.relative_to(root)}:{number}: {suppressed.group(0)}")

dead_names = re.compile(r'"((?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.(?:rs|kt|swift))"')
rule_text = (root / "scripts" / "check_zoo.sh").read_text() if (root / "scripts" / "check_zoo.sh").exists() else ""
everything = [str(path.relative_to(source)) for path in source.rglob("*.rs")]
for name in sorted(set(dead_names.findall(rule_text))):
    if name.startswith("_") or name in ("dblayer.kt", "dblayer.swift", "main.rs") or any(listed == name or listed.endswith("/" + name) for listed in everything) or (root / name).exists():
        continue
    findings.append(f"Z39 a zoo rule names a file that does not exist, so it guards nothing: {name}")

before_open = {"ffi/encrypted.rs", "ffi/transport/mod.rs", "transport/online.rs", "transport/mod.rs", "transport/retry.rs", "transport/encrypted/bootstrap.rs"}
for path in files:
    relative = str(path.relative_to(source))
    if not (relative.startswith(("ffi/", "owners/", "store/", "runtime/", "transport/"))) or relative in before_open:
        continue
    for number, line in enumerate(contents[path].splitlines(), 1):
        if "connectivity" in line.lower():
            findings.append(f"Z30 after open online has one source, the Network owner (P-NET-08): {path.relative_to(root)}:{number}: {line.strip()}")

test_decl = re.compile(r"^(pub(\(crate\))? )?mod (\w+);\s*$")
test_directories = {"spec", "selftest", "check", "schema_fixtures", "test_wait", "test_matrix", "mock", "test_clock", "screens_fixture", "tape", "fixtures", "rig"}
for path in sorted(root.rglob("*.rs")):
    if any(part.startswith(("target", ".")) for part in path.relative_to(root).parts):
        continue
    parts = path.relative_to(root).parts
    lines = path.read_text().splitlines()
    if path.name.endswith("_tests.rs") and "src" in parts and not any(part in ("tests", "spec", "selftest", "check") for part in parts[:-1]):
        findings.append(f"Z32 a test file lives in the tests/ directory of its code directory: {path.relative_to(root)}")
    for number, line in enumerate(lines, 1):
        match = test_decl.match(line)
        if not match:
            continue
        name = match.group(3)
        previous = lines[number - 2].strip() if number > 1 else ""
        if previous.startswith("#[path"):
            findings.append(f"Z32 a module is declared without #[path]: {path.relative_to(root)}:{number}")
        if path.name not in ("mod.rs", "lib.rs", "main.rs") and "src" in parts:
            findings.append(f"Z32 a module with children is a directory with mod.rs: {path.relative_to(root)}:{number}")
        if previous == "#[cfg(test)]" and name != "tests" and name not in test_directories:
            findings.append(f"Z32 #[cfg(test)] stands only on mod tests; and on a whole test directory: {path.relative_to(root)}:{number}")
        if name == "tests" and not (path.parent / "tests" / "mod.rs").exists():
            findings.append(f"Z32 mod tests; needs tests/mod.rs: {path.relative_to(root)}:{number}")
        if name == "tests" and previous != "#[cfg(test)]":
            findings.append(f"Z32 mod tests; is declared under #[cfg(test)]: {path.relative_to(root)}:{number}")

poll_words = re.compile(r"\b(yield_until(?:_showing)?|spin_until)\b|\byield_now\s*\(|\bthread::sleep\s*\(")
for path in sorted((root / "core" / "src").rglob("*.rs")):
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if (poll_words.search(line) or re.search(r"\b(while|loop)\b.*\bsleep\s*\(", line)) and f"z33:{path.relative_to(root / 'core' / 'src')}" not in allowed:
            findings.append(f"Z33 a wait polls: wait for the barrier (settled) or the signal of the party (signalled), not a loop: {path.relative_to(root)}:{number}")

stand_write = re.compile(r"\bBatchPlan::from_server\b|\b\w+_for_tests\s*\(|\bStoreInput::|\.to_store\(|\.to_windows\(|\bto_feed\(")
for stand in ("ffi/selftest", "ffi/tests"):
    for path in sorted((source / stand).rglob("*.rs")):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if stand_write.search(line) and f"z35:{path.relative_to(source)}" not in allowed:
                findings.append(f"Z35 the public test stand lands server rows and stale marks through the senders (cable, answer, mutation), it imports no Store write or owner send: {path.relative_to(root)}:{number}")

pair_builders = {"ffi/write.rs", "ffi/upload.rs", "owners/life/calls_runner.rs", "transport/encrypted/graphql.rs"}
for path in files:
    relative = str(path.relative_to(source))
    text = contents[path]
    if relative.startswith(("ffi/selftest/", "server_contract/")): continue
    if "graphql_body(" in text and relative not in pair_builders:
        findings.append(f"Z36 a body of a sequenced request is built only by the listed builders (graphql_body, full document): {path.relative_to(root)}")
    if ("persistedQuery" in text or "hash_only(" in text) and relative not in {"transport/encrypted/graphql.rs", "owners/session/flow.rs"}:
        findings.append(f"Z36 the hash-only body of a pair is made only by Session pair_bodies from the full body: {path.relative_to(root)}")

double_impl = re.compile(r"\bimpl(?:<[^>]*>)? (?:FakeHttp|HttpExchange|Upstream|FakeTransport|Transport) for\b")
double_rules = re.compile(r'"PERSISTED_QUERY_NOT_FOUND"|"SEQUENCE_CONFLICT"|(?<![.\w])(?:sequence|last) \+ 1\b|\bsettled\s*:')
for path in sorted(source.rglob("*.rs")):
    relative = str(path.relative_to(source))
    if relative.startswith("server_contract/") or not in_test_module(path): continue
    text = path.read_text()
    if not double_impl.search(text): continue
    for number, line in enumerate(text.splitlines(), 1):
        if double_rules.search(line) and f"z37:{relative}" not in allowed:
            findings.append(f"Z37 a double of the server keeps no pair memory, sequence rule or document registry of its own: it asks server_contract: {path.relative_to(root)}:{number}")

contract_users = {"lib.rs", "ffi/mod.rs", "ffi/contract.rs", "transport/mock.rs"}
for path in files:
    relative = str(path.relative_to(source))
    if relative.startswith(("server_contract/", "ffi/selftest/")) or relative in contract_users: continue
    if re.search(r"\bserver_contract\b|\bServerContract\b", contents[path]) and f"z38:{relative}" not in allowed:
        findings.append(f"Z38 the server contract of the test doubles is reached only from its export (ffi/contract.rs), tests and the platform doubles, no production path calls it: {path.relative_to(root)}")

for path in files:
    relative = str(path.relative_to(source))
    if relative in ("ffi/write.rs", "store/plan/write.rs"): continue
    if re.search(r"\bplan_update\(", contents[path]) and f"z40:{relative}" not in allowed:
        findings.append(f"Z40 values that carry local fields are planned by plan_supplied (the local part goes to the same transaction), plan_update drops them: {path.relative_to(root)}")

wire_readers = {"transport/answer_kind.rs", "transport/encrypted/graphql.rs", "transport/encrypted/bootstrap.rs", "owners/write/answer.rs", "transport/session_code.rs", "transport/error.rs", "store/session_state.rs", "ffi/encrypted.rs"}
for path in files:
    relative = str(path.relative_to(source))
    if relative in wire_readers: continue
    if re.search(r'SessionCode::(?:from_wire|of|named)\(|get\("error"\)|\["error"\]\.as_str', contents[path]) and f"z41:{relative}" not in allowed:
        findings.append(f"Z41 the code of a server answer is read by transport/answer_kind.rs (refusal, classify), not by a second parser: {path.relative_to(root)}")

clock_inputs = {"OutboxInput": "Tick", "TransferInput": "Clock", "RoundsInput": "Clock", "WindowsInput": "Tick", "DeliveryInput": "Tick"}
clock_used = re.compile(r"\b(?:" + "|".join(f"{name}::{variant}" for name, variant in clock_inputs.items()) + r")\b")

def enum_body(text, name):
    found = re.search(rf"\benum {name}\b[^{{]*{{", text)
    if not found: return ""
    depth, start = 1, found.end()
    for index in range(start, len(text)):
        depth += {"{": 1, "}": -1}.get(text[index], 0)
        if depth == 0: return text[start:index]
    return text[start:]

owner_trait = source / "runtime" / "owner.rs"
if owner_trait.exists() and re.search(r"\bfn tick\(", enum_body(owner_trait.read_text().replace("pub trait StateOwner", "enum StateOwner"), "StateOwner")):
    findings.append(f"Z42 the time of an owner is the argument of its step, not an input or a trait function: {owner_trait.relative_to(root)}")
for path in files:
    relative = str(path.relative_to(source))
    text = contents[path]
    declared = any(re.search(rf"(?:^|[,{{}}])\s*{variant}\s*[(,{{}}]", enum_body(text, name)) for name, variant in clock_inputs.items())
    if (declared or clock_used.search(text)) and f"z42:{relative}" not in allowed:
        findings.append(f"Z42 the time of an owner is the argument of its step, not an input or a trait function: {path.relative_to(root)}")

for path in files:
    relative = str(path.relative_to(source))
    for number, line in enumerate(contents[path].splitlines(), 1):
        if re.search(r"\bopened_at\b", line) and f"z43:{relative}" not in allowed:
            findings.append(f"Z43 a window takes the time of its step, a stream keeps no time of its open for the loads it asks: {path.relative_to(root)}:{number}")

windows_types = source / "owners" / "read" / "windows" / "windows_types.rs"
if windows_types.exists():
    text = windows_types.read_text()
    context = re.search(r"\bstruct Ctx\b[^{]*\{[^}]*\}", text)
    for found in re.finditer(r"\bnow: i64\b", text):
        if context and context.start() <= found.start() < context.end(): continue
        number = text.count("\n", 0, found.start()) + 1
        findings.append(f"Z44 an input or an event of Windows carries no time: the owner reads the time of its step (Ctx.now): {windows_types.relative_to(root)}:{number}")

for path in files:
    relative = str(path.relative_to(source))
    if not relative.startswith("owners/read/windows/"): continue
    for number, line in enumerate(contents[path].splitlines(), 1):
        if re.search(r"\bread_id: Option<", line):
            findings.append(f"Z45 every window machine opens by reading its saved window (ensure), none is made without the read: {path.relative_to(root)}:{number}")

for target, pattern in ((source / "runtime" / "owner.rs", r"\bfn due\("), (source / "owners" / "read" / "feed" / "mod.rs", r"\bnow: Time\b")):
    if not target.exists(): continue
    for number, line in enumerate(target.read_text().splitlines(), 1):
        if re.search(pattern, line):
            findings.append(f"Z46 Feed takes the time of its step, no input of Feed carries a time and no owner rewrites a due input: {target.relative_to(root)}:{number}")

deliveries = source / "owners" / "read" / "deliveries" / "mod.rs"
if deliveries.exists():
    text = deliveries.read_text()
    body = enum_body(text, "DeliveryInput")
    start = text.find(body) if body else -1
    for found in re.finditer(r"\bnow:", body):
        number = text.count("\n", 0, start + found.start()) + 1
        findings.append(f"Z47 an input of Deliveries carries no time: the owner reads the time of its step: {deliveries.relative_to(root)}:{number}")

write = source / "ffi" / "write.rs"
if write.exists():
    for number, line in enumerate(write.read_text().splitlines(), 1):
        if re.search(r"life\.now\(\)", line):
            findings.append(f"Z48 a write call reads no clock: Outbox and LocalWrites take the time of their step: {write.relative_to(root)}:{number}")
outbox_types = source / "owners" / "write" / "outbox_types.rs"
if outbox_types.exists():
    for number, line in enumerate(outbox_types.read_text().splitlines(), 1):
        if re.search(r"\bLocalWrite \{[^}]*\bnow:", line):
            findings.append(f"Z48 LocalWrite carries no time: the owner reads the time of its step: {outbox_types.relative_to(root)}:{number}")

upload = source / "ffi" / "upload.rs"
if upload.exists():
    for number, line in enumerate(upload.read_text().splitlines(), 1):
        if re.search(r"life\.now\(\)", line):
            findings.append(f"Z49 an upload call reads no clock: Transfer takes the time of its step: {upload.relative_to(root)}:{number}")
transfer_types = source / "owners" / "write" / "transfer_types.rs"
if transfer_types.exists():
    for number, line in enumerate(transfer_types.read_text().splitlines(), 1):
        if re.search(r"^\s*(Accept|AcceptBatch|Tick) \{[^}]*\bnow:", line):
            findings.append(f"Z49 an input of Transfer carries no time: the owner reads the time of its step: {transfer_types.relative_to(root)}:{number}")
for path in sorted(source.rglob("*.rs")):
    if "tests" in path.parts or path.name.endswith("_tests.rs") or "spec" in path.relative_to(source).parts[:1]: continue
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if re.search(r"wiring\.now\b", line):
            findings.append(f"Z49 the time of an owner comes from the runtime step, the wiring holds no clock: {path.relative_to(root)}:{number}")

session_inputs = source / "owners" / "session" / "mod.rs"
if session_inputs.exists():
    text = session_inputs.read_text()
    body = enum_body(text, "SessionInput")
    start = text.find(body) if body else -1
    for found in re.finditer(r"\bTime\b", body):
        number = text.count("\n", 0, start + found.start()) + 1
        findings.append(f"Z50 an input of Session carries no time: the owner reads the time of its step: {session_inputs.relative_to(root)}:{number}")

for path in sorted((source / "owners").rglob("*.rs")):
    if "tests" in path.parts or path.name.endswith("_tests.rs"): continue
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if re.search(r"\b(set_phase|failed_phase)\b", line):
            findings.append(f"Z51 the phase of a window is a function of the stage, the outcome and the rows, no code writes it: {path.relative_to(root)}:{number}")
window_facts = source / "owners" / "read" / "windows" / "windows_facts.rs"
if window_facts.exists():
    text = window_facts.read_text()
    start = text.find("pub(super) struct Facts {")
    body = text[start:text.find("\n}\n", start)] if start >= 0 else ""
    for found in re.finditer(r"\bphase:", body):
        number = text.count("\n", 0, start + found.start()) + 1
        findings.append(f"Z51 the window keeps no phase field: the value computes it: {window_facts.relative_to(root)}:{number}")

validate_calls = []
for path in sorted((source / "store").rglob("*.rs")):
    if "tests" in path.parts or path.name.endswith("_tests.rs"): continue
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if re.search(r"\.validate\(", line):
            validate_calls.append(f"{path.relative_to(root)}:{number}")
if len(validate_calls) != 1:
    for place in validate_calls:
        findings.append(f"Z52 the batch plan has one check, in the one write entry of Store after the follow operations and the reduced repeats ({len(validate_calls)} calls): {place}")

for path in sorted(source.rglob("*.rs")):
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if re.search(r"\bwindow_phase\b", line):
            findings.append(f"Z53 the stream keeps the window value as the window sent it, no second phase field: {path.relative_to(root)}:{number}")
for path in sorted((source / "owners" / "read" / "deliveries").rglob("*.rs")):
    if "tests" in path.parts or path.name.endswith("_tests.rs"): continue
    inside = False
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if re.match(r"\s*pub\(super\) fn delivered\(", line): inside = True
        elif inside and re.match(r"    \}$", line): inside = False
        if not inside and re.search(r"\bWindowState \{ (phase|error|loaded|has_more|settled|total)\b", line):
            findings.append(f"Z53 the state of a stream value is built only in Delivery::delivered (P-DLV-32): {path.relative_to(root)}:{number}")

for path in sorted((source / "owners" / "write").rglob("*.rs")):
    if "tests" in path.parts or path.name.endswith("_tests.rs"): continue
    text = path.read_text()
    for number, line in enumerate(text.splitlines(), 1):
        if re.search(r"\bTime\(0\)", line):
            findings.append(f"Z54 the time of the turn is the argument now of the step, no floor: {path.relative_to(root)}:{number}")
    for name in ("OutboxState", "TransferState"):
        start = text.find(f"pub struct {name} {{")
        if start < 0: continue
        body = text[start:text.find("\n}\n", start)]
        for found in re.finditer(r"^\s*pub(\([a-z]+\))? now:", body, re.M):
            number = text.count("\n", 0, start + found.start()) + 2
            findings.append(f"Z54 the state keeps no copy of the time of the turn (R5): {path.relative_to(root)}:{number}")

if findings:
    print("zoo check found", len(findings), "problem(s):")
    print("Each finding is a second mechanism or a second meaning of one word. Find the cause and remove the duplicate: keep the owner, delete the copy. A rename or an allowlist line that only makes the check pass is refused at review.")
    for finding in findings:
        print(" -", finding)
    sys.exit(1)
print("zoo check: clean")
PY
