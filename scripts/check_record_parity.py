#!/usr/bin/env python3
"""C1: every field and variant of a UniFFI record or enum of the core has a row in the bridge contract (BREC), and every row names a field the core has."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DERIVE = re.compile(r"#\[derive\(([^)]*)\)\]")
KINDS = ("uniffi::Record", "uniffi::Enum", "uniffi::Error")


def strip(text):
    return re.sub(r"(?s)/\*.*?\*/|//[^\n]*", "", text)


def blocks(text):
    """Yield (kind, name, body) of every item that derives a UniFFI record, enum or error."""
    text = strip(text)
    for found in DERIVE.finditer(text):
        if not any(kind in found.group(1) for kind in KINDS):
            continue
        head = re.compile(r"\s*(?:#\[[^\]]*\]\s*)*pub\s+(struct|enum)\s+(\w+)\s*(?:<[^>]*>)?\s*([{(;])").match(text, found.end())
        if not head:
            continue
        kind, name, opener = head.groups()
        if opener != "{":
            yield kind, name, ""
            continue
        depth, at = 0, head.end() - 1
        for index in range(at, len(text)):
            depth += text[index] == "{"
            depth -= text[index] == "}"
            if depth == 0:
                yield kind, name, text[at + 1:index]
                break


def top_level(body):
    depth, current, parts = 0, [], []
    for char in body:
        if char in "{(<[":
            depth += 1
        elif char in "})>]":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return [re.sub(r"#\[[^\]]*\]", "", part).strip() for part in parts if part.strip()]


def fields_of(kind, body):
    if kind == "struct":
        return {re.match(r"(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:", part).group(1) for part in top_level(body) if re.match(r"(?:pub(?:\([^)]*\))?\s+)?\w+\s*:", part)}
    found = set()
    for part in top_level(body):
        variant = re.match(r"(\w+)\s*(?:\{(.*)\})?", part, re.S)
        if not variant:
            continue
        if variant.group(2) is None:
            found.add(variant.group(1))
        else:
            found |= {f"{variant.group(1)}.{field}" for field in fields_of("struct", variant.group(2))}
    return found


def core_records(core):
    records = {}
    for path in sorted(core.rglob("*.rs")):
        if "tests" in path.parts or path.name.endswith("_tests.rs"):
            continue
        for kind, name, body in blocks(path.read_text()):
            records.setdefault(name, set()).update(fields_of(kind, body))
    return records


def contract_rows(bridge):
    rows = {}
    for line in bridge.read_text().splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) >= 6 and re.fullmatch(r"BREC-\d+", cells[0]):
            rows[cells[1]] = {field.strip() for field in cells[2].split(",") if field.strip()}
    return rows


def compare(records, rows):
    problems = []
    for name, fields in sorted(rows.items()):
        if name not in records:
            problems.append(f"contract row {name} names no record of the core")
            continue
        for field in sorted(fields - records[name]):
            problems.append(f"contract row {name} lists {field}, the core has no such field")
        for field in sorted(records[name] - fields):
            problems.append(f"{name}.{field} of the core has no contract field")
    return problems


def main(argv):
    bridge = Path(argv[1]) if len(argv) > 1 else ROOT / "specs" / "bridge.md"
    if not bridge.is_file():
        print(f"check_record_parity: {bridge} is missing", file=sys.stderr)
        return 1
    problems = compare(core_records(ROOT / "core" / "src"), contract_rows(bridge))
    for problem in problems:
        print(f"check_record_parity: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
