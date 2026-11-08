#!/usr/bin/env python3
"""C1: every field and variant of a UniFFI record, enum or error of the core appears in the generated Swift and Kotlin type of the same name with the same optionality, unless bridge.md carries a BSKIP row with the reason."""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_record_parity as parity  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SWIFT = ROOT / "swift" / "Sources" / "DBlayer" / "dblayer.swift"
KOTLIN = ROOT / "kotlin" / "src" / "main" / "kotlin" / "com" / "noma4i" / "dblayer" / "dblayer.kt"


def squash(name):
    return name.replace("`", "").replace("_", "").lower()


def optional_core(kind):
    return kind.strip().startswith("Option<")


def core_shape(core):
    """name -> {slot: optional}, slot = field or Variant or Variant.field, normalised by squash."""
    shapes = {}
    for path in sorted(core.rglob("*.rs")):
        if "tests" in path.parts or path.name.endswith("_tests.rs"):
            continue
        for kind, name, body in parity.blocks(path.read_text()):
            shapes.setdefault(name, {}).update(core_slots(kind, body))
    return shapes


def core_slots(kind, body):
    slots = {}
    for part in parity.top_level(body):
        if kind == "struct":
            field = re.match(r"(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:\s*(.*)", part, re.S)
            if field:
                slots[squash(field.group(1))] = optional_core(field.group(2))
            continue
        variant = re.match(r"(\w+)\s*(?:\{(.*)\})?", part, re.S)
        if not variant:
            continue
        if variant.group(2) is None:
            slots[squash(variant.group(1))] = False
            continue
        slots[squash(variant.group(1))] = False
        for field, optional in core_slots("struct", variant.group(2)).items():
            slots[f"{squash(variant.group(1))}.{field}"] = optional
    return slots


def typed_params(text):
    slots = {}
    for part in parity.top_level(re.sub(r"(?s)/\*.*?\*/|//[^\n]*", "", text)):
        part = re.sub(r"^(?:public\s+)?(?:var|val|let)\s+", "", part)
        found = re.match(r"`?(\w+)`?\s*:\s*(.*)", part, re.S)
        if found:
            slots[squash(found.group(1))] = re.sub(r"\s+=\s+[^=()@]*$", "", found.group(2).strip()).endswith("?")
    return slots


def swift_shapes(text):
    shapes = {}
    for head in re.finditer(r"(?m)^(?:public )?(?:indirect )?(struct|enum) (\w+)[^\n{]*\{\n", text):
        end = text.find("\n}\n", head.end() - 1)
        body = text[head.end():end + 1]
        if head.group(1) == "struct":
            shapes[head.group(2)] = typed_params("\n,".join(re.findall(r"(?m)^    public (?:var|let) [^\n]*", body)).replace("\n,", ","))
            continue
        slots = {}
        for case in re.finditer(r"(?ms)^    case `?(\w+)`?(?:\((.*?)\n    \))?(?=\n)", body):
            name = squash(case.group(1))
            slots[name] = False
            for field, optional in typed_params(case.group(2) or "").items():
                slots[f"{name}.{field}"] = optional
        shapes[head.group(2)] = slots
    return shapes


def kotlin_shapes(text):
    shapes = {}
    for head in re.finditer(r"(?m)^(data class|enum class|sealed class|class|object) (\w+)\s*([^\n]*)\n", text):
        kind, name = head.group(1), head.group(2)
        if kind in ("class", "object") and name in shapes:
            continue
        if kind == "data class":
            close = text.find("\n)", head.end())
            shapes[name] = typed_params(text[head.end():close].replace("\n    ,", ",") if head.group(3).strip() == "(" else "")
        elif kind == "enum class":
            end = text.find("\n}\n", head.end())
            shapes[name] = {squash(token): False for token in re.findall(r"(?m)^    ([A-Z][A-Z0-9_]*)\b", text[head.end():end])}
        elif kind == "sealed class":
            end = text.find("\n}\n", head.end())
            body = text[head.end():end + 1]
            slots = {}
            for case in re.finditer(r"(?ms)^    (?:data )?(?:class|object) (\w+)(\((.*?)\))?[^\n]*(?=\n)", body):
                variant = squash(case.group(1))
                slots[variant] = False
                for field, optional in typed_params(case.group(3) or "").items():
                    slots[f"{variant}.{field}"] = optional
            shapes[name] = slots
    return shapes


def skip_rows(bridge):
    rows = set()
    for line in bridge.read_text().splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) >= 5 and re.fullmatch(r"BSKIP-\d+", cells[0]) and cells[4]:
            rows.add((cells[1], cells[2], squash(cells[3])))
    return rows


def compare(core, projections, skips, platform):
    problems = []
    for name, slots in sorted(core.items()):
        shape = projections.get(name)
        if shape is None and platform == "kotlin" and name.endswith("Error"):
            shape = projections.get(name[:-5] + "Exception")
        if shape is None:
            problems.append(f"{platform}: type {name} of the core has no projection")
            continue
        for slot, optional in sorted(slots.items()):
            if any(skip[0] == name and squash(skip[1]) == slot and skip[2] in (platform, "both") for skip in skips):
                continue
            if slot not in shape:
                problems.append(f"{platform}: {name}.{slot} of the core has no projection field")
            elif shape[slot] != optional:
                problems.append(f"{platform}: {name}.{slot} optional is {optional} in the core and {shape[slot]} in the projection")
        for slot in sorted(set(shape) - set(slots)):
            problems.append(f"{platform}: {name}.{slot} of the projection is not in the core")
    return problems


def mapping_rows(bridge):
    """(record, [snake fields], swift [Type.prop], kotlin [Type.prop]) of the bridge rows that map a Wire field to a hand-written projection."""
    rows = []
    for line in bridge.read_text().splitlines():
        cells = [cell.strip().replace("`", "") for cell in line.strip().strip("|").split("|")]
        head = re.match(r"(Wire\w+)\.(.+)$", cells[0]) if len(cells) >= 3 else None
        if not head:
            continue
        fields = [field.strip() for field in head.group(2).split(",")]
        swift = targets(cells[1], len(fields))
        kotlin = swift if cells[2].startswith("те же") else targets(cells[2], len(fields))
        if swift and kotlin:
            rows.append((head.group(1), fields, swift, kotlin))
    return rows


def targets(cell, count):
    cell = re.split(r"[:{]", cell)[0]
    parts = [part.strip() for part in cell.split(",")]
    first = re.match(r"([A-Z]\w*)\.(\w+)$", parts[0])
    if not first or len(parts) != count:
        return None
    return [(first.group(1), first.group(2))] + [(first.group(1), part) for part in parts[1:]]


def swift_props(text, type_name):
    head = re.search(rf"(?m)^(?:public )?(?:struct|final class|class) {type_name}\b[^\n]*\{{\n", text)
    if not head:
        return None
    end = text.find("\n}\n", head.end())
    lines = re.findall(r"(?m)^  public (?:var|let) [^\n]*", text[head.end():end])
    return typed_params(",".join(lines))


def kotlin_props(text, type_name):
    head = re.search(rf"(?m)^data class {type_name}\(", text)
    if not head:
        return None
    end = text.find("\n)", head.end())
    return typed_params(re.sub(r"@SerialName\([^)]*\)\s*", "", text[head.end():end]))


def hand_written(root, suffix, skip):
    return "\n".join(path.read_text() for path in sorted(root.glob(f"*.{suffix}")) if path.name != skip)


def camel(snake):
    return squash(snake)


def compare_mappings(rows, core, swift_text, kotlin_text, skips):
    problems = []
    named = {}
    for record, fields, swift, kotlin in rows:
        entry = named.setdefault(record, {"swift": swift[0][0], "kotlin": kotlin[0][0], "map": {}})
        for field, (_, s_prop), (_, k_prop) in zip(fields, swift, kotlin):
            entry["map"][squash(field)] = (squash(s_prop), squash(k_prop))
    for record, entry in sorted(named.items()):
        for platform, props in (("swift", swift_props(swift_text, entry["swift"])), ("kotlin", kotlin_props(kotlin_text, entry["kotlin"]))):
            type_name = entry[platform]
            if props is None:
                problems.append(f"{platform}: projection {type_name} of {record} is not declared")
                continue
            expected = {}
            for slot, optional in core.get(record, {}).items():
                if any(skip[0] == record and squash(skip[1]) == slot and skip[2] in (platform, "both") for skip in skips):
                    continue
                expected[entry["map"].get(slot, (slot, slot))[0 if platform == "swift" else 1]] = (slot, optional)
            for name, (slot, optional) in sorted(expected.items()):
                if name not in props:
                    problems.append(f"{platform}: {record}.{slot} of the core has no property in {type_name}")
                elif props[name] != optional:
                    problems.append(f"{platform}: {record}.{slot} optional is {optional} in the core and {props[name]} in {type_name}")
            for name in sorted(set(props) - set(expected)):
                problems.append(f"{platform}: {type_name}.{name} is not a field of {record}")
    return problems


def main(argv):
    bridge = Path(argv[1]) if len(argv) > 1 else ROOT / "specs" / "bridge.md"
    for path in (bridge, SWIFT, KOTLIN):
        if not path.is_file():
            print(f"check_projection_parity: {path} is missing", file=sys.stderr)
            return 1
    core = core_shape(ROOT / "core" / "src")
    skips = skip_rows(bridge)
    problems = compare(core, swift_shapes(SWIFT.read_text()), skips, "swift") + compare(core, kotlin_shapes(KOTLIN.read_text()), skips, "kotlin")
    swift_text = hand_written(SWIFT.parent, "swift", "dblayer.swift")
    kotlin_text = hand_written(KOTLIN.parent, "kt", "dblayer.kt")
    rows = mapping_rows(bridge)
    problems += compare_mappings(rows, core, swift_text, kotlin_text, skips)
    for problem in problems:
        print(f"check_projection_parity: {problem}", file=sys.stderr)
    print(f"check_projection_parity: compared {len(core)} types, {sum(map(len, core.values()))} slots, {len(rows)} mapped rows, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
