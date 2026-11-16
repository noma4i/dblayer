import json
import sys

WIDTH = 180


def scalar(value):
    return json.dumps(value, ensure_ascii=False)


def multi(value):
    return isinstance(value, (dict, list)) and len(value) > 1


def breaks(value):
    if isinstance(value, list):
        return (len(value) > 1 and all(type(item) is type(value[0]) and multi(item) for item in value)) or any(breaks(item) for item in value)
    return isinstance(value, dict) and any(breaks(item) for item in value.values())


def inline(value):
    if isinstance(value, dict):
        return "{}" if not value else "{ " + ", ".join(scalar(key) + ": " + inline(value[key]) for key in sorted(value)) + " }"
    if isinstance(value, list):
        return "[]" if not value else "[" + ", ".join(inline(item) for item in value) + "]"
    return scalar(value)


def layout(value, depth, used):
    if not isinstance(value, (dict, list)) or not value:
        return inline(value)
    one = inline(value)
    if not breaks(value) and used + len(one) <= WIDTH:
        return one
    pad = "  " * (depth + 1)
    if isinstance(value, dict):
        keys = sorted(value)
        lines = [pad + scalar(key) + ": " + layout(value[key], depth + 1, len(pad) + len(scalar(key)) + 2 + (index < len(keys) - 1)) for index, key in enumerate(keys)]
        return "{\n" + ",\n".join(lines) + "\n" + "  " * depth + "}"
    lines = [pad + layout(item, depth + 1, len(pad) + (index < len(value) - 1)) for index, item in enumerate(value)]
    return "[\n" + ",\n".join(lines) + "\n" + "  " * depth + "]"


sys.stdout.write(layout(json.load(sys.stdin), 0, 0) + "\n")
