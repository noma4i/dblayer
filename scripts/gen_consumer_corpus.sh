#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ "$1" != "--check" ] && [ "$1" != "--swift" ] && [ "$1" != "--kotlin" ]; }; then
  echo "usage: scripts/gen_consumer_corpus.sh [--check|--swift|--kotlin]" >&2
  exit 2
fi

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export LC_ALL=C
cd -- "$root"
app="$root/fixtures/consumer/app"
out="$root/core/tests/corpus/consumer"

models=()
while IFS= read -r file; do models+=("$file"); done < <(find "$app/schemas" -name '*.dbl' -type f | sort)

if [ "${1:-}" = "--swift" ] || [ "${1:-}" = "--kotlin" ]; then
  cargo run -p dblayer-gen -- --schema "$app/graphql/schema.graphql" "${1#--}" "${models[@]}"
  exit
fi

spec="$(cargo run -p dblayer-gen -- --internal --schema "$app/graphql/schema.graphql" spec "${models[@]}" 2> /dev/null)"
operations="$(find "$app/graphql" -name '*.graphql' -type f ! -name schema.graphql | sort | python3 -I -c '
import json, re, sys
text = "\n".join(open(path.strip()).read() for path in sys.stdin)
text = re.sub(r"(?m)#.*$", "", text)
definitions, depth, start = [], 0, None
for index, char in enumerate(text):
    if char == "{":
        if depth == 0 and start is None:
            start = text.rfind("\n\n", 0, index) + 1 if "\n\n" in text[:index] else 0
        depth += 1
    elif char == "}":
        depth -= 1
        if depth == 0:
            definitions.append(text[start:index + 1].strip())
            text = text[:start] + " " * (index + 1 - start) + text[index + 1:]
            start = None
fragments, operations = {}, {}
for definition in definitions:
    head = re.match(r"(query|mutation|subscription|fragment)\s+(\w+)", definition)
    (fragments if head.group(1) == "fragment" else operations)[head.group(2)] = definition
def needed(body, seen):
    for name in re.findall(r"\.\.\.\s*(\w+)", body):
        if name != "on" and name in fragments and name not in seen:
            seen.append(name)
            needed(fragments[name], seen)
    return seen
result = []
for name in sorted(operations):
    body = operations[name]
    document = "\n\n".join([body] + [fragments[fragment] for fragment in needed(body, [])])
    result.append({"name": name, "document": document})
print(json.dumps(result, indent=1))
')"

if [ "$#" -eq 1 ]; then
  [ "$spec" = "$(cat "$out/spec.json")" ] && [ "$operations" = "$(cat "$out/operations.json")" ] || { echo "Consumer corpus differs from the app schemas. Run scripts/gen_consumer_corpus.sh."; exit 1; }
  echo "Consumer corpus matches."
  exit 0
fi

mkdir -p "$out"
printf '%s\n' "$spec" > "$out/spec.json"
printf '%s\n' "$operations" > "$out/operations.json"
