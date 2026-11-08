#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || { [ "$#" -eq 2 ] && [ "$2" != "--check" ]; }; then
  echo "usage: scripts/sync_consumer.sh <app path> [--check]" >&2
  exit 2
fi

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
app="$(cd -- "$1" && pwd -P)"
dest="$root/fixtures/consumer/app"
export LC_ALL=C

for kind in schemas/models schemas/operations; do
  [ -d "$app/$kind" ] || { echo "missing $app/$kind" >&2; exit 2; }
done
[ -f "$app/graphql/schema.graphql" ] || { echo "missing $app/graphql/schema.graphql" >&2; exit 2; }

if [ "$#" -eq 2 ]; then
  status=0
  for kind in schemas/models schemas/operations; do
    diff -rq -x .DS_Store "$app/$kind" "$dest/$kind" || status=1
  done
  diff -rq -x .DS_Store "$app/graphql" "$dest/graphql" || status=1
  [ "$status" -eq 0 ] || { echo "Consumer fixtures differ from the app. Run scripts/sync_consumer.sh <app path>."; exit 1; }
  echo "Consumer fixtures match."
  exit 0
fi

mkdir -p "$dest/schemas/models" "$dest/schemas/operations" "$dest/graphql"
for kind in schemas/models schemas/operations; do
  find "$dest/$kind" -name '*.dbl' -type f -delete
  find "$app/$kind" -maxdepth 1 -name '*.dbl' -type f -exec cp {} "$dest/$kind/" \;
done
find "$dest/graphql" -mindepth 1 -delete
find "$app/graphql" -type f -name '*.graphql' | while read -r file; do
  relative="${file#"$app"/graphql/}"
  mkdir -p "$dest/graphql/$(dirname "$relative")"
  cp "$file" "$dest/graphql/$relative"
done
