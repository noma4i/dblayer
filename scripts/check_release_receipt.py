#!/usr/bin/env python3
# plan: per lane run|reuse from sha256 of the real lane inputs against earlier green receipts; write: RECEIPT.json from the ran rows and the reused records.
import argparse, hashlib, json, os, re, subprocess, sys, tempfile, zipfile
from pathlib import Path

SEQ = ["core", "swift", "kotlin"]
APP = ["ios", "android"]
NATIVE = ["core/src", "core/Cargo.toml", "core/uniffi.toml", "gen", "bindgen", "fixtures", "Cargo.toml", "Cargo.lock", ".cargo", "rust-toolchain.toml", "rust-toolchain", "scripts/native_source_hash.sh"]
CHECKS = ["scripts/check_sequences.sh", "scripts/check_bounded.sh", "scripts/check_steps.sh"]
KOTLIN_BUILD = ["scripts/build_kotlin.sh", "scripts/build_bindgen.sh", "scripts/generate_kotlin_bindings.sh", "scripts/gen_fixtures.sh"]
ARTIFACTS = {"DBlayerFFI.xcframework.zip", "dblayer.swift", "dblayer-android.zip", "dblayer-gen-macos-universal"}
SCHEMA = 1
SCANNED_STAGES = 3


def refuse(message):
    sys.exit(f"release receipt: {message}")


def sha(data):
    return hashlib.sha256(data if isinstance(data, bytes) else data.encode()).hexdigest()


def stream(handle):
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1 << 20), b""):
        digest.update(chunk)
    return digest.hexdigest()


def file_sha(path):
    with open(path, "rb") as handle:
        return stream(handle)


def zip_sha(path):
    if not path.is_file():
        return None
    with zipfile.ZipFile(path) as archive:
        members = {}
        for info in archive.infolist():
            if not info.is_dir():
                with archive.open(info) as handle:
                    members[info.filename] = stream(handle)
    return sha(json.dumps(members, sort_keys=True))


def tree_sha(base):
    if base.is_symlink():
        refuse(f"{base} is a symlink: its target is not fingerprinted")
    if base.is_file():
        return {"": file_sha(base)}
    entries = {}
    for path in sorted(base.rglob("*")):
        relative = path.relative_to(base)
        if path.is_symlink():
            refuse(f"{path} is a symlink: its target is not fingerprinted")
        if path.is_file():
            entries[str(relative)] = file_sha(path)
    return entries


def paths_sha(spec):
    result = {}
    for item in spec.split(os.pathsep):
        if not item or not Path(item).exists():
            return None
        result[item] = tree_sha(Path(item))
    return sha(json.dumps(result, sort_keys=True))


def git(root, *args):
    run = subprocess.run(["git", "-C", str(root), *args], capture_output=True)
    return run.stdout if run.returncode == 0 else None


def objects(root, commit, paths):
    return {p: (git(root, "rev-parse", "-q", "--verify", f"{commit}:{p}") or b"absent").decode().strip() for p in paths}


def manifest_rows(root, commit, manifest, lane):
    text = git(root, "show", f"{commit}:{manifest}")
    rows = None if text is None else sorted(l for l in text.decode().splitlines()[2:] if l.split("\t")[2:3] == [lane])
    return rows and sha("\n".join(rows))


def lane_parts(c, lane):
    zipped = lambda name: zip_sha(c.stage / name)
    single = lambda name: file_sha(c.stage / name) if (c.stage / name).is_file() else None
    env = lambda key, default=None: c.env.get(key) or default
    parts = {}
    if lane in SEQ:
        parts["manifest"] = manifest_rows(c.root, c.commit, c.manifest, lane)
        parts.update(objects(c.root, c.commit, CHECKS))
    if lane == "core":
        parts.update(objects(c.root, c.commit, NATIVE))
    elif lane == "swift":
        parts.update(objects(c.root, c.commit, ["swift"]))
        parts.update(xcframework=zipped("DBlayerFFI.xcframework.zip"), binding=single("dblayer.swift"))
    elif lane == "kotlin":
        parts.update(objects(c.root, c.commit, ["kotlin", *NATIVE, *KOTLIN_BUILD]))
        parts["android"] = zipped("dblayer-android.zip")
    else:
        key = lane.upper()
        parts.update(objects(c.root, c.commit, ["scripts/check_consumers.sh", *(["swift/Sources"] if lane == "ios" else [])]))
        parts["generator"] = single("dblayer-gen-macos-universal")
        if lane == "ios":
            package = c.package_swift and Path(c.package_swift)
            text = package.read_text() if package and package.is_file() else None
            parts.update(xcframework=zipped("DBlayerFFI.xcframework.zip"), binding=single("dblayer.swift"))
            parts["package"] = text and sha(re.sub(r'(url|checksum): "[^"]*"', r'\1: ""', text))
        else:
            parts["maven"] = c.maven and c.maven.is_dir() and sha(json.dumps(tree_sha(c.maven), sort_keys=True))
        command, external, repo = env(f"CONSUMER_{key}_COMMAND"), env(f"CONSUMER_INPUTS_{key}"), env("CONSUMER_REPO")
        ref = repo and git(repo, "rev-parse", "--verify", "--quiet", env(f"CONSUMER_{key}_REF", "master") + "^{commit}")
        parts.update(consumer_ref=ref and ref.decode().strip(), consumer_command=command and sha(command), external_inputs=external and paths_sha(external))
    return parts


def lane_inputs(c, lanes):
    result = {}
    for lane in lanes:
        parts = lane_parts(c, lane)
        result[lane] = {"sha": None if any(not v for v in parts.values()) else sha(json.dumps(parts, sort_keys=True)), "parts": parts}
    return result


def hexed(value, size):
    return isinstance(value, str) and re.fullmatch(f"[0-9a-f]{{{size}}}", value) is not None


def load(c, cdir):
    """(version, {lane: (inputs, record, origin)}) of an earlier receipt; ValueError when it cannot be trusted."""
    receipt = json.loads((cdir / "RECEIPT.json").read_text())
    version, commit, artifacts = receipt["version"], receipt["source_commit"], receipt["artifacts_sha256"]
    if receipt["schema"] != SCHEMA or not hexed(commit, 40) or not hexed(receipt["manifest_sha256"], 64) or set(artifacts) != ARTIFACTS or not all(hexed(v, 64) for v in artifacts.values()):
        raise ValueError("receipt fields are incomplete")
    lines = (cdir / "SHA256SUMS").read_text().splitlines()
    sums = dict(l.split("  ", 1)[::-1] for l in lines)
    if len(lines) != len(ARTIFACTS) or sums != artifacts or any(file_sha(cdir / n) != d for n, d in artifacts.items()):
        raise ValueError("SHA256SUMS or an artifact differs from the receipt")
    blob = git(c.root, "show", f"{commit}:{c.manifest}")
    if blob is None or sha(blob) != receipt["manifest_sha256"]:
        raise ValueError("manifest differs from the receipt")
    records = {**receipt["sequences"], **receipt["consumer_lanes"]}
    found = {}
    for lane, record in records.items():
        inputs, origin = receipt["lane_inputs"].get(lane), receipt["lane_origin"].get(lane)
        if not inputs or not origin or (lane in APP and not inputs["parts"]):
            continue
        if not hexed(origin["source_commit"], 40) or not re.fullmatch(r"[\w.-]+", origin["run_id"]) or git(c.root, "cat-file", "-e", origin["source_commit"] + "^{commit}") is None:
            raise ValueError(f"origin of lane {lane} is incomplete")
        if not hexed(inputs["sha"], 64) or (inputs["parts"] and sha(json.dumps(inputs["parts"], sort_keys=True)) != inputs["sha"]):
            raise ValueError(f"inputs of lane {lane} do not match their parts")
        if lane in SEQ:
            again = argparse.Namespace(root=c.root, commit=origin["source_commit"], manifest=c.manifest, stage=cdir, maven=None, package_swift=None, env={})
            if lane_inputs(again, [lane])[lane]["sha"] != inputs["sha"]:
                raise ValueError(f"inputs of lane {lane} differ from its origin {origin['source_commit'][:9]}")
        found[lane] = (inputs, record, origin)
    return version, found


def expected_counts(manifest_text):
    counts = {}
    for line in manifest_text.splitlines()[2:]:
        cells = line.split("\t")
        if len(cells) > 3:
            counts.setdefault(cells[2], set()).add(cells[3])
    return {lane: len(tests) for lane, tests in counts.items()}


def valid(lane, record, expected):
    if not isinstance(record, dict) or record.get("rc") != "0":
        return False
    return lane in APP or (expected > 0 and record.get("listed") == expected and record.get("passed") == expected)


def context(a):
    return argparse.Namespace(root=Path(a.root), commit=a.commit, manifest=a.manifest, stage=Path(a.stage), maven=Path(a.maven) if a.maven else None, package_swift=a.package_swift, env=dict(os.environ))


def plan(a):
    c, stage = context(a), Path(a.stage)
    lanes = SEQ + a.consumer_lanes.split()
    current = lane_inputs(c, lanes)
    unknown = [f"{l}: {', '.join(k for k, v in n['parts'].items() if not v)}" for l, n in current.items() if n["sha"] is None]
    if unknown:
        refuse("lane inputs are unknown, nothing is run and nothing is reused (missing staged file, git object, CONSUMER_REPO/_REF/_COMMAND or CONSUMER_INPUTS_<LANE> path): " + "; ".join(unknown))
    counts = expected_counts((c.root / a.manifest).read_text())
    loaded = []
    if (stage / "RECEIPT.json").is_file():
        try:
            loaded.append((stage, *load(c, stage)))
        except (OSError, ValueError, KeyError, TypeError, AttributeError, json.JSONDecodeError, zipfile.BadZipFile) as error:
            refuse(f"the receipt of this stage is unusable: {error}")
    others = sorted((p.parent for p in (c.root / "target").glob("release-*/RECEIPT.json") if p.parent != stage), key=lambda p: (p / "RECEIPT.json").stat().st_mtime, reverse=True)
    for cdir in others[:SCANNED_STAGES]:
        try:
            loaded.append((cdir, *load(c, cdir)))
        except (OSError, ValueError, KeyError, TypeError, AttributeError, json.JSONDecodeError, zipfile.BadZipFile) as error:
            print(f"note\t-\t{cdir.name} skipped: {error}", file=sys.stderr)
    result = {"commit": a.commit, "run_id": a.run_id, "lanes": {}}
    for lane in lanes:
        now, entry, reason = current[lane], {"verdict": "run", "inputs": current[lane]}, "no earlier receipt"
        for cdir, version, found in loaded:
            old, record, origin = found.get(lane, ({"sha": None, "parts": {}}, None, None))
            if old["sha"] != now["sha"]:
                reason = f"inputs changed against {cdir.name}: " + (", ".join(sorted(k for k in now["parts"] if old["parts"].get(k) != now["parts"][k])) or "no part list recorded")
            elif not valid(lane, record, counts.get(lane, 0)):
                reason = f"{cdir.name} holds no complete green result of lane {lane}"
            else:
                entry.update(verdict="reuse", record=record, origin={**origin, "reused_from": version})
                reason = f"inputs equal to {cdir.name}, origin {origin['source_commit'][:9]} run {origin['run_id']}"
                break
        result["lanes"][lane] = entry
        print(f"{entry['verdict']}\t{lane}\t{reason}")
    Path(a.plan).write_text(json.dumps(result, sort_keys=True))


def write(a):
    stage, manifest = Path(a.stage), Path(a.root) / a.manifest
    planned = json.loads(Path(a.plan).read_text())
    plans, apps = planned["lanes"], a.consumer_lanes.split()
    if planned["commit"] != a.commit or planned["run_id"] != a.run_id or sorted(plans) != sorted(SEQ + apps):
        refuse("the plan belongs to another source, run or lane set")
    if lane_inputs(context(a), list(plans)) != {l: p["inputs"] for l, p in plans.items()}:
        refuse("lane inputs changed while the checks ran: no receipt")

    def ran(name, kind, width):
        wanted = [l for l in kind if l in plans and plans[l]["verdict"] == "run"]
        path, result = stage / name, {}
        if not wanted:
            return result
        if not path.is_file() or path.stat().st_size == 0:
            refuse(f"{name} is missing or empty")
        for row in (line.split("\t") for line in path.read_text().splitlines()):
            if len(row) < width or row[0] in result or row[width - 2] != a.commit or row[width - 1] != a.run_id:
                refuse(f"{name} has a malformed, repeated or foreign row: {row}")
            result[row[0]] = row
        if sorted(result) != sorted(wanted):
            refuse(f"{name} lanes are {sorted(result)}, expected {sorted(wanted)}")
        return result

    counts, rows = expected_counts(manifest.read_text()), {**ran("sequences.tsv", SEQ, 7), **ran("consumers.tsv", APP, 5)}
    records, origin = {}, {}
    for lane, planned_lane in plans.items():
        if planned_lane["verdict"] == "reuse":
            records[lane], origin[lane] = planned_lane["record"], planned_lane["origin"]
            continue
        row, origin[lane] = rows[lane], {"source_commit": a.commit, "run_id": a.run_id}
        try:
            records[lane] = {"listed": int(row[1]), "passed": int(row[2]), "rc": row[3]} if lane in SEQ else {"rc": row[1]}
        except ValueError:
            refuse(f"sequences.tsv lane {lane} has non-numeric counts: {row}")
    for lane, record in records.items():
        if not valid(lane, record, counts.get(lane, 0)):
            refuse(f"lane {lane} is red or incomplete: {record} manifest={counts.get(lane, 0)}")
    sums = {line.split("  ", 1)[1]: line.split("  ", 1)[0] for line in (stage / "SHA256SUMS").read_text().splitlines()}
    if set(sums) != ARTIFACTS:
        refuse(f"SHA256SUMS names {sorted(sums)}")
    receipt = {
        "schema": SCHEMA, "version": a.version, "source_commit": a.commit, "manifest_sha256": sha(manifest.read_bytes()), "artifacts_sha256": sums,
        "sequences": {l: records[l] for l in SEQ}, "consumers": a.consumers, "consumer_lanes": {l: records[l] for l in apps},
        "consumer_lanes_required": apps, "lane_inputs": {l: p["inputs"] for l, p in plans.items()}, "lane_origin": origin,
    }
    with tempfile.NamedTemporaryFile("w", dir=stage, prefix=".RECEIPT.", delete=False) as handle:
        handle.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    os.replace(handle.name, stage / "RECEIPT.json")


parser = argparse.ArgumentParser()
parser.add_argument("command", choices=["plan", "write", "inputs"])
for name, default in [("root", str(Path(__file__).resolve().parent.parent)), ("stage", None), ("manifest", "core/src/ffi/selftest/manifest.tsv"), ("commit", None), ("version", ""), ("run-id", ""), ("maven", ""), ("package-swift", ""), ("consumers", "required"), ("consumer-lanes", ""), ("plan", "")]:
    parser.add_argument(f"--{name}", default=default, required=name in ("stage", "commit"))
args = parser.parse_args()
if args.command == "inputs":
    print(json.dumps(lane_inputs(context(args), SEQ + args.consumer_lanes.split()), indent=2, sort_keys=True))
else:
    {"plan": plan, "write": write}[args.command](args)
