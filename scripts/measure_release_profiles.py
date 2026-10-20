import hashlib
import json
import os
from pathlib import Path
import resource
import re
import shutil
import subprocess
import sys


root = Path(__file__).resolve().parents[1]
output = root / "target/profile-measurements"
output.mkdir(parents=True, exist_ok=True)
profiles = [
    ("base", "3", "false", "16"),
    ("s", "s", "false", "16"),
    ("z", "z", "false", "16"),
    ("thin16", "3", "thin", "16"),
    ("fat16", "3", "fat", "16"),
    ("none1", "3", "false", "1"),
    ("thin1", "3", "thin", "1"),
    ("fat1", "3", "fat", "1"),
]
ndk = sorted((Path.home() / "Library/Android/sdk/ndk").iterdir())[-1]
llvm = ndk / "toolchains/llvm/prebuilt/darwin-x86_64/bin"
env = {**os.environ, "LC_ALL": "C", "ANDROID_NDK_HOME": str(ndk)}
env["PATH"] = "/opt/homebrew/opt/rustup/bin:" + env["PATH"]


def run(args, name, environment):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    with (output / f"{name}.log").open("w") as log:
        result = subprocess.run(args, cwd=root, env=environment, stdout=log, stderr=subprocess.STDOUT)
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if result.returncode:
        raise RuntimeError(f"{name}: exit {result.returncode}, see {output / (name + '.log')}")
    return after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime


def operation_cpu(name):
    log = (output / f"{name}.log").read_text()
    operations = {key: float(value) for key, value in re.findall(r"(\w+) rows=.*?median=([\d.]+)ms", log)}
    assert len(operations) == 5, log
    return operations


def build():
    for name, opt, lto, units in profiles:
        destination = output / name
        destination.mkdir(exist_ok=True)
        if (destination / "build.json").exists():
            continue
        if (output / "build").exists():
            shutil.rmtree(output / "build")
        environment = {**env, "CARGO_TARGET_DIR": str(output / "build"),
                       "CARGO_PROFILE_RELEASE_OPT_LEVEL": opt,
                       "CARGO_PROFILE_RELEASE_LTO": lto,
                       "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": units,
                       "CARGO_PROFILE_RELEASE_STRIP": "none",
                       "CARGO_PROFILE_RELEASE_PANIC": "unwind"}
        cpu = {}
        print(f"build {name}", flush=True)
        cpu["host"] = run(["cargo", "build", "-p", "dblayer", "--release", "--features", "bench", "--bin", "rowbench"], f"{name}-host", environment)
        shutil.copy2(output / "build/release/rowbench", destination / "rowbench")
        print(f"{name}: host complete, CPU {cpu['host']:.2f}s", flush=True)
        for target in ["aarch64-linux-android", "armv7-linux-androideabi", "aarch64-apple-ios"]:
            if "android" in target:
                abi = "arm64-v8a" if target.startswith("aarch64") else "armeabi-v7a"
                args = ["cargo", "ndk", "-t", abi, "build", "-p", "dblayer", "--release", "--lib"]
                library = "libdblayer.so"
            else:
                args = ["cargo", "build", "-p", "dblayer", "--release", "--lib", "--target", target]
                library = "libdblayer.a"
            target_env = dict(environment)
            if target == "aarch64-apple-ios":
                target_env["SDKROOT"] = subprocess.check_output(["xcrun", "--sdk", "iphoneos", "--show-sdk-path"], text=True).strip()
                target_env["IPHONEOS_DEPLOYMENT_TARGET"] = "17.0"
            cpu[target] = run(args, f"{name}-{target}", target_env)
            source = output / "build" / target / "release" / library
            shutil.copy2(source, destination / f"{target}-{library}")
            print(f"{name}: {target} complete, CPU {cpu[target]:.2f}s", flush=True)
        (destination / "build.json").write_text(json.dumps(cpu, indent=2) + "\n")
        print(f"built {name}: {cpu}", flush=True)


def cpu_build():
    project = output / "cpu-bench"
    project.mkdir(exist_ok=True)
    source = (root / "core/src/bin/rowbench.rs").read_text()
    assert source.count("use std::time::Instant;") == 1
    source = source.replace("use std::time::Instant;", "use cpu_clock::Instant;")
    source += '''
mod cpu_clock {
    pub struct Instant(f64);
    fn seconds() -> f64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) }, 0);
        let usage = unsafe { usage.assume_init() };
        usage.ru_utime.tv_sec as f64 + usage.ru_stime.tv_sec as f64
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0
    }
    impl Instant {
        pub fn now() -> Self { Self(seconds()) }
        pub fn elapsed(&self) -> std::time::Duration {
            std::time::Duration::from_secs_f64(seconds() - self.0)
        }
    }
}
'''
    (project / "main.rs").write_text(source)
    manifest = f'''[workspace]
[package]
name = "rowbench-cpu"
version = "0.0.0"
edition = "2021"
[[bin]]
name = "rowbench-cpu"
path = "main.rs"
[dependencies]
dblayer = {{ path = {json.dumps(str(root / "core"))}, features = ["bench"] }}
rusqlite = {{ version = "=0.40.2", features = ["bundled"] }}
serde_json = "=1.0.151"
tempfile = "=3.27.0"
libc = "=0.2.190"
'''
    (project / "Cargo.toml").write_text(manifest)
    (project / "Cargo.lock").write_bytes((root / "Cargo.lock").read_bytes())
    for name, opt, lto, units in profiles:
        destination = output / name / "rowbench-cpu"
        destination.parent.mkdir(exist_ok=True)
        if destination.exists():
            continue
        environment = {**env, "CARGO_TARGET_DIR": str(output / "cpu-build"),
                       "CARGO_PROFILE_RELEASE_OPT_LEVEL": opt,
                       "CARGO_PROFILE_RELEASE_LTO": lto,
                       "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": units,
                       "CARGO_PROFILE_RELEASE_STRIP": "none",
                       "CARGO_PROFILE_RELEASE_PANIC": "unwind"}
        run(["cargo", "build", "--offline", "--release", "--manifest-path", str(project / "Cargo.toml")], f"{name}-cpu-build", environment)
        shutil.copy2(output / "cpu-build/release/rowbench-cpu", destination)
        print(f"CPU timer executable ready: {name}", flush=True)


def measure():
    samples = []
    for repeat in range(5):
        for name, _, _, _ in profiles:
            executable = output / name / "rowbench-cpu"
            cpu = run([str(executable), "--rows", "10000", "--repeats", "1"], f"{name}-sample-{repeat}", env)
            operations = operation_cpu(f"{name}-sample-{repeat}")
            samples.append({"profile": name, "repeat": repeat, "cpu_seconds": cpu,
                            "operations_cpu_ms": operations, "load": os.getloadavg()})
            print(samples[-1], flush=True)
    (output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
    counts = []
    for name, _, _, _ in profiles:
        run([str(output / name / "rowbench-cpu"), "--rows", "1000", "--counts"], f"{name}-counts", env)
        counts.append((output / f"{name}-counts.log").read_text())
    assert len(set(counts)) == 1, "profiles performed different SQL work"
    control = {10000: [], 20000: []}
    for repeat in range(5):
        for rows in control:
            label = f"control-{rows}-{repeat}"
            cpu = run([str(output / "base/rowbench-cpu"), "--rows", str(rows), "--repeats", "1"], label, env)
            control[rows].append({"cpu_seconds": cpu, "operations_cpu_ms": operation_cpu(label)})
    for operation in ["read_ids", "read_query", "filter_plain"]:
        assert min(s["operations_cpu_ms"][operation] for s in control[20000]) > max(s["operations_cpu_ms"][operation] for s in control[10000]), (operation, "CPU measurement did not detect doubled work")
    (output / "control.json").write_text(json.dumps(control, indent=2) + "\n")


def sizes():
    measurements = []
    for name, _, _, _ in profiles:
        for target in ["aarch64-linux-android", "armv7-linux-androideabi"]:
            source = output / name / f"{target}-libdblayer.so"
            hashes = []
            exports = []
            for strip in ["none", "debuginfo", "symbols"]:
                destination = output / name / f"{target}-{strip}.so"
                shutil.copy2(source, destination)
                if strip != "none":
                    subprocess.run([str(llvm / "llvm-strip"), "--strip-debug" if strip == "debuginfo" else "--strip-all", str(destination)], check=True)
                text = subprocess.check_output([str(llvm / "llvm-size"), "-A", str(destination)], text=True)
                text_bytes = int(re.search(r"(?m)^\.text\s+(\d+)", text).group(1))
                section = destination.with_suffix(".text")
                subprocess.run([str(llvm / "llvm-objcopy"), f"--dump-section=.text={section}", str(destination), os.devnull], check=True)
                hashes.append(hashlib.sha256(section.read_bytes()).hexdigest())
                exports.append(subprocess.check_output([str(llvm / "llvm-nm"), "--dynamic", "--defined-only", str(destination)]))
                section.unlink()
                measurements.append({"profile": name, "target": target, "strip": strip,
                                     "bytes": destination.stat().st_size, "text_bytes": text_bytes,
                                     "text_sha256": hashes[-1]})
            assert len(set(hashes)) == 1, (name, target, "stripping changed instructions")
            assert len(set(exports)) == 1, (name, target, "stripping changed dynamic exports")
    (output / "sizes.json").write_text(json.dumps(measurements, indent=2) + "\n")


if sys.argv[1:] == ["build"]:
    build()
elif sys.argv[1:] == ["measure"]:
    measure()
elif sys.argv[1:] == ["cpu-build"]:
    cpu_build()
elif sys.argv[1:] == ["sizes"]:
    sizes()
else:
    raise SystemExit("usage: measure_release_profiles.py build|cpu-build|measure|sizes")
