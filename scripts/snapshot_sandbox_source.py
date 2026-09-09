"""Freeze the actual dirty build inputs without copying credentials or user notes."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tarfile
from datetime import UTC, datetime
from pathlib import Path


def snapshot(root: Path, destination: Path) -> dict[str, object]:
    destination.mkdir(parents=True, exist_ok=False)
    names = (
        subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root
        )
        .decode()
        .split("\0")
    )
    files: dict[str, str] = {}
    for name in sorted(set(names)):
        path = Path(name)
        if not name or not (
            path.parts[0] in {"src", "tests", "scripts", "deploy"}
            or name in {"pyproject.toml", "uv.lock", "README.md", "README.en.md"}
        ):
            continue
        source = root / name
        if not source.is_file() or source.is_symlink():
            continue
        content = source.read_bytes()
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        shutil.copymode(source, target)
        files[name] = hashlib.sha256(content).hexdigest()
    digest = hashlib.sha256(
        "".join(f"{name}\0{value}\n" for name, value in files.items()).encode()
    ).hexdigest()
    status = subprocess.check_output(["git", "status", "--porcelain=v1"], cwd=root).decode()
    manifest: dict[str, object] = {
        "schema_version": 1,
        "commit_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root)
        .decode()
        .strip(),
        "dirty": bool(status),
        "git_status": status.splitlines(),
        "snapshot_sha256": digest,
        "snapshot_hash_algorithm": "sha256(sorted path + NUL + file sha256 + LF)",
        "captured_at": datetime.now(UTC).isoformat(),
        "files": files,
    }
    (destination / "build-source.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    manifest = snapshot(Path(__file__).resolve().parents[1], args.destination)
    args.archive.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(args.archive, "w:gz") as archive:
        archive.add(args.destination, arcname="source")
    args.archive.with_suffix(".json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({key: value for key, value in manifest.items() if key != "files"}))


if __name__ == "__main__":
    main()
