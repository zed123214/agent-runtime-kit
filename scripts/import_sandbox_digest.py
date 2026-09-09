"""Bind a verified kind image manifest to its digest reference after kind load."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--inspect-file", type=Path, required=True)
    parser.add_argument("--cluster", default="agentrt-sandbox-e2e")
    args = parser.parse_args()
    info = json.loads(args.inspect_file.read_text())[0]
    ref = info["RepoDigests"][0]
    if "/" not in ref:
        ref = "docker.io/library/" + ref
    tag = "docker.io/library/" + args.image
    digest = ref.split("@", 1)[1]
    nodes = subprocess.check_output(
        ["kind", "get", "nodes", "--name", args.cluster], text=True
    ).split()
    for node in nodes:
        command = ["docker", "exec", node, "ctr", "--namespace", "k8s.io", "images"]
        listed = subprocess.check_output([*command, "list"], text=True)
        rows = [line.split() for line in listed.splitlines()[1:]]
        assert any(row[0] == tag and row[2] == digest for row in rows), (
            "import changed image digest"
        )
        existing = [row for row in rows if row[0] == ref]
        if existing:
            assert existing[0][2] == digest, "digest alias conflicts with imported content"
        else:
            subprocess.run([*command, "tag", tag, ref], check=True)
    print(ref)


if __name__ == "__main__":
    main()
