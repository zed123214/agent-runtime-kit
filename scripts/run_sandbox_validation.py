"""Run M1/M2 in a single Core-identity Job and export verifiable raw artifacts."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import io
import json
import os
import platform
import subprocess
import sys
import tarfile
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def verify_source(path: Path) -> dict[str, Any]:
    source: dict[str, Any] = json.loads(path.read_text())
    files = source["files"]
    for name, digest in files.items():
        actual = path.parent / name
        if not actual.is_file() or hashlib.sha256(actual.read_bytes()).hexdigest() != digest:
            raise ValueError(f"Image source snapshot mismatch: {name}")
    aggregate = hashlib.sha256(
        "".join(f"{name}\0{digest}\n" for name, digest in sorted(files.items())).encode()
    ).hexdigest()
    if aggregate != source["snapshot_sha256"]:
        raise ValueError("Source snapshot aggregate mismatch")
    return source


async def inventory() -> dict[str, list[dict[str, str]]]:
    from agent_runtime.core.config import get_config
    from agent_runtime.core.sandbox.kube_api import KubernetesApi

    config = get_config().sandbox
    api = KubernetesApi(config.kubernetes.namespace)
    try:
        result = {}
        for kind in ("pod", "service", "secret"):
            items = (await api.list(kind))["items"]
            result[kind] = [
                {"name": item["metadata"]["name"], "uid": item["metadata"]["uid"]} for item in items
            ]
        return result
    finally:
        await api.close()


def export(output: Path) -> None:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(output.rglob("*")):
            if (
                path.is_file()
                and not path.is_symlink()
                and path.suffix in {".json", ".jsonl", ".xml"}
            ):
                archive.add(path, arcname=str(path.relative_to(output)), recursive=False)
    raw = buffer.getvalue()
    print("ARTIFACT_TGZ_SHA256 " + hashlib.sha256(raw).hexdigest())
    print("ARTIFACT_TGZ_BASE64_BEGIN")
    encoded = base64.b64encode(raw).decode()
    for start in range(0, len(encoded), 4096):
        print(encoded[start : start + 4096])
    print("ARTIFACT_TGZ_BASE64_END", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enable-kubernetes", action="store_true", required=True)
    parser.add_argument("--stage", choices=("m1", "m2", "all"), default="m1")
    parser.add_argument("--output", type=Path, default=Path("/artifacts"))
    parser.add_argument("--source-manifest", type=Path, default=Path("/app/build-source.json"))
    parser.add_argument(
        "--environment", type=Path, default=Path("/run/validation/environment.json")
    )
    args = parser.parse_args()
    if not os.environ.get("KUBERNETES_SERVICE_HOST"):
        parser.error("run in a Core-identity Pod, not against a host context")
    source = verify_source(args.source_manifest)
    environment = json.loads(args.environment.read_text())
    if environment["source_snapshot_sha256"] != source["snapshot_sha256"]:
        raise ValueError("Environment manifest belongs to another source snapshot")
    if environment["images"]["worker"] != os.environ["AGENTRT_SANDBOX_KUBERNETES_IMAGE"]:
        raise ValueError("Environment manifest describes another Worker image")
    args.output.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "stage": args.stage,
        "commit_sha": source["commit_sha"],
        "source_dirty": source["dirty"],
        "source_snapshot_sha256": source["snapshot_sha256"],
        "test_time": datetime.now(UTC).isoformat(),
        **environment,
        "provider": "offline deterministic test providers; no LLM API requests",
        "image_prepulled": True,
        "runtime_os": platform.platform(),
        "runtime_python": sys.version,
        "passed": False,
        "failed": None,
        "case_count": 0,
        "iteration_count": 0,
        "concurrency": 1,
        "policy_version": "sandbox-deny-v1",
    }
    code = 1
    try:
        if args.stage in ("m1", "all"):
            report = args.output / "m1-junit.xml"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/integration/test_kubernetes_sandbox.py",
                    "-v",
                    "-m",
                    "kubernetes",
                    "--tb=short",
                    "-o",
                    "cache_dir=/artifacts/pytest-cache",
                    "--junitxml",
                    str(report),
                    "--basetemp",
                    str(args.output / "pytest"),
                ],
                env=dict(os.environ, AGENTRT_K8S_E2E="1", ANTHROPIC_API_KEY=""),
                check=False,
            )
            totals = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
            if report.exists():
                for suite in ET.parse(report).getroot().iter("testsuite"):
                    for key in totals:
                        totals[key] += int(suite.attrib.get(key, "0"))
            manifest["m1"] = {**totals, "exit_code": result.returncode}
            manifest["case_count"] = totals["tests"]
            manifest["iteration_count"] = totals["tests"]
            manifest["passed_count"] = (
                totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"]
            )
            manifest["failed"] = totals["failures"] + totals["errors"]
            if result.returncode:
                raise RuntimeError("M1 E2E failed; M2 experiments were not started")
        if args.stage in ("m2", "all"):
            from sandbox_m2_suite import main as m2

            summary = asyncio.run(m2(args.output / "m2"))
            manifest["m2"] = summary
            manifest["case_count"] = len(summary["cases"])
            manifest["iteration_count"] = summary["attempted"]
            manifest["concurrency"] = 20
            manifest["failed"] = summary["failed"]
            manifest["passed_count"] = summary["attempted"] - summary["failed"]
            manifest["resource_profile"] = summary["resource_profile"]
            manifest["policy_version"] = summary["policy_version"]
            if not summary["passed"]:
                raise RuntimeError("M2 did not meet every acceptance threshold")
        remaining = asyncio.run(inventory())
        manifest["remaining_resources"] = remaining
        manifest["leaked_resources"] = sum(len(items) for items in remaining.values())
        if manifest["leaked_resources"]:
            raise RuntimeError("Sandbox namespace contains resources after cleanup")
        manifest["passed"] = True
        manifest["failed"] = 0
        code = 0
    except Exception as exc:
        manifest["failure"] = {"error_type": type(exc).__name__, "message": str(exc)[:1000]}
        try:
            remaining = asyncio.run(inventory())
            manifest["remaining_resources"] = remaining
            manifest["leaked_resources"] = sum(len(items) for items in remaining.values())
        except Exception:
            manifest["leaked_resources"] = None
    finally:
        manifest["finished_at"] = datetime.now(UTC).isoformat()
        manifest["exit_code"] = code
        manifest["raw_receipt_paths"] = [
            str(path.relative_to(args.output))
            for path in sorted(args.output.rglob("*"))
            if path.is_file() and path.suffix in {".jsonl", ".json", ".xml"}
        ]
        (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps(manifest), flush=True)
        export(args.output)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
