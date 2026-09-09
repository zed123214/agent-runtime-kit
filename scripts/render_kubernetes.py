"""Render deployment files for review; does not create resources or run checks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from string import Template

from agent_runtime.core.sandbox.config import SandboxConfig, validate_sandbox_config
from agent_runtime.core.sandbox.pod_spec import pod_spec, service_spec


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--worker-image", required=True, help="repository@sha256:digest")
    parser.add_argument("--core-image", required=True, help="repository@sha256:digest")
    parser.add_argument("--validation-image", help="optional E2E image, repository@sha256:digest")
    parser.add_argument(
        "--network-policy-verified",
        action="store_true",
        help="Operator attestation after CNI enforcement validation",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = SandboxConfig(deployment_scope=args.scope)
    config.kubernetes.image = args.worker_image
    validate_sandbox_config(config, dependencies=False)
    if args.validation_image:
        config.kubernetes.image = args.validation_image
        validate_sandbox_config(config, dependencies=False)
    config.kubernetes.image = args.core_image
    validate_sandbox_config(config, dependencies=False)
    if not args.scope or not args.worker_image or not args.core_image:
        parser.error("scope and digest-pinned images are required")
    config.kubernetes.image = args.worker_image
    values = {
        "DEPLOYMENT_SCOPE": args.scope,
        "CORE_IMAGE": args.core_image,
        "WORKER_IMAGE": args.worker_image,
        "NETWORK_POLICY_VERIFIED": str(args.network_policy_verified).lower(),
    }
    if args.validation_image:
        values["VALIDATION_IMAGE"] = args.validation_image
    source = Path(__file__).resolve().parents[1] / "deploy" / "kubernetes"
    args.output.mkdir(parents=True, exist_ok=True)
    for name in (
        "namespaces.yaml",
        "rbac.yaml",
        "limits.yaml",
        "network-policy.yaml.template",
        "core.yaml.template",
    ):
        rendered = Template((source / name).read_text(encoding="utf-8")).substitute(values)
        (args.output / name.removesuffix(".template")).write_text(rendered, encoding="utf-8")
    if args.validation_image:
        # Separate directory: applying the normal deployment must never start
        # E2E or create a second active Core by accident.
        validation_dir = args.output / "validation"
        validation_dir.mkdir(exist_ok=True)
        template = (source / "validation-job.yaml.template").read_text(encoding="utf-8")
        (validation_dir / "job.yaml").write_text(
            Template(template).substitute(values), encoding="utf-8"
        )
    # Inspection-only examples live outside the apply directory; Core signs real
    # resource metadata and adds the exact Pod UID to the Service ownerReference.
    examples = {
        "podSpec": pod_spec(config, "0" * 32, "sb-" + "0" * 32),
        "serviceSpec": service_spec(config, "0" * 32),
    }
    (args.output / "sandbox-specs.json.txt").write_text(
        json.dumps(examples, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
