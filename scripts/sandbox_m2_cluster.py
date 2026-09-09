"""Explicit-context operator setup and independent CNI/admission attestation."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import platform
import secrets
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "sandbox.agentrt.dev/"


class Cluster:
    def __init__(self, kubeconfig: str, context: str) -> None:
        self.command = ["kubectl", "--kubeconfig", kubeconfig, "--context", context]

    def call(self, *args: str, body: dict[str, Any] | None = None, check: bool = True) -> str:
        result = subprocess.run(
            [*self.command, *args],
            input=json.dumps(body) if body is not None else None,
            text=True,
            capture_output=True,
            timeout=45,
        )
        if check and result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout if result.returncode == 0 else result.stdout + result.stderr

    def obj(self, *args: str) -> dict[str, Any]:
        return json.loads(self.call(*args, "-o", "json"))

    def create(self, body: dict[str, Any]) -> None:
        self.call("create", "-f", "-", body=body)

    def wait(self, ns: str, name: str, *, completed: bool = False) -> dict[str, Any]:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            pod = self.obj("-n", ns, "get", "pod", name)
            status = pod.get("status", {})
            if status.get("phase") in ("Succeeded", "Failed"):
                return pod
            if (
                not completed
                and status.get("podIP")
                and any(
                    c["type"] == "Ready" and c["status"] == "True"
                    for c in status.get("conditions", [])
                )
            ):
                return pod
            time.sleep(0.5)
        raise TimeoutError(f"Pod {ns}/{name} did not become ready/complete")


def probe_pod(ns: str, name: str, image: str, code: str, labels: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": ns, "name": name, "labels": labels},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 10001,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "probe",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["python", "-u", "-c", code],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "32Mi", "ephemeral-storage": "1Mi"},
                        "limits": {"cpu": "100m", "memory": "64Mi", "ephemeral-storage": "16Mi"},
                    },
                }
            ],
        },
    }


def attest(cluster: Cluster, image: str, scope: str, out: Path) -> None:
    core = "kitagent-core"
    sandbox = "kitagent-sandboxes"
    other = "agentrt-m2-probes"
    cluster.call(
        "apply",
        "-f",
        "-",
        body={
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": other,
                "labels": {"pod-security.kubernetes.io/enforce": "restricted"},
            },
        },
    )
    created: list[tuple[str, str]] = []
    samples: list[dict[str, Any]] = []
    label = {"app.kubernetes.io/name": "agentrt-core", PREFIX + "scope": scope}
    server_code = "import http.server; http.server.HTTPServer(('0.0.0.0',8080),http.server.BaseHTTPRequestHandler).serve_forever()"
    try:
        for ns, name, labels in [
            (sandbox, "m2-policy-worker", {PREFIX + "owner": "agent-runtime-kit"}),
            (core, "m2-policy-target", {"app": "m2-policy-target"}),
        ]:
            cluster.create(probe_pod(ns, name, image, server_code, labels))
            created.append((ns, name))
        worker = cluster.wait(sandbox, "m2-policy-worker")["status"]["podIP"]
        target = cluster.wait(core, "m2-policy-target")["status"]["podIP"]
        api = cluster.obj("-n", "default", "get", "service", "kubernetes")["spec"]["clusterIP"]
        cases = [
            (core, "allow-core-worker", label, worker, 8080, True),
            (core, "baseline-target", label, target, 8080, True),
            (core, "baseline-api", label, api, 443, True),
            (core, "deny-wrong-label", {"app": "untrusted"}, worker, 8080, False),
            (other, "deny-wrong-namespace", label, worker, 8080, False),
            (sandbox, "deny-egress-target", {}, target, 8080, False),
            (sandbox, "deny-egress-api", {}, api, 443, False),
        ]
        for ns, case, labels, host, port, expected in cases:
            code = (
                "import socket,json,time\n"
                "time.sleep(2)\n"
                "results=[]\n"
                "for i in range(3):\n"
                " start=time.monotonic()\n"
                f" try:\n  s=socket.create_connection(({host!r},{port}),2); s.close(); connected=True; error=None\n"
                " except OSError as e:\n  connected=False; error=type(e).__name__\n"
                " results.append(dict(connected=connected,error=error,duration_ms=(time.monotonic()-start)*1000))\n"
                f"print(json.dumps(dict(case={case!r},target={host!r},port={port},expected_connected={expected!r},samples=results)))\n"
                f"raise SystemExit(0 if all(r['connected']=={expected!r} for r in results) else 1)"
            )
            name = "m2-" + case
            cluster.create(probe_pod(ns, name, image, code, labels))
            created.append((ns, name))
            pod = cluster.wait(ns, name, completed=True)
            raw = cluster.call("-n", ns, "logs", name)
            sample = json.loads(raw)
            sample["pod_uid"] = pod["metadata"]["uid"]
            sample["passed"] = pod["status"]["phase"] == "Succeeded"
            samples.append(sample)
            (out / "cni-attestation.json").write_text(json.dumps(samples, indent=2) + "\n")
            print(case, sample["passed"], flush=True)
        if not all(sample["passed"] for sample in samples):
            raise RuntimeError("CNI attestation failed; do not enable sandbox execution")
    finally:
        for ns, name in reversed(created):
            cluster.call("-n", ns, "delete", "pod", name, "--ignore-not-found", "--wait=false")
    (out / "network-policies.json").write_text(
        cluster.call("-n", sandbox, "get", "networkpolicies", "-o", "json")
    )


def admission(cluster: Cluster, image: str, out: Path) -> None:
    rows = []
    identity = "system:serviceaccount:kitagent-core:agentrt-core"
    for verb, resource, ns, expected in [
        ("create", "pods", "kitagent-sandboxes", True),
        ("list", "secrets", "kitagent-sandboxes", True),
        ("list", "networkpolicies", "kitagent-sandboxes", True),
        ("create", "pods/exec", "kitagent-sandboxes", False),
        ("patch", "pods", "kitagent-sandboxes", False),
        ("create", "networkpolicies", "kitagent-sandboxes", False),
        ("get", "secrets", "kitagent-core", False),
        ("create", "deployments", "kitagent-sandboxes", False),
        ("get", "nodes", "kitagent-sandboxes", False),
    ]:
        resource_args = resource.split("/", 1)
        raw = cluster.call(
            "auth",
            "can-i",
            verb,
            resource_args[0],
            *(["--subresource", resource_args[1]] if len(resource_args) > 1 else []),
            "-n",
            ns,
            "--as",
            identity,
            check=False,
        )
        allowed = raw.strip().splitlines()[0] == "yes"
        rows.append(
            {
                "case": f"rbac:{verb}:{resource}:{ns}",
                "expected_allowed": expected,
                "raw": raw,
                "passed": allowed == expected,
            }
        )
    base = probe_pod("kitagent-sandboxes", "m2-admission", image, "pass", {})
    mutations = {
        "root_uid": lambda p: p["spec"]["securityContext"].update(runAsUser=0),
        "privilege_escalation": lambda p: p["spec"]["containers"][0]["securityContext"].update(
            allowPrivilegeEscalation=True
        ),
        "capabilities": lambda p: p["spec"]["containers"][0]["securityContext"][
            "capabilities"
        ].update(add=["NET_ADMIN"]),
        "unconfined": lambda p: p["spec"]["securityContext"].update(
            seccompProfile={"type": "Unconfined"}
        ),
        "host_pid": lambda p: p["spec"].update(hostPID=True),
        "host_path": lambda p: p["spec"].update(
            volumes=[{"name": "host", "hostPath": {"path": "/"}}]
        ),
        "cpu_max": lambda p: p["spec"]["containers"][0]["resources"]["limits"].update(cpu="3"),
        "memory_max": lambda p: p["spec"]["containers"][0]["resources"]["limits"].update(
            memory="2Gi"
        ),
        "storage_max": lambda p: p["spec"]["containers"][0]["resources"]["limits"].update(
            {"ephemeral-storage": "3Gi"}
        ),
    }
    for case, mutate in mutations.items():
        body = copy.deepcopy(base)
        mutate(body)
        raw = cluster.call("create", "--dry-run=server", "-f", "-", body=body, check=False)
        expected = "maximum" if case.endswith("_max") else "violates PodSecurity"
        rows.append(
            {"case": "admission:" + case, "raw": raw, "passed": expected.lower() in raw.lower()}
        )
    (out / "admission-rbac.json").write_text(json.dumps(rows, indent=2) + "\n")
    for kind in ("resourcequotas", "limitranges", "roles", "rolebindings"):
        (out / f"{kind}.json").write_text(
            cluster.call("-n", "kitagent-sandboxes", "get", kind, "-o", "json")
        )
    (out / "cni-daemonset.json").write_text(
        cluster.call("-n", "kube-system", "get", "daemonset", "cilium", "-o", "json")
    )
    if not all(row["passed"] for row in rows):
        raise RuntimeError("RBAC/admission attestation failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--build-run", required=True)
    parser.add_argument("--scope", default="agentrt-m2")
    parser.add_argument("--attest-only", action="store_true")
    args = parser.parse_args()
    cluster = Cluster(args.kubeconfig, args.context)
    active = cluster.call(
        "-n",
        "kitagent-core",
        "get",
        "pods",
        "-l",
        "app.kubernetes.io/name=agentrt-core",
        "-o",
        "json",
        check=False,
    )
    if active.startswith("{"):
        assert not any(
            p.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
            for p in json.loads(active)["items"]
        ), "stop the active Core/Job before preparing validation"
    out = ROOT / "artifacts" / "sandbox-validation" / args.build_run
    if (out / "cni-attestation.json").exists():
        history = out / "preflight-history" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        history.mkdir(parents=True)
        for name in (
            "cni-attestation.json",
            "network-policies.json",
            "environment.json",
            "admission-rbac.json",
            "cni-daemonset.json",
            "nodes.json",
            "resourcequotas.json",
            "limitranges.json",
            "roles.json",
            "rolebindings.json",
        ):
            if (out / name).is_file():
                shutil.copy2(out / name, history / name)
    images = {}
    for role in ("worker",) if args.attest_only else ("worker", "core", "validation"):
        info = json.loads((out / f"image-{role}.json").read_text())[0]
        images[role] = info["RepoDigests"][0]
    deploy = out / "rendered"
    # Render the policy before attestation. The confirmation remains false.
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/render_kubernetes.py"),
            "--scope",
            args.scope,
            "--worker-image",
            images["worker"],
            "--core-image",
            images.get("core", images["worker"]),
            "--output",
            str(deploy),
        ],
        check=True,
    )
    for name in ("namespaces", "rbac", "limits", "network-policy"):
        cluster.call("apply", "-f", str(deploy / f"{name}.yaml"))
    attest(cluster, images["worker"], args.scope, out)
    if args.attest_only:
        return
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/render_kubernetes.py"),
            "--scope",
            args.scope,
            "--worker-image",
            images["worker"],
            "--core-image",
            images["core"],
            "--validation-image",
            images["validation"],
            "--network-policy-verified",
            "--output",
            str(deploy),
        ],
        check=True,
    )
    key_dir = Path("/root/.local/share/agentrt-m2/secrets")
    key_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = key_dir / "ownership"
    if not key.exists():
        fd = os.open(key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(secrets.token_bytes(32))
    secret = cluster.call(
        "-n",
        "kitagent-core",
        "create",
        "secret",
        "generic",
        "agentrt-ownership",
        f"--from-file=key={key}",
        "--dry-run=client",
        "-o",
        "json",
    )
    cluster.call("apply", "-f", "-", body=json.loads(secret))
    nodes = cluster.obj("get", "nodes")
    cni = cluster.obj("-n", "kube-system", "get", "daemonset", "cilium")
    environment = {
        "os": platform.platform(),
        "cpu": next(
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        "logical_cpu_count": os.cpu_count(),
        "memory": Path("/proc/meminfo").read_text().splitlines()[0],
        "kubernetes_version": cluster.obj("version")["serverVersion"]["gitVersion"],
        "kind_version": subprocess.check_output(["kind", "version"], text=True).strip(),
        "cni": {
            "name": "cilium",
            "uid": cni["metadata"]["uid"],
            "labels": cni["metadata"].get("labels", {}),
            "images": [c["image"] for c in cni["spec"]["template"]["spec"]["containers"]],
            "status": cni["status"],
        },
        "cluster_uid": cluster.obj("get", "namespace", "kube-system")["metadata"]["uid"],
        "node_uids": [n["metadata"]["uid"] for n in nodes["items"]],
        "node_count": len(nodes["items"]),
        "images": images,
        "core_image_digest": images["validation"],
        "core_release_image_digest": images["core"],
        "core_execution_mode": "single validation Job executing Core source with offline providers",
        "sandbox_image_digest": images["worker"],
        "source_snapshot_sha256": json.loads((out / "source.tar.json").read_text())[
            "snapshot_sha256"
        ],
        "attested_at": datetime.now(UTC).isoformat(),
        "cni_config_sha256": hashlib.sha256(
            json.dumps(
                cluster.obj("-n", "kube-system", "get", "configmap", "cilium-config")["data"],
                sort_keys=True,
            ).encode()
        ).hexdigest(),
        "context": args.context,
        "cni_attestation": json.loads((out / "cni-attestation.json").read_text()),
    }
    (out / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    (out / "nodes.json").write_text(json.dumps(nodes, indent=2) + "\n")
    config_map = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "agentrt-validation-environment", "namespace": "kitagent-core"},
        "data": {"environment.json": json.dumps(environment)},
    }
    cluster.call("apply", "-f", "-", body=config_map)
    admission(cluster, images["worker"], out)
    print("Prepared deployment:", deploy, flush=True)


if __name__ == "__main__":
    main()
