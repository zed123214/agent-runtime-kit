"""Launch or collect one explicitly named validation Job without pods/exec."""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import tarfile
from datetime import UTC, datetime

import yaml
from sandbox_m2_cluster import ROOT, Cluster


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("launch", "collect"))
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--build-run", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--stage", choices=("m1", "m2", "all"), default="m1")
    args = parser.parse_args()
    cluster = Cluster(args.kubeconfig, args.context)
    out = ROOT / "artifacts/sandbox-validation" / args.build_run
    if args.action == "launch":
        pods = cluster.obj(
            "-n", "kitagent-core", "get", "pods", "-l", "app.kubernetes.io/name=agentrt-core"
        )
        active = [
            p["metadata"]["name"]
            for p in pods["items"]
            if p.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
        ]
        if active:
            raise RuntimeError(f"A Core owner is already active: {active}")
        environment = json.loads((out / "environment.json").read_text())
        checks = json.loads((out / "admission-rbac.json").read_text())
        assert len(checks) == 18 and all(row["passed"] for row in checks), (
            "admission checks did not pass"
        )
        assert (
            environment["cluster_uid"]
            == cluster.obj("get", "namespace", "kube-system")["metadata"]["uid"]
        ), "environment belongs to a different cluster"
        assert (
            datetime.now(UTC) - datetime.fromisoformat(environment["attested_at"])
        ).total_seconds() < 900, "repeat CNI attestation before launching"
        cni = cluster.obj("-n", "kube-system", "get", "daemonset", "cilium")
        assert cni["metadata"]["uid"] == environment["cni"]["uid"]
        assert [c["image"] for c in cni["spec"]["template"]["spec"]["containers"]] == environment[
            "cni"
        ]["images"]
        assert cni["status"]["numberReady"] == cni["status"]["desiredNumberScheduled"]
        config = cluster.obj("-n", "kube-system", "get", "configmap", "cilium-config")["data"]
        assert (
            hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
            == environment["cni_config_sha256"]
        ), "CNI configuration changed"
        stored_policies = json.loads((out / "network-policies.json").read_text())["items"]
        current_policies = cluster.obj("-n", "kitagent-sandboxes", "get", "networkpolicies")[
            "items"
        ]
        assert {p["metadata"]["name"]: p["spec"] for p in stored_policies} == {
            p["metadata"]["name"]: p["spec"] for p in current_policies
        }, "NetworkPolicy changed"
        for role, image in environment["images"].items():
            info = json.loads((out / f"image-{role}.json").read_text())[0]
            assert image in info["RepoDigests"]
            assert (
                info["Config"]["Labels"]["dev.agentrt.source-sha256"]
                == environment["source_snapshot_sha256"]
            )
        job = yaml.safe_load((out / "rendered/validation/job.yaml").read_text())
        assert (
            job["spec"]["template"]["spec"]["containers"][0]["image"]
            == environment["images"]["validation"]
        )
        environment["core_execution_resources"] = job["spec"]["template"]["spec"]["containers"][0][
            "resources"
        ]
        # Each Job gets an immutable copy. A subsequent build cannot rewrite
        # the environment evidence observed by an already-running Job.
        encoded_environment = json.dumps(environment, sort_keys=True)
        config_name = (
            "agentrt-env-"
            + hashlib.sha256((args.job + encoded_environment).encode()).hexdigest()[:16]
        )
        cluster.create(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": config_name, "namespace": "kitagent-core"},
                "immutable": True,
                "data": {"environment.json": encoded_environment},
            }
        )
        for volume in job["spec"]["template"]["spec"]["volumes"]:
            if volume["name"] == "environment":
                volume["configMap"]["name"] = config_name
        job["metadata"]["name"] = args.job
        job["spec"].pop("ttlSecondsAfterFinished", None)
        job["spec"]["activeDeadlineSeconds"] = 3600
        job["spec"]["template"]["spec"]["containers"][0]["args"] = ["--stage", args.stage]
        (out / f"{args.job}.json").write_text(json.dumps(job, indent=2) + "\n")
        cluster.create(job)
        print("Started", args.job)
    else:
        job = cluster.obj("-n", "kitagent-core", "get", "job", args.job)
        (out / f"{args.job}-status.json").write_text(json.dumps(job, indent=2) + "\n")
        raw = cluster.call("-n", "kitagent-core", "logs", "job/" + args.job)
        (out / f"{args.job}.log").write_text(raw)
        if "ARTIFACT_TGZ_BASE64_END" not in raw:
            print("Job has not exported artifacts; saved current log")
            print(raw[-5000:])
            return
        digest = raw.split("ARTIFACT_TGZ_SHA256 ", 1)[1].splitlines()[0]
        data = base64.b64decode(
            raw.split("ARTIFACT_TGZ_BASE64_BEGIN\n", 1)[1].split("ARTIFACT_TGZ_BASE64_END", 1)[0]
        )
        assert hashlib.sha256(data).hexdigest() == digest
        destination = out / args.job
        destination.mkdir(exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            archive.extractall(destination, filter="data")
        print((destination / "manifest.json").read_text())


if __name__ == "__main__":
    main()
