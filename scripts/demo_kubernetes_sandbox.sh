#!/usr/bin/env bash
# An explicit-context, offline demonstration of the Session/Worker lifecycle.
set -euo pipefail
run=${1:?usage: demo_kubernetes_sandbox.sh BUILD_RUN JOB_NAME}
job=${2:?supply a new Kubernetes Job name}
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
python=${AGENTRT_BUILD_PYTHON:-python3}
kubeconfig=${AGENTRT_M2_KUBECONFIG:?set the validation kubeconfig path}
context=${AGENTRT_M2_CONTEXT:?set the validation context}
"$python" "$repo/scripts/sandbox_m2_cluster.py" --kubeconfig "$kubeconfig" --context "$context" --build-run "$run"
"$python" "$repo/scripts/sandbox_m2_run.py" launch --kubeconfig "$kubeconfig" --context "$context" --build-run "$run" --job "$job" --stage m1
printf 'Follow creation, shared workspace, child agents and cleanup:\n'
kubectl --kubeconfig "$kubeconfig" --context "$context" -n kitagent-core wait --for=condition=Ready pod -l "job-name=$job" --timeout=180s
kubectl --kubeconfig "$kubeconfig" --context "$context" -n kitagent-core logs -f "job/$job" | sed '/^ARTIFACT_TGZ_BASE64_BEGIN$/,$d'
"$python" "$repo/scripts/sandbox_m2_run.py" collect --kubeconfig "$kubeconfig" --context "$context" --build-run "$run" --job "$job"
