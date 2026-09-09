#!/usr/bin/env bash
# Run in Linux/WSL. No push; import the exact locally built images into kind.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
run=${1:?usage: sandbox_m2_build.sh RUN_ID}
[[ "$run" =~ ^[a-z0-9-]+$ ]] || exit 2
python=${AGENTRT_BUILD_PYTHON:-python3}
build_root=${AGENTRT_BUILD_ROOT:-/root/.local/share/agentrt-m2/builds}
snapshot="$build_root/$run"
artifacts="$repo/artifacts/sandbox-validation/$run"
mkdir -p "$artifacts"
if [[ ! -f "$snapshot/build-source.json" ]]; then
  "$python" "$repo/scripts/snapshot_sandbox_source.py" --destination "$snapshot" --archive "$artifacts/source.tar.gz" > "$artifacts/snapshot.log"
fi
commit=$("$python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["commit_sha"])' "$snapshot/build-source.json")
source_sha=$("$python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["snapshot_sha256"])' "$snapshot/build-source.json")
base='python@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea'
for role in worker core validation; do
  case "$role" in
    worker) dockerfile=src/agent_runtime/sandbox_server/Dockerfile ;;
    core) dockerfile=deploy/kubernetes/Core.Dockerfile ;;
    validation) dockerfile=deploy/kubernetes/Validation.Dockerfile ;;
  esac
  if [[ ! -f "$artifacts/image-$role.json" ]]; then
    if [[ -f "$artifacts/build-$role.log" ]]; then
      cp "$artifacts/build-$role.log" "$artifacts/build-$role.failed-$(date -u +%Y%m%dT%H%M%S).log"
    fi
    docker build --provenance=false --progress=plain --build-arg "PYTHON_BASE=$base" --build-arg "COMMIT_SHA=$commit" --build-arg "SOURCE_SHA256=$source_sha" -f "$snapshot/$dockerfile" -t "agentrt-m2-$role:$run" "$snapshot" > "$artifacts/build-$role.log" 2>&1
    docker image inspect "agentrt-m2-$role:$run" > "$artifacts/image-$role.json"
  fi
  kind load docker-image "agentrt-m2-$role:$run" --name agentrt-sandbox-e2e > "$artifacts/load-$role.log" 2>&1
  "$python" "$repo/scripts/import_sandbox_digest.py" --image "agentrt-m2-$role:$run" --inspect-file "$artifacts/image-$role.json" >> "$artifacts/load-$role.log" 2>&1
  printf 'Built and loaded %s\n' "$role"
done
printf 'Build artifacts: %s\n' "$artifacts"
