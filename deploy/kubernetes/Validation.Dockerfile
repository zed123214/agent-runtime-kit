ARG PYTHON_BASE=python:3.12-slim
FROM ${PYTHON_BASE}
WORKDIR /app
RUN pip install --no-cache-dir uv==0.12.10
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY tests ./tests
COPY scripts ./scripts
COPY build-source.json ./build-source.json
COPY deploy ./deploy
COPY README.en.md ./README.en.md
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --extra kubernetes --extra sandbox-worker --no-install-package mypy --no-install-package ruff
ARG COMMIT_SHA
ARG SOURCE_SHA256
LABEL org.opencontainers.image.revision=${COMMIT_SHA} dev.agentrt.source-sha256=${SOURCE_SHA256}
ENV PATH="/app/.venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 VALIDATION_COMMIT=${COMMIT_SHA}
USER 10000:10000
ENTRYPOINT ["python", "scripts/run_sandbox_validation.py", "--enable-kubernetes"]
