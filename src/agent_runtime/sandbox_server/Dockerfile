# Build from the repository root. Supply a verified Python base digest too.
ARG PYTHON_BASE=python:3.12-slim
FROM ${PYTHON_BASE}
WORKDIR /app
RUN pip install --no-cache-dir uv==0.12.10
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra sandbox-worker
ARG COMMIT_SHA
ARG SOURCE_SHA256
LABEL org.opencontainers.image.revision=${COMMIT_SHA} dev.agentrt.source-sha256=${SOURCE_SHA256}
ENV PATH="/app/.venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["python", "-m", "agent_runtime.sandbox_server"]
CMD ["executor"]
