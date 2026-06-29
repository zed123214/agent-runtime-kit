FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY scripts ./scripts
COPY .env.example ./

RUN uv sync --frozen --no-dev

EXPOSE 7437

CMD ["uv", "run", "agentrt-core"]
