FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Install dependencies first so code changes don't invalidate this layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY jobq ./jobq
COPY fake_payments ./fake_payments
COPY migrations ./migrations
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
CMD ["uvicorn", "jobq.api:app", "--host", "0.0.0.0", "--port", "8000"]
