#FROM astral/uv:python3.11-alpine AS builder
FROM python:3.11-slim AS builder

WORKDIR /app

# Install UV from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvbin/uv


# Copy only the dependency files first
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual env
RUN /uvbin/uv sync --frozen --no-install-project --no-dev

# Stage - 2 : Final lightweight image
FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
COPY . .
EXPOSE 8556
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8556", "--server.address=0.0.0.0"]

