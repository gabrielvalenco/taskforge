# syntax=docker/dockerfile:1.7

# ---- Stage 1: instala as dependências num venv isolado ---------------------------------------
FROM python:3.13-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[all]"

# ---- Stage 2: imagem final, enxuta e sem privilégios -----------------------------------------
FROM python:3.13-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TASKFORGE_DATABASE_PATH=/data/taskforge.db \
    TASKFORGE_HEARTBEAT_FILE=/tmp/taskforge.heartbeat

RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app \
 && mkdir /data && chown app:app /data

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
USER app
VOLUME ["/data"]

# O worker toca o arquivo de heartbeat a cada iteração; se ele envelhecer, o loop travou.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["taskforge", "health", "--max-age", "90"]

ENTRYPOINT ["taskforge"]
CMD ["worker"]
