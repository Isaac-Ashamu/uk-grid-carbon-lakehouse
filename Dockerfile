# syntax=docker/dockerfile:1.7
# Runtime image for the gridcarbon CLI and API. dbt and DuckDB are pure pip
# installs, so one small image serves the pipeline, the API and CI.

FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml README.md ./
COPY src ./src
RUN uv venv /opt/venv && VIRTUAL_ENV=/opt/venv uv pip install --no-cache .

FROM python:3.12-slim AS runtime
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    GRIDCARBON_DATA_ROOT=/opt/gridcarbon/data \
    GRIDCARBON_DBT_PROJECT_DIR=/opt/gridcarbon/dbt \
    GRIDCARBON_LOG_JSON=1
RUN useradd --create-home --uid 1000 gridcarbon \
    && mkdir -p /opt/gridcarbon/data && chown -R gridcarbon /opt/gridcarbon
COPY --from=builder /opt/venv /opt/venv
COPY --chown=gridcarbon dbt /opt/gridcarbon/dbt
USER gridcarbon
WORKDIR /opt/gridcarbon
VOLUME ["/opt/gridcarbon/data"]
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').status == 200 else 1)" || exit 1
ENTRYPOINT ["gridcarbon"]
CMD ["serve"]
