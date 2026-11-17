# External builder only. Context is a reviewed, sanitized source export plus bundle/.
# Supply an approved immutable Python 3.12 Linux/amd64 base reference.
ARG PYTHON_BASE
FROM ${PYTHON_BASE}
ARG SOURCE_REVISION=4ff43a92d8b71640a13c3f531599b12597a5d5e6
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
WORKDIR /opt/gateway
COPY bundle/ /bundle/
# SHA256SUMS covers uv, docker and every wheel, verified against approved sources.
RUN cd /bundle && sha256sum -c SHA256SUMS
COPY pyproject.toml uv.lock alembic.ini ./
COPY app/ ./app/
COPY tests/ ./tests/
COPY bundle/source.sha256 /tmp/source.sha256
RUN test "$SOURCE_REVISION" = 4ff43a92d8b71640a13c3f531599b12597a5d5e6 \
 && sha256sum -c /tmp/source.sha256 \
 && install -m 755 /bundle/uv /usr/local/bin/uv \
 && install -m 755 /bundle/docker /usr/local/bin/docker \
 && python -c "import sys; assert sys.version_info[:2] == (3,12)" \
 && uv venv --python /usr/local/bin/python /opt/cert-venv \
 && uv export --offline --frozen --all-groups --no-emit-project --format requirements-txt --output-file /tmp/locked.txt \
 && uv pip sync --python /opt/cert-venv/bin/python --no-index --find-links /bundle/wheels --require-hashes /tmp/locked.txt \
 && uv pip check --python /opt/cert-venv/bin/python \
 && /opt/cert-venv/bin/python -c "import redis,fastapi,uvicorn,pytest,pytest_asyncio,psycopg,app.server; assert redis.__version__ == '8.1.0'"
ENV PATH="/opt/cert-venv/bin:${PATH}" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
STOPSIGNAL SIGTERM
# No shell/uv launcher between Docker SIGTERM and the managed server.
ENTRYPOINT ["python", "-B", "-m", "app.server"]
CMD ["--host", "0.0.0.0", "--port", "8000"]
