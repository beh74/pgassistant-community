# Use a specific lightweight base image
FROM python:3.12-alpine

# Install only the runtime system packages. PostgreSQL 18 client tools can
# connect to older supported servers and avoid installing several client sets.
RUN apk add --no-cache \
    bash \
    postgresql18-client

# Install Python dependencies globally: a container is already isolated, so a
# virtualenv would duplicate pip and its metadata. Bytecode is omitted to keep
# the runtime layer compact.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --no-compile -r /tmp/requirements.txt && \
    python -m compileall -q /usr/local/lib/python3.12/site-packages/sql_formatter && \
    rm /tmp/requirements.txt

# Create a non-root user for security
RUN addgroup -S pgassistant && adduser -S pgassistant -G pgassistant -h /home/pgassistant
USER pgassistant

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FLASK_APP=run.py \
    DEBUG=False \
    PGA_POSTGRESQL_VERSIONS_CACHE_FILE=/home/pgassistant/data/postgresql_versions_cache.json

WORKDIR /home/pgassistant

# Runtime data is kept outside the application sources and remains writable
# by the non-root pgassistant user. The release cache is created on first use.
RUN mkdir -p /home/pgassistant/data

# Copy the runtime application context. Development files, documentation,
# tests, local configuration and Docker Compose sources are excluded by
# .dockerignore so they are never added to an image layer.
COPY --chown=pgassistant:pgassistant . /home/pgassistant/

# Expose application port
EXPOSE 5005

# Define entry point for the application
ENTRYPOINT ["gunicorn", "--config", "gunicorn-cfg.py", "run:app"]
