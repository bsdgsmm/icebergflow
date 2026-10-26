FROM python:3.12-slim-bookworm

# Spark 4.0 needs a Java 17+ runtime; `java` on PATH is enough, JAVA_HOME is optional.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 1000 lakehouse
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"

# Fetch the Iceberg runtime jar at build time so containers run offline.
RUN icebergflow setup

COPY sql ./sql
COPY tests ./tests
COPY benchmarks ./benchmarks
COPY data/sample ./data/sample

RUN mkdir -p warehouse data/raw && chown -R lakehouse:lakehouse /app
USER lakehouse

ENTRYPOINT ["icebergflow"]
CMD ["--help"]
