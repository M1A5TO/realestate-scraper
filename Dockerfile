FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# system deps for lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY scrapper ./scrapper

RUN pip install --upgrade pip \
    && pip install .

# default: show help
ENTRYPOINT ["python", "-m", "scrapper.cli"]
CMD ["--help"]
