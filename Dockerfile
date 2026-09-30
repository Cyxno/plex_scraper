FROM python:3.12-slim-bookworm AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libfuse3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 \
    && rm -rf /var/lib/apt/lists/* \
    && sed -i 's/^#user_allow_other/user_allow_other/' /etc/fuse.conf

WORKDIR /app
COPY requirements.txt .
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels

COPY src/ /app/src/
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "plex_scraper.cli"]
