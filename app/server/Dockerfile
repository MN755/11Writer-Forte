FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/11writer

COPY pyproject.toml ./
COPY alembic.ini ./
COPY alembic ./alembic
COPY src ./src
COPY data ./data

RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir .

EXPOSE 8000

CMD ["/bin/sh", "-c", "11writer db-bootstrap && 11writer serve --host 0.0.0.0 --port 8000"]
