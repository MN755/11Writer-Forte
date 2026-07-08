FROM python:3.11-slim

WORKDIR /srv/11writer

COPY alembic.ini ./
COPY pyproject.toml ./
COPY src ./src

RUN python -m pip install --upgrade pip && \
    python -m pip install -e .

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
