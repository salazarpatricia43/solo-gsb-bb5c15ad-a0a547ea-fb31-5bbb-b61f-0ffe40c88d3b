FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SENSOR_HOST=0.0.0.0 \
    SENSOR_PORT=8000 \
    SENSOR_DB_PATH=/data/sensors.db

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# The SQLite database (plus WAL files) lives here; mount a volume to persist.
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# init_db runs inside the FastAPI lifespan handler, so a fresh database and
# schema are created automatically on container start.
CMD ["sh", "-c", "uvicorn app.main:app --host ${SENSOR_HOST} --port ${SENSOR_PORT}"]
