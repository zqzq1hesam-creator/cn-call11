FROM python:3.12-slim

WORKDIR /app

COPY server/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY server ./server

ENV PYTHONPATH=/app

CMD ["sh", "-c", "uvicorn server.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
