FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY data_generator/requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY /data_generator /app/data_generator
COPY /shared /app/shared

RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

CMD ["python3", "-m", "data_generator.main"]
