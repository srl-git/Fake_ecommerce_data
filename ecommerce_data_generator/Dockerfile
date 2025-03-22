FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY ecommerce_data_generator/requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY . /app
COPY /google_cloud /app/google_cloud

RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

CMD ["python3", "-m", "ecommerce_data_generator.main"]
