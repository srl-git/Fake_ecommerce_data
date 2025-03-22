FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY api/requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY . /app
COPY /google_cloud /app/google_cloud

RUN adduser -u 5679 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

EXPOSE 8080

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]