FROM python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv
COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir -r requirements.txt -c requirements.lock
COPY app ./app
COPY config /srv/config
RUN useradd --system --uid 10001 gateway
USER gateway
EXPOSE 8080
# One worker: idempotency and lease state are per process in Phase 2 (see GATEWAY.md, limitations).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--timeout-graceful-shutdown", "20"]
