FROM python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv
COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir -r requirements.txt -c requirements.lock
COPY ss_policy ./ss_policy
COPY ss_replication ./ss_replication
COPY ss_dr ./ss_dr
COPY engine_service ./engine_service
COPY agent_service ./agent_service
COPY config /srv/config
RUN useradd --system --uid 10002 policy
USER policy
# Engine: uvicorn engine_service.main:app --port 8090      Agent: uvicorn agent_service.main:app --port 8091
CMD ["uvicorn", "engine_service.main:app", "--host", "0.0.0.0", "--port", "8090", "--workers", "1"]
