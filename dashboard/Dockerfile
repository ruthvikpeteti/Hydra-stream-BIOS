# Build context: the repository root (docker compose: context "..", dockerfile dashboard/Dockerfile).
FROM node:22-alpine@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914 AS ui
WORKDIR /ui
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/index.html dashboard/tsconfig.json dashboard/vite.config.ts ./
COPY dashboard/src ./src
RUN npm run build

FROM python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv
COPY policy/requirements.txt policy/requirements.lock ./
RUN pip install --no-cache-dir -r requirements.txt -c requirements.lock
COPY policy/ss_dr ./ss_dr
COPY policy/ss_obs ./ss_obs
COPY --from=ui /ui/dist /srv/dashboard
RUN useradd --system --uid 10003 dash
USER dash
ENV SS_DASH_STATIC=/srv/dashboard
CMD ["uvicorn", "ss_obs.main:app", "--host", "0.0.0.0", "--port", "8200", "--workers", "1"]
