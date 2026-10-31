#!/usr/bin/env bash
# Generates one unique KRaft cluster ID per region into infrastructure/.env.
# Existing IDs are kept (changing an ID on existing volumes makes the brokers refuse to start).
# Use --force only together with `down.sh --volumes`.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/.env"
force=0; [[ "${1:-}" == "--force" ]] && force=1

new_id() { python3 -c 'import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="))'; }

if [[ -f "$ENV_FILE" && $force -eq 0 ]]; then
  echo "Keeping existing $ENV_FILE"
else
  a=$(new_id); b=$(new_id); c=$(new_id)
  while [[ "$a" == "$b" || "$a" == "$c" || "$b" == "$c" ]]; do b=$(new_id); c=$(new_id); done
  printf 'CLUSTER_ID_A=%s\nCLUSTER_ID_B=%s\nCLUSTER_ID_C=%s\n' "$a" "$b" "$c" > "$ENV_FILE"
  echo "Wrote $ENV_FILE"
fi
# Client API key for the gateway (X-API-Key). Kept if present.
grep -q '^SS_GATEWAY_API_KEY=' "$ENV_FILE" || printf 'SS_GATEWAY_API_KEY=%s\n' "$(python3 -c 'import secrets;print(secrets.token_urlsafe(24))')" >> "$ENV_FILE"
# Phase 3 credentials (each kept if present): PostgreSQL password and the three policy-engine API roles.
for var in POSTGRES_PASSWORD SS_POLICY_ADMIN_KEY SS_POLICY_WRITER_KEY SS_POLICY_SERVICE_KEY SS_REPL_KEY_A SS_REPL_KEY_B SS_REPL_KEY_C SS_DASH_VIEWER_TOKEN SS_DASH_OPERATOR_TOKEN; do
  grep -q "^$var=" "$ENV_FILE" || printf '%s=%s\n' "$var" "$(python3 -c 'import secrets;print(secrets.token_urlsafe(24))')" >> "$ENV_FILE"
done
# Ed25519 key pair for signed policy bundles: the private key is mounted into the engine only; agents get the public key.
KEYS="$ROOT/config/keys"; mkdir -p "$KEYS"
if [[ ! -f "$KEYS/bundle-private.pem" || $force -eq 1 ]]; then
  openssl genpkey -algorithm ed25519 -out "$KEYS/bundle-private.pem"
  openssl pkey -in "$KEYS/bundle-private.pem" -pubout -out "$KEYS/bundle-public.pem"
  chmod 644 "$KEYS"/*.pem        # development only: containers run as non-root users; use a secrets manager in production
  echo "Generated bundle signing keys in $KEYS"
fi
# Show the file with every secret masked.
awk -F= '{ if ($1 ~ /(KEY|PASSWORD|KEY_[A-Z]|TOKEN)$/) print "  " $1 "=<hidden, see .env>"; else print "  " $0 }' "$ENV_FILE"
