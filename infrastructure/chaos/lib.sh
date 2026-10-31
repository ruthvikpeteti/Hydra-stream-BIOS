#!/usr/bin/env bash
# Shared helpers for the chaos scripts. Toxiproxy API: http://localhost:8474 (override with TOXIPROXY_URL).
# A "proxy" is one replication path, named by its path id, e.g. a-b-direct, a-b-peering, a-b-relay (see gen_paths.py).
# Toxiproxy works on TCP streams: latency, bandwidth and resets are real, but "packet loss" is EMULATED as probabilistic
# connection resets (it cannot drop individual IP packets).
TOXI="${TOXIPROXY_URL:-http://localhost:8474}"

toxi() { curl -fsS -X "$1" "$TOXI$2" ${3:+-H 'Content-Type: application/json' -d "$3"}; }
add_toxic() {  # add_toxic <proxy> <name> <type> <stream> <toxicity> <attributes-json>
  toxi DELETE "/proxies/$1/toxics/$2" >/dev/null 2>&1 || true
  toxi POST "/proxies/$1/toxics" "{\"name\":\"$2\",\"type\":\"$3\",\"stream\":\"$4\",\"toxicity\":$5,\"attributes\":$6}" >/dev/null
}
need() { [[ -n "${1:-}" ]] || { echo "usage: $2" >&2; exit 2; }; }
