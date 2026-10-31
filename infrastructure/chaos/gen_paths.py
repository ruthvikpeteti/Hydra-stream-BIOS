#!/usr/bin/env python3
"""Generates the path registry seed (policy/config/seed-paths.json) and the Toxiproxy proxy list
(infrastructure/chaos/toxiproxy.json): three egress paths per ordered region pair, each its own proxy port.

  DIRECT   plaintext, cost 1      PEERING  encrypted private peering, cost 3      RELAY  encrypted relay via the third region, cost 2 (fallback)

Port scheme: 18000 + 100*source + 10*target + path, with a=1, b=2, c=3 and direct=1, peering=2, relay=3."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REGIONS = {"a": "region-a", "b": "region-b", "c": "region-c"}
LINK_ZONES = {
    ("a", "b"): ["EU"],
    ("b", "a"): ["EU"],
    ("a", "c"): ["EU", "ATLANTIC", "US"],
    ("c", "a"): ["US", "ATLANTIC", "EU"],
    ("b", "c"): ["EU", "ATLANTIC", "US"],
    ("c", "b"): ["US", "ATLANTIC", "EU"],
}
paths, proxies = [], []
for s, sr in REGIONS.items():
    for d, dr in REGIONS.items():
        if s == d:
            continue
        third = next(x for x in REGIONS if x not in (s, d))
        relay_zones = sorted(set(LINK_ZONES[(s, third)]) | set(LINK_ZONES[(third, d)]))
        for k, (kind, zones, via, enc, cost, fb) in enumerate(
            (
                ("direct", LINK_ZONES[(s, d)], [], False, 1.0, False),
                ("peering", LINK_ZONES[(s, d)], [], True, 3.0, False),
                ("relay", relay_zones, [REGIONS[third]], True, 2.0, True),
            ),
            start=1,
        ):
            port = 18000 + 100 * ("abc".index(s) + 1) + 10 * ("abc".index(d) + 1) + k
            pid = f"{s}-{d}-{kind}"
            paths.append(
                {
                    "path_id": pid,
                    "source_region": sr,
                    "target_region": dr,
                    "kind": kind.upper(),
                    "via_regions": via,
                    "transit_zones": zones,
                    "endpoint_url": f"http://ss-toxiproxy:{port}",
                    "encrypted": enc,
                    "cost": cost,
                    "fallback": fb,
                    "enabled": True,
                }
            )
            proxies.append({"name": pid, "listen": f"0.0.0.0:{port}", "upstream": f"ss-{d}-agent:8091", "enabled": True})
(ROOT / "policy/config/seed-paths.json").write_text(json.dumps({"paths": paths}, indent=2) + "\n")
(ROOT / "infrastructure/chaos/toxiproxy.json").write_text(json.dumps(proxies, indent=2) + "\n")
print(f"{len(paths)} paths, {len(proxies)} proxies")
