"""Nothing that leaves the dashboard API may carry a message payload or a credential. Two layers: the collector builds every view
from an explicit whitelist of fields, and this module scrubs whatever is left (forbidden keys, and any value equal to a known secret)."""

from typing import Any, Iterable

FORBIDDEN_KEYS = frozenset(
    {
        "payload",
        "payloads",
        "value",
        "values",
        "records",
        "record",
        "partition_key",
        "key",
        "keys",
        "body",
        "message",
        "messages",
        "api_key",
        "apikey",
        "x-api-key",
        "x-service-key",
        "x-admin-key",
        "authorization",
        "password",
        "passwd",
        "secret",
        "secrets",
        "token",
        "private_key",
        "credentials",
        "envelope",
    }
)
MIN_SECRET_LEN = 8


def scrub(obj: Any, secrets: Iterable[str] = ()) -> Any:
    sec = [s for s in secrets if s and len(s) >= MIN_SECRET_LEN]

    def walk(o: Any) -> Any:
        if isinstance(o, dict):
            return {k: walk(v) for k, v in o.items() if str(k).lower() not in FORBIDDEN_KEYS}
        if isinstance(o, (list, tuple)):
            return [walk(v) for v in o]
        if isinstance(o, str):
            for s in sec:
                if s in o:
                    o = o.replace(s, "[redacted]")
            return o
        return o

    return walk(obj)


def find_violations(obj: Any, secrets: Iterable[str] = (), path: str = "$") -> list[str]:
    """For tests: every place a forbidden key or a known secret survives."""
    sec = [s for s in secrets if s and len(s) >= MIN_SECRET_LEN]
    out: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in FORBIDDEN_KEYS:
                out.append(f"{path}.{k}")
            out += find_violations(v, sec, f"{path}.{k}")
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            out += find_violations(v, sec, f"{path}[{i}]")
    elif isinstance(obj, str):
        out += [f"{path} contains a secret" for s in sec if s in obj]
    return out
