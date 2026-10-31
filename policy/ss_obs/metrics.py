"""Prometheus text exposition with a label policy: only low-cardinality, non-sensitive labels, and values that cannot be an event id,
a customer identifier or a free-form string. A violation raises: a metric that breaks the policy is a bug, not something to export."""

import math
import re
from collections import defaultdict

ALLOWED_LABELS = frozenset(
    {"region", "source", "target", "path", "state", "operation", "decision", "worker", "status", "from", "to", "le", "point", "outcome", "classification"}
)
VALUE_RE = re.compile(r"^[A-Za-z0-9_.:+-]{1,48}$")
UUID_LIKE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-|[0-9a-fA-F]{24,}")


class LabelPolicyError(ValueError):
    pass


def _check(labels: dict) -> None:
    for k, v in labels.items():
        if k not in ALLOWED_LABELS:
            raise LabelPolicyError(f"label {k!r} is not allowed")
        v = str(v)
        if not VALUE_RE.match(v) or UUID_LIKE.search(v):
            raise LabelPolicyError(f"label value {v!r} for {k!r} looks like an identifier or free text")


def _fmt(v: float) -> str:
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return "NaN" if math.isnan(v) else ("+Inf" if v > 0 else "-Inf")
    return repr(float(v)) if isinstance(v, float) else str(v)


class Registry:
    def __init__(self) -> None:
        self._help: dict[str, str] = {}
        self._type: dict[str, str] = {}
        self._samples: dict[str, list[tuple[dict, float]]] = defaultdict(list)
        self._raw: list[str] = []

    def add(self, name: str, mtype: str, help_: str, value: float, **labels) -> None:
        _check(labels)
        self._help.setdefault(name, help_)
        self._type.setdefault(name, mtype)
        self._samples[name].append((labels, value))

    def add_raw_histogram(self, lines: list[str]) -> None:
        """Gateway histogram lines are passed through only after every label passes the same policy."""
        for ln in lines:
            if ln.startswith("#") or not ln.strip():
                if ln not in self._raw:  # one HELP/TYPE per metric even when several gateways report it
                    self._raw.append(ln)
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+(\S+)$", ln)
            if not m:
                raise LabelPolicyError(f"unparseable metric line {ln[:60]!r}")
            labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2)))
            _check(labels)
            self._raw.append(ln)

    def render(self) -> str:
        out: list[str] = []
        for name in sorted(self._samples):
            out += [f"# HELP {name} {self._help[name]}", f"# TYPE {name} {self._type[name]}"]
            for labels, v in self._samples[name]:
                lab = ",".join(f'{k}="{labels[k]}"' for k in sorted(labels))
                out.append(f"{name}{{{lab}}} {_fmt(v)}" if lab else f"{name} {_fmt(v)}")
        out += self._raw
        return "\n".join(out) + "\n"
