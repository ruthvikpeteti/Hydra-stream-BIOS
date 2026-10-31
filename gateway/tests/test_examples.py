"""The example policies in examples/policies must be accepted by the gateway's own validation."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "gateway"))
from app.models import TopicCreate  # noqa: E402
from app.registry import Registry, apply_defaults, validate_policy  # noqa: E402


@pytest.mark.parametrize("name", sorted(p.stem for p in (ROOT / "examples" / "policies").glob("*.json")))
def test_example_policy_is_valid(name):
    d = json.loads((ROOT / "examples" / "policies" / f"{name}.json").read_text())
    d.pop("_comment", None)
    m = TopicCreate(**d).model_dump()
    m.pop("name"), m.pop("partitions")
    reg = Registry.load(str(ROOT / "gateway" / "config" / "registry.json"))
    validate_policy(apply_defaults(m, reg, "region-a"), reg, "region-a")
