"""Signed policy bundles. The engine signs with an Ed25519 private key; regional agents and other consumers
hold only the public key, so they can verify but never forge a bundle."""

import base64
import json
from dataclasses import dataclass, field

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from .models import Link, NetworkPath, Region, TopicPolicy


class BundleError(Exception):
    pass


def canonical(body: dict) -> bytes:
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sign_bundle(body: dict, private_pem: bytes, key_id: str = "bundle-1") -> dict:
    key = serialization.load_pem_private_key(private_pem, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise BundleError("bundle key must be Ed25519")
    return {"body": body, "key_id": key_id, "signature": base64.b64encode(key.sign(canonical(body))).decode()}


@dataclass(frozen=True)
class VerifiedBundle:
    bundle_version: int
    regions: dict[str, Region]
    links: dict[tuple[str, str], Link]
    policies: dict[str, TopicPolicy]
    paths: dict = field(default_factory=dict)  # path_id -> NetworkPath
    topology: dict = field(default_factory=dict)  # topic -> {active_region, leadership_epoch, replication_generation, flows[]}


def verify_bundle(signed: dict, public_pem: bytes) -> VerifiedBundle:
    try:
        pub = serialization.load_pem_public_key(public_pem)
        if not isinstance(pub, Ed25519PublicKey):
            raise BundleError("bundle public key must be Ed25519")
        pub.verify(base64.b64decode(signed["signature"]), canonical(signed["body"]))
    except (InvalidSignature, KeyError, ValueError, TypeError) as exc:
        raise BundleError("bundle signature invalid") from exc
    b = signed["body"]
    try:
        return VerifiedBundle(
            int(b["bundle_version"]),
            {r["region_id"]: Region.model_validate(r) for r in b["regions"]},
            {(l["from_region"], l["to_region"]): Link.model_validate(l) for l in b["links"]},
            {t: TopicPolicy.model_validate(p) for t, p in b["policies"].items()},
            {x["path_id"]: NetworkPath.model_validate(x) for x in b.get("paths", [])},
            dict(b.get("topology", {})),
        )
    except Exception as exc:  # noqa: BLE001
        raise BundleError(f"bundle content invalid: {exc}") from exc
