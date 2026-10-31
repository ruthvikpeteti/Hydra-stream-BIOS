"""Regional agent: independent validation, signed bundles, versioning and staleness (fake clock)."""

import pytest

from fixtures import eu_policy, world
from ss_policy.agent_core import AgentRequest, RegionalPolicyAgent
from ss_policy.bundle import BundleError, sign_bundle
from ss_policy.models import DecisionKind, Operation, ReplicaStatus
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def keypair():
    k = Ed25519PrivateKey.generate()
    priv = k.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    pub = k.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    return priv, pub


def body(version, policies, bundle_version=1):
    regs, links = world()
    return {
        "bundle_version": bundle_version,
        "regions": [r.model_dump(mode="json") for r in regs.values()],
        "links": [l.model_dump(mode="json") for l in links.values()],
        "policies": {p.topic: p.model_dump(mode="json") for p in policies},
    }


class Clock:
    t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def env():
    priv, pub = keypair()
    clock = Clock()
    agent = RegionalPolicyAgent("eu-west", pub, clock)
    return priv, pub, clock, agent


def req(**kw):
    return AgentRequest(topic=kw.pop("topic", "orders.eu"), operation=kw.pop("operation", Operation.ASSIGN), source_region=kw.pop("source", "eu-central"), **kw)


def test_no_bundle_fails_closed(env):
    *_, agent = env
    d = agent.authorize(req())
    assert d.decision == DecisionKind.POLICY_UNAVAILABLE and d.reasons[0].code == "BUNDLE_UNAVAILABLE"


def test_forged_and_downgraded_bundles_are_refused(env):
    priv, pub, clock, agent = env
    agent.install(sign_bundle(body(1, [eu_policy()], 5), priv))
    evil_priv, _ = keypair()
    with pytest.raises(BundleError):
        agent.install(sign_bundle(body(1, [eu_policy()], 6), evil_priv))  # wrong signer
    tampered = sign_bundle(body(1, [eu_policy()], 7), priv)
    tampered["body"]["policies"]["orders.eu"]["allowed_storage_regions"].append("us-east")
    with pytest.raises(BundleError):
        agent.install(tampered)  # edited after signing
    with pytest.raises(BundleError):
        agent.install(sign_bundle(body(1, [eu_policy()], 4), priv))  # older bundle_version
    assert agent.bundle_version == 5


def test_receiver_revalidates_independently_of_the_sender(env):
    priv, _, _, agent = env
    agent.install(sign_bundle(body(1, [eu_policy()]), priv))
    assert agent.authorize(req()).allowed
    # a sender that is wrongly convinced us-east may replicate to eu-west's agent is irrelevant; test the reverse:
    agent_us = RegionalPolicyAgent("us-east", env[1], env[2])
    agent_us.install(sign_bundle(body(1, [eu_policy()]), priv))
    d = agent_us.authorize(req())  # sender "decided" ALLOWED; receiver says no
    assert d.decision == DecisionKind.REJECTED and "REGION_NOT_ALLOWED_STORAGE" in {r.code for r in d.reasons}


def test_policy_version_change_stops_a_stale_worker(env):
    priv, _, clock, agent = env
    agent.install(sign_bundle(body(1, [eu_policy(version=1)]), priv))
    assert agent.authorize(req(flow_id="f1", policy_version=1)).allowed
    assert agent.admit_batch("orders.eu", "f1", 1, "eu-central", None).allowed
    # the organisation tightens the policy: v2 no longer allows eu-west
    v2 = eu_policy(version=2, allowed_storage_regions=["eu-central", "eu-north"], allowed_processing_regions=["eu-central", "eu-north"])
    agent.install(sign_bundle(body(2, [v2], 2), priv))
    d = agent.admit_batch("orders.eu", "f1", 1, "eu-central", None)  # worker still on v1
    assert d.decision == DecisionKind.REJECTED and d.reasons[0].code == "STALE_POLICY_VERSION" and d.policy_version == 2
    d = agent.admit_batch("orders.eu", "f1", 2, "eu-central", None)  # worker refreshed, but eu-west is now illegal
    assert d.decision == DecisionKind.REJECTED and "REGION_NOT_ALLOWED_STORAGE" in {r.code for r in d.reasons}


def test_flow_admitted_under_old_version_must_reinitialise(env):
    priv, _, _, agent = env
    agent.install(sign_bundle(body(1, [eu_policy(version=1)]), priv))
    assert agent.authorize(req(flow_id="f1", policy_version=1)).allowed
    agent.install(sign_bundle(body(2, [eu_policy(version=2, maximum_replication_lag_ms=1000)], 2), priv))  # still legal
    d = agent.admit_batch("orders.eu", "f1", 2, "eu-central", None)
    assert d.decision == DecisionKind.REJECTED and d.reasons[0].code == "STALE_FLOW"
    assert agent.authorize(req(flow_id="f1", policy_version=2)).allowed  # re-initialise
    assert agent.admit_batch("orders.eu", "f1", 2, "eu-central", None).allowed
    assert agent.admit_batch("orders.eu", "unknown-flow", 2, "eu-central", None).reasons[0].code == "UNKNOWN_FLOW"


def test_sender_ahead_of_agent_fails_closed(env):
    priv, _, _, agent = env
    agent.install(sign_bundle(body(1, [eu_policy(version=1)]), priv))
    d = agent.authorize(req(policy_version=2))
    assert d.decision == DecisionKind.POLICY_UNAVAILABLE and d.reasons[0].code == "AGENT_POLICY_BEHIND"
    assert agent.needs_refresh("orders.eu", 2)


@pytest.mark.parametrize("cls,limit", [("RESTRICTED", 30), ("CONFIDENTIAL", 120), ("INTERNAL", 600), ("PUBLIC", 3600)])
def test_stale_policy_state_fails_closed_per_classification(env, cls, limit):
    priv, _, clock, agent = env
    agent.install(sign_bundle(body(1, [eu_policy(classification=cls)]), priv))
    clock.t += limit - 1
    assert agent.authorize(req()).allowed  # engine unreachable, still within limit
    clock.t += 2
    d = agent.authorize(req())
    assert d.decision == DecisionKind.POLICY_UNAVAILABLE and d.reasons[0].code == "POLICY_STALE"
    agent.install(sign_bundle(body(1, [eu_policy(classification=cls)], 2), priv))  # refresh heals it
    assert agent.authorize(req()).allowed


def test_restricted_has_the_tightest_limit(env):
    priv, _, clock, agent = env
    agent.install(sign_bundle(body(1, [eu_policy(topic="r", classification="RESTRICTED"), eu_policy(topic="i", classification="INTERNAL")]), priv))
    clock.t += 60
    assert not agent.authorize(req(topic="r")).allowed and agent.authorize(req(topic="i")).allowed


def test_unknown_topic_and_promotion_and_process(env):
    priv, _, _, agent = env
    agent.install(sign_bundle(body(1, [eu_policy()]), priv))
    assert agent.authorize(req(topic="nope")).decision == DecisionKind.POLICY_UNAVAILABLE
    assert agent.authorize(req(operation=Operation.PROMOTE, replica_status=ReplicaStatus.IN_SYNC)).allowed
    assert not agent.authorize(req(operation=Operation.PROMOTE)).allowed  # no replica status
    assert agent.authorize(req(operation=Operation.PROCESS)).allowed
    us = RegionalPolicyAgent("us-east", env[1], env[2])
    us.install(sign_bundle(body(1, [eu_policy()]), priv))
    assert not us.authorize(req(operation=Operation.PROMOTE, replica_status=ReplicaStatus.IN_SYNC)).allowed
    assert not us.authorize(req(operation=Operation.PROCESS)).allowed
