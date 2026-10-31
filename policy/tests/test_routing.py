"""Deterministic tests of the routing core (fake clock, no network, no randomness)."""

import pytest
from hypothesis import given, settings, strategies as st

from fixtures import ctx, eu_policy
from ss_policy.engine import eligible_paths, evaluate_path
from ss_policy.models import NetworkPath
from ss_replication.backlog import HARD, OK, SOFT, RecordSize, ThroughputWindow, classify, compute_backlog
from ss_replication.routing import CLOSED, HALF_OPEN, OPEN, CircuitBreaker, PathInfo, PathStats, RouteSelector, RoutingConfig, Sample, score_path


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class Net:
    """A tiny simulated network: feed one probe per path per second, then ask the selector."""

    def __init__(self, cfg=None, paths=("a", "b"), costs=None, fallback=None):
        self.cfg, self.clock = cfg or RoutingConfig(), Clock()
        self.infos = [PathInfo(p, (costs or {}).get(p, 1.0), p == fallback) for p in paths]
        self.stats = {p: PathStats() for p in paths}
        self.br = {p: CircuitBreaker(self.cfg) for p in paths}
        self.sel = RouteSelector(self.cfg, self.clock)
        self.switches = []

    def probe(self, path, rtt=None, ok=True, bw=1e6):
        now = self.clock()
        self.stats[path].add(Sample(now, ok, rtt if ok else None, timeout=not ok, bandwidth_bps=bw if ok else None))
        self.br[path].record(ok, now)

    def tick(self, seconds=1.0, rtts=None, down=(), bw=None, backlog=0.0):
        """Advance time one step per second, probing every path with the given RTTs."""
        out = None
        for _ in range(int(seconds)):
            self.clock.t += 1.0
            for p in self.stats:
                if p in down:
                    self.probe(p, ok=False)
                else:
                    self.probe(p, (rtts or {}).get(p, 20.0), bw=(bw or {}).get(p, 1e6))
            out = self.sel.decide(self.infos, self.stats, self.br, backlog)
            if out.changed:
                self.switches.append((self.clock.t, out.previous, out.selected, out.reason))
        return out


def settle(net, **kw):
    return net.tick(8, **kw)


# ---- selection basics ---------------------------------------------------------------------------------------------
def test_initial_selection_picks_the_best_scored_path():
    n = Net()
    d = n.tick(5, rtts={"a": 300, "b": 30})
    assert d.selected == "b" and n.switches[0][3] == "INITIAL_SELECTION"


def test_path_b_is_selected_when_a_is_degraded_and_b_is_healthier():
    n = Net()
    n.tick(6, rtts={"a": 20, "b": 60})
    assert n.sel.current == "a"
    d = n.tick(12, rtts={"a": 400, "b": 60})  # A degrades to 400 ms, B stays at 60 ms
    assert d.selected == "b" and [s[2] for s in n.switches] == ["a", "b"]
    assert n.switches[-1][3] in ("SUSTAINED_IMPROVEMENT", "CURRENT_PATH_UNUSABLE")


def test_scoring_is_deterministic_and_normalised():
    cfg = RoutingConfig()
    n = Net()
    n.tick(5, rtts={"a": 100, "b": 100})
    m = n.stats["a"].measure(n.clock(), cfg.sample_max_age_s)
    s1 = score_path(cfg, m, PathInfo("a", 2.0), 4.0, 0.0)
    s2 = score_path(cfg, m, PathInfo("a", 2.0), 4.0, 0.0)
    assert s1 == s2
    assert all(0.0 <= s1[k] <= 1.0 for k in ("latency", "loss", "backlog_delay", "cost"))
    assert s1["score"] == pytest.approx(0.30 * 0.1 + 0.30 * 0 + 0.30 * 0 + 0.10 * 0.5)


def test_backlog_delay_term_prefers_the_higher_bandwidth_path():
    n = Net()
    d = n.tick(6, rtts={"a": 50, "b": 50}, bw={"a": 1e4, "b": 1e6}, backlog=5e6)  # 500 s vs 5 s to drain
    assert d.selected == "b" and d.scores["b"]["score"] < d.scores["a"]["score"]


# ---- legal / security restrictions are inputs, never overridden ----------------------------------------------------
def test_a_prohibited_path_is_never_selected_even_with_the_best_network_score():
    # path "p" is the best by far but is not in the eligible set handed to the selector
    n = Net(paths=("p", "q"))
    n.infos = [PathInfo("q")]  # policy removed p
    n.tick(10, rtts={"p": 1, "q": 400})
    assert n.sel.current == "q" and all(s[2] != "p" for s in n.switches)
    # and if it WAS current and becomes prohibited, it is dropped at once
    n2 = Net(paths=("p", "q"))
    n2.tick(6, rtts={"p": 1, "q": 400})
    assert n2.sel.current == "p"
    n2.infos = [PathInfo("q")]
    d = n2.tick(1, rtts={"p": 1, "q": 400})
    assert d.selected == "q" and d.reason == "CURRENT_PATH_UNUSABLE"


def mk_path(pid, zones=("EU",), via=(), encrypted=True, enabled=True):
    return NetworkPath(
        path_id=pid,
        source_region="eu-central",
        target_region="eu-west",
        kind="RELAY" if via else "DIRECT",
        via_regions=list(via),
        transit_zones=list(zones),
        endpoint_url="http://x",
        encrypted=encrypted,
        enabled=enabled,
    )


def test_path_legality_transit_relay_security_and_enabled():
    c = ctx(eu_policy(classification="RESTRICTED"))
    assert evaluate_path(c, mk_path("ok")).allowed
    d = evaluate_path(c, mk_path("zone", zones=("EU", "ATLANTIC")))
    assert {r.code for r in d.reasons} == {"PATH_TRANSIT_ZONE_NOT_ALLOWED"}
    d = evaluate_path(c, mk_path("relay", via=("us-east",)))
    assert "RELAY_REGION_NOT_ALLOWED" in {r.code for r in d.reasons}
    assert evaluate_path(c, mk_path("eu-relay", via=("eu-south",))).allowed
    d = evaluate_path(c, mk_path("plain", encrypted=False))
    assert {r.code for r in d.reasons} == {"PATH_NOT_ENCRYPTED"} and d.reasons[0].category == "SECURITY"
    assert evaluate_path(ctx(eu_policy(classification="INTERNAL")), mk_path("plain", encrypted=False)).allowed
    assert "PATH_DISABLED" in {r.code for r in evaluate_path(c, mk_path("off", enabled=False)).reasons}
    ok, rejected = eligible_paths(c, [mk_path("b"), mk_path("a"), mk_path("z", zones=("US",))])
    assert [p.path_id for p in ok] == ["a", "b"] and set(rejected) == {"z"}


# ---- hysteresis, observation window, cooldown, flapping -----------------------------------------------------------
def test_small_latency_fluctuation_does_not_cause_repeated_switching():
    n = Net()
    n.tick(6, rtts={"a": 100, "b": 110})
    assert n.sel.current == "a"
    for i in range(120):  # two minutes of +-15 % wobble that crosses over now and then
        wob = 15 if i % 2 else -15
        n.tick(1, rtts={"a": 100 + wob, "b": 110 - wob})
    assert n.sel.changes == 1, n.switches  # only the initial selection


def test_a_brief_spike_is_not_a_sustained_improvement():
    n = Net()
    n.tick(6, rtts={"a": 100, "b": 100})
    assert n.sel.current == "a"
    n.tick(2, rtts={"a": 300, "b": 100})  # A is bad for 2 s only (shorter than the hold)
    n.tick(8, rtts={"a": 100, "b": 100})
    assert n.sel.current == "a" and n.sel.changes == 1


def test_a_sustained_improvement_eventually_triggers_a_switch_and_not_before_the_hold():
    n = Net()
    n.tick(6, rtts={"a": 300, "b": 300})
    assert n.sel.current in ("a", "b")
    cur = n.sel.current
    other = "b" if cur == "a" else "a"
    t0 = n.clock()
    n.tick(40, rtts={cur: 300, other: 60})
    assert n.sel.current == other
    when = next(t for t, prev, new, why in n.switches if new == other)
    assert when - t0 >= n.cfg.improvement_hold_s  # not instantly


def test_cooldown_blocks_back_to_back_voluntary_switches():
    cfg = RoutingConfig(cooldown_s=30.0, improvement_hold_s=2.0, min_observation_s=2.0)
    n = Net(cfg)
    n.tick(5, rtts={"a": 300, "b": 100})
    assert n.sel.current == "b"
    n.tick(10, rtts={"a": 50, "b": 400})  # A is now much better, but we switched < 30 s ago
    assert n.sel.current == "b"
    assert n.tick(1, rtts={"a": 50, "b": 400}).reason == "COOLDOWN"
    n.tick(30, rtts={"a": 50, "b": 400})
    assert n.sel.current == "a"


def test_an_unusable_current_path_is_replaced_immediately_despite_cooldown():
    cfg = RoutingConfig(cooldown_s=1000.0)
    n = Net(cfg)
    n.tick(6)
    first = n.sel.current
    other = "b" if first == "a" else "a"
    n.tick(4, down=(first,))
    assert n.sel.current == other and n.switches[-1][3] == "CURRENT_PATH_UNUSABLE"


# ---- circuit breaker and recovery probing -------------------------------------------------------------------------
def test_circuit_breaker_opens_half_opens_and_closes_after_recovery_probes():
    cfg = RoutingConfig(breaker_failures=3, breaker_open_s=5, breaker_close_successes=2)
    b = CircuitBreaker(cfg)
    t = 0.0
    assert [b.record(False, t + i) for i in range(3)] == [CLOSED, CLOSED, OPEN]
    assert b.state(t + 3) == OPEN
    b.record(False, t + 6)  # still failing while open: the quiet period restarts
    assert b.state(t + 7) == OPEN and b.state(t + 11) == HALF_OPEN
    assert b.record(True, t + 20) == HALF_OPEN  # first probe after the quiet period passes: still half-open
    assert b.record(False, t + 21) == OPEN  # a failed probe re-opens
    assert b.record(True, t + 40) == HALF_OPEN and b.record(True, t + 41) == CLOSED


def test_recovered_path_is_not_used_until_its_breaker_closes_and_it_is_better():
    n = Net(RoutingConfig(breaker_open_s=3, cooldown_s=2, improvement_hold_s=2, min_observation_s=2))
    n.tick(6, rtts={"a": 20, "b": 200})
    assert n.sel.current == "a"
    n.tick(5, down=("a",))  # A goes away
    assert n.sel.current == "b"
    d = n.tick(1, rtts={"a": 20, "b": 200})  # A answers again: breaker is open / half-open
    assert d.excluded.get("a") in ("CIRCUIT_OPEN", "CIRCUIT_HALF_OPEN") and n.sel.current == "b"
    n.tick(30, rtts={"a": 20, "b": 200})  # probing closes the breaker, A is clearly better again
    assert n.sel.current == "a"


def test_all_paths_down_selects_nothing_and_recovery_selects_again():
    n = Net(RoutingConfig(breaker_open_s=3))
    n.tick(6)
    d = n.tick(6, down=("a", "b"))
    assert d.selected is None and d.reason == "NO_USABLE_PATH" and set(d.excluded) == {"a", "b"}
    d = n.tick(12)
    assert d.selected in ("a", "b")


def test_fallback_path_is_tried_first_when_nothing_is_measured_healthy():
    n = Net(paths=("direct", "backup"), fallback="backup", costs={"direct": 1.0, "backup": 3.0})
    n.tick(6)
    assert n.sel.current == "direct"
    # measurements go stale (no probes for a long time) and the direct path has been failing at the transport level
    n.clock.t += 100
    n.stats["direct"].add(Sample(n.clock(), False, timeout=True))
    for _ in range(3):
        n.br["direct"].record(False, n.clock())
    d = n.sel.decide(n.infos, n.stats, n.br)
    assert d.selected == "backup" and d.reason == "FALLBACK_UNMEASURED"


# ---- bad measurements ----------------------------------------------------------------------------------------------
def test_missing_stale_and_invalid_measurements_are_handled_safely():
    cfg = RoutingConfig()
    st = PathStats()
    assert st.measure(100.0, cfg.sample_max_age_s).known is False
    for bad in (float("nan"), float("inf"), -5.0):
        assert st.add(Sample(100.0, True, bad)) is False
    assert st.invalid == 3 and st.measure(100.0, 15).samples == 0
    assert st.add(Sample(100.0, True, 50.0))
    assert st.measure(100.0, 15).rtt_ms == 50.0
    assert st.measure(130.0, 15).known is False  # stale: ignored, not trusted
    assert score_path(cfg, st.measure(130.0, 15), PathInfo("x"), 1.0, 0.0) is None
    n = Net()
    d = n.sel.decide(n.infos, n.stats, n.br)  # nothing measured yet
    assert d.selected in ("a", "b") and d.reason == "FALLBACK_UNMEASURED"  # picks, but never claims it is healthy
    assert not d.scores


def test_thresholds_exclude_unhealthy_paths_regardless_of_score():
    cfg = RoutingConfig(max_rtt_ms=500, max_loss=0.3)
    n = Net(cfg)
    d = n.tick(6, rtts={"a": 900, "b": 100})
    assert d.excluded["a"] == "RTT_ABOVE_THRESHOLD" and d.selected == "b"
    n2 = Net(cfg)
    for i in range(10):
        n2.clock.t += 1
        n2.probe("a", 10.0, ok=(i % 2 == 0))  # 50 % loss
        n2.probe("b", 200.0)
    d = n2.sel.decide(n2.infos, n2.stats, n2.br)
    assert d.excluded.get("a") in ("LOSS_ABOVE_THRESHOLD", "ENDPOINT_UNAVAILABLE") and d.selected == "b"


# ---- properties ----------------------------------------------------------------------------------------------------
@settings(max_examples=200, deadline=None)
@given(rtts=st.lists(st.tuples(st.floats(0, 3000), st.floats(0, 3000)), min_size=5, max_size=60), allowed=st.sampled_from([("a",), ("b",), ("a", "b")]))
def test_the_selector_only_ever_returns_an_allowed_path(rtts, allowed):
    n = Net()
    for ra, rb in rtts:
        n.clock.t += 1
        n.probe("a", ra)
        n.probe("b", rb)
        n.infos = [PathInfo(p) for p in allowed]
        d = n.sel.decide(n.infos, n.stats, n.br)
        assert d.selected is None or d.selected in allowed


@settings(max_examples=200, deadline=None)
@given(st.lists(st.floats(0, 2000), min_size=6, max_size=40), st.floats(0.0, 1e8))
def test_same_inputs_give_the_same_decision(rtts, backlog):
    def run():
        n = Net()
        out = []
        for r in rtts:
            n.clock.t += 1
            n.probe("a", r)
            n.probe("b", 100.0)
            d = n.sel.decide(n.infos, n.stats, n.br, backlog)
            out.append((d.selected, d.reason, tuple(sorted((k, v["score"]) for k, v in d.scores.items()))))
        return out

    assert run() == run()


@settings(max_examples=150, deadline=None)
@given(st.lists(st.floats(0, 1000), min_size=30, max_size=120))
def test_noise_inside_the_hysteresis_band_never_flaps(noise):
    n = Net()
    n.clock.t += 0
    for i, x in enumerate(noise):
        n.clock.t += 1
        base = 200.0
        n.probe("a", base + x * 0.05)  # +-50 ms on 200 ms (<= 25 % of the score difference band)
        n.probe("b", base + (50 - x * 0.05))
        n.sel.decide(n.infos, n.stats, n.br)
    assert n.sel.changes <= 2


# ---- backlog and bounded-backlog policy -------------------------------------------------------------------------------
def test_backlog_is_measured_from_offsets_and_classified_against_limits():
    b = compute_backlog(end_offset=1000, last_acked=399, bytes_per_record=500, throughput_bps=0.0, path_bandwidth_bps=1000.0, soft=100_000, hard=400_000)
    assert b.records == 600 and b.bytes_estimate == 300_000 and b.state == SOFT and b.drain_seconds == pytest.approx(300.0)
    assert compute_backlog(1000, 999, 500, 0, 1000, 100_000, 400_000).state == OK
    assert compute_backlog(1000, -1, 500, 5000.0, None, 100_000, 400_000).state == HARD
    assert compute_backlog(1000, -1, 500, 5000.0, None, None, None).state == OK
    assert classify(400_000, 100_000, 400_000) == HARD and classify(399_999, 100_000, 400_000) == SOFT
    assert compute_backlog(10, -1, 100, 0.0, None, None, None).drain_seconds is None  # cannot estimate a drain time
    assert compute_backlog(5, 4, 100, 0.0, None, None, None).drain_seconds == 0.0


def test_throughput_window_and_record_size_estimate():
    w = ThroughputWindow(window_s=10)
    w.add(0.0, 1000, 10)
    w.add(5.0, 3000, 30)
    bps, rps = w.rates(10.0)
    assert bps == pytest.approx(400.0) and rps == pytest.approx(4.0)
    assert w.rates(100.0) == (0.0, 0.0)
    r = RecordSize(default=1.0)
    r.update(1000, 10)
    assert r.value == 100.0
    r.update(2000, 10)
    assert r.value == pytest.approx(0.8 * 100 + 0.2 * 200)
