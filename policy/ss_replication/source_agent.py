"""Source replication agent: reads its region's Kafka partition by partition and ships batches to the target regions'
receivers. At-least-once: a checkpoint is persisted only AFTER the receiver acknowledged the batch.

Per (flow, partition) there is one pipeline with its own consumer, its own checkpoint and its own retry state, so
partitions progress independently and targets (A->B, A->C) do not hold each other back. Order within a partition is
kept: batches are acknowledged and checkpointed strictly in order, and a failed batch is resent together with every
batch after it (go-back-N)."""

import asyncio
import base64
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import httpx
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from .backlog import RecordSize, ThroughputWindow, compute_backlog
from .batch import BatchIn
from .checkpoints import CheckpointStore
from .cutover import CutoverInvalid, Scanned, compute_partition_cutover
from .flow_control import backoff_delay
from .netmon import FlowRoute, PairMonitor
from .routing import RoutingConfig

log = logging.getLogger("ss.replicator")


@dataclass
class Config:
    region: str
    cluster_id: str
    bootstrap: str
    engine_url: str
    service_key: str
    agent_key: str
    receiver_urls: dict
    batch_max_records: int = 500
    batch_max_bytes: int = 1_048_576
    batch_linger_ms: int = 100
    max_inflight: int = 1  # batches in flight per partition (ordering is kept by go-back-N and the receiver gate)
    queue_max_batches: int = 8  # batches read but not yet acknowledged, per partition: the memory bound
    request_timeout_s: float = 10.0
    backoff_base_s: float = 0.25
    backoff_cap_s: float = 10.0
    discovery_interval_s: float = 2.0
    shutdown_timeout_s: float = 15.0
    # Phase 5: probing, routing, backlog
    probe_interval_s: float = 1.0
    probe_timeout_s: float = 2.0
    bw_probe_every: int = 5
    route_interval_s: float = 1.0
    backlog_report_interval_s: float = 2.0
    default_backlog_soft_bytes: Optional[int] = None
    default_backlog_hard_bytes: Optional[int] = None
    routing: RoutingConfig = field(default_factory=RoutingConfig)

    @classmethod
    def from_env(cls) -> "Config":
        import json

        e = os.environ
        return cls(
            region=e["SS_REGION"],
            cluster_id=e["SS_CLUSTER_ID"],
            bootstrap=e["SS_KAFKA_BOOTSTRAP"],
            engine_url=e["SS_ENGINE_URL"].rstrip("/"),
            service_key=e["SS_SERVICE_KEY"],
            agent_key=e["SS_REPL_AGENT_KEY"],
            receiver_urls=json.loads(e["SS_RECEIVER_URLS"]),
            batch_max_records=int(e.get("SS_BATCH_MAX_RECORDS", 500)),
            batch_max_bytes=int(e.get("SS_BATCH_MAX_BYTES", 1_048_576)),
            batch_linger_ms=int(e.get("SS_BATCH_LINGER_MS", 100)),
            max_inflight=int(e.get("SS_MAX_INFLIGHT", 1)),
            queue_max_batches=int(e.get("SS_QUEUE_MAX_BATCHES", 8)),
            request_timeout_s=float(e.get("SS_REQUEST_TIMEOUT_S", 10)),
            backoff_base_s=float(e.get("SS_BACKOFF_BASE_S", 0.25)),
            backoff_cap_s=float(e.get("SS_BACKOFF_CAP_S", 10)),
            discovery_interval_s=float(e.get("SS_DISCOVERY_INTERVAL_S", 2)),
            shutdown_timeout_s=float(e.get("SS_SHUTDOWN_TIMEOUT_S", 15)),
            probe_interval_s=float(e.get("SS_PROBE_INTERVAL_S", 1)),
            probe_timeout_s=float(e.get("SS_PROBE_TIMEOUT_S", 2)),
            bw_probe_every=int(e.get("SS_BW_PROBE_EVERY", 5)),
            route_interval_s=float(e.get("SS_ROUTE_INTERVAL_S", 1)),
            backlog_report_interval_s=float(e.get("SS_BACKLOG_REPORT_S", 2)),
            default_backlog_soft_bytes=int(e["SS_BACKLOG_SOFT_BYTES"]) if e.get("SS_BACKLOG_SOFT_BYTES") else None,
            default_backlog_hard_bytes=int(e["SS_BACKLOG_HARD_BYTES"]) if e.get("SS_BACKLOG_HARD_BYTES") else None,
            routing=RoutingConfig(
                min_observation_s=float(e.get("SS_ROUTE_MIN_OBSERVATION_S", 4)),
                improvement_hold_s=float(e.get("SS_ROUTE_HOLD_S", 4)),
                cooldown_s=float(e.get("SS_ROUTE_COOLDOWN_S", 8)),
                hysteresis=float(e.get("SS_ROUTE_HYSTERESIS", 0.25)),
                breaker_failures=int(e.get("SS_BREAKER_FAILURES", 3)),
                breaker_open_s=float(e.get("SS_BREAKER_OPEN_S", 5)),
                breaker_close_successes=int(e.get("SS_BREAKER_CLOSE_SUCCESSES", 2)),
                max_rtt_ms=float(e.get("SS_ROUTE_MAX_RTT_MS", 5000)),
                max_loss=float(e.get("SS_ROUTE_MAX_LOSS", 0.5)),
                sample_max_age_s=float(e.get("SS_ROUTE_SAMPLE_MAX_AGE_S", 15)),
                min_samples=int(e.get("SS_ROUTE_MIN_SAMPLES", 3)),
            ),
        )


class SendError(Exception):
    def __init__(self, code: str, message: str, retryable: bool, immediate: bool = False):
        super().__init__(message)
        self.code, self.retryable, self.immediate = code, retryable, immediate


@dataclass
class Pending:
    batch: dict
    attempt: int = 1


@dataclass
class PartitionStatus:
    state: str = "STARTING"
    last_acked: int = -1
    last_destination_offset: int = -1
    pending: int = 0
    inflight: int = 0
    retries: int = 0
    acked_batches: int = 0
    last_error: Optional[str] = None
    last_error_code: Optional[str] = None
    caught_up: bool = False
    backlog_records: int = 0
    backlog_bytes: int = 0
    throughput_bps: float = 0.0
    route: Optional[str] = None


def origin_of(rec, source_cluster: str) -> tuple[dict, str]:
    """(origin coordinates, hop path so far) of a record read from this region's cluster."""
    h = {k: v.decode() for k, v in (rec.headers or [])}
    if "ss-origin-cluster" in h:
        origin = {
            "cluster": h["ss-origin-cluster"],
            "topic": h["ss-origin-topic"],
            "partition": int(h["ss-origin-partition"]),
            "offset": int(h["ss-origin-offset"]),
        }
        return origin, h.get("ss-hop-path", h["ss-origin-cluster"])
    return {"cluster": source_cluster, "topic": rec.topic, "partition": rec.partition, "offset": rec.offset}, source_cluster


class PartitionPipeline:
    def __init__(self, cfg: Config, flow: dict, partition: int, store: CheckpointStore, http: httpx.AsyncClient, engine: httpx.AsyncClient, route: FlowRoute):
        self.cfg, self.flow, self.p, self.store, self.http, self.engine, self.route = cfg, flow, partition, store, http, engine, route
        self.record_size, self.throughput, self._last_backlog = RecordSize(), ThroughputWindow(), 0.0
        self.status = PartitionStatus()
        self.stop = asyncio.Event()
        self.paused = False
        self.session = uuid.uuid4().hex[:12]
        self._reported: Optional[str] = None

    # ---- helpers -----------------------------------------------------------------------------------------
    def _key(self):
        f = self.flow
        return f["source_cluster"], f["target_cluster"], f["topic"], self.p

    async def _read_batch(self, consumer, tp, prev_offset: int) -> Optional[dict]:
        got = await consumer.getmany(tp, timeout_ms=self.cfg.batch_linger_ms, max_records=self.cfg.batch_max_records)
        recs = got.get(tp, [])
        if not recs:
            return None
        out, size = [], 0
        for r in recs:
            size += len(r.value or b"") + len(r.key or b"")
            origin, hops = origin_of(r, self.flow["source_cluster"])
            out.append(
                {
                    "offset": r.offset,
                    "timestamp": max(0, r.timestamp),
                    "key": base64.b64encode(r.key).decode() if r.key is not None else None,
                    "value": base64.b64encode(r.value).decode(),
                    "origin": origin,
                    "hop_path": hops,
                }
            )
            if size >= self.cfg.batch_max_bytes:
                consumer.seek(tp, r.offset + 1)  # leave the rest for the next batch
                break
        self.record_size.update(size, len(out))
        return {"records": out, "prev_offset": prev_offset, "first_offset": out[0]["offset"], "last_offset": out[-1]["offset"]}

    def _body(self, pend: Pending) -> dict:
        f, b = self.flow, pend.batch
        return {
            "batch_id": f"{f['topic']}:{f['source_region']}->{f['target_region']}:g{f['generation']}:p{self.p}:{b['first_offset']}-{b['last_offset']}",
            "attempt": pend.attempt,
            "stream_session": self.session,
            "source_region": f["source_region"],
            "source_cluster": f["source_cluster"],
            "destination_region": f["target_region"],
            "topic": f["topic"],
            "partition": self.p,
            "policy_version": f["policy_version"],
            "replication_generation": f["generation"],
            "leadership_epoch": f["leadership_epoch"],
            "prev_offset": b["prev_offset"],
            "first_offset": b["first_offset"],
            "last_offset": b["last_offset"],
            "records": b["records"],
        }

    async def _send(self, pend: Pending) -> dict:
        path = self.route.current_path()
        if path is None:
            raise SendError("NO_ROUTE", "no eligible network path is currently usable", True)
        body = self._body(pend)
        BatchIn.model_validate(body)  # never send a malformed batch
        nbytes = sum(len(r["value"]) + len(r["key"] or "") for r in body["records"])
        t0 = time.monotonic()
        try:
            r = await self.http.post(
                f"{path['endpoint_url']}/internal/v1/replication/batches",
                json=body,
                timeout=self.cfg.request_timeout_s,
                headers={"Authorization": f"Bearer {self.cfg.agent_key}", "X-Replication-Agent": self.cfg.region, "X-Replication-Path": path["path_id"]},
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            self.route.monitor.record_send(path["path_id"], False, timeout=isinstance(exc, httpx.TimeoutException))
            raise SendError("TRANSPORT", f"{type(exc).__name__}: {exc}", True) from exc
        if r.status_code == 200:
            self.route.monitor.record_send(path["path_id"], True, time.monotonic() - t0, nbytes)
            self.status.route = path["path_id"]
            return r.json()
        try:
            e = r.json()["error"]
        except Exception:  # noqa: BLE001
            e = {"code": f"HTTP_{r.status_code}", "message": r.text[:200], "retryable": r.status_code >= 500}
        if r.status_code >= 502:  # gateway-level failures count against the path
            self.route.monitor.record_send(path["path_id"], False)
        raise SendError(e["code"], e["message"], bool(e.get("retryable")) or r.status_code >= 500, immediate=e["code"] == "ORDER_GAP")

    async def _report_status(self, status: str) -> None:
        if self._reported == status:
            return
        try:
            r = await self.engine.put(
                f"/api/v1/placement/{self.flow['topic']}/assignments/{self.p}/{self.flow['target_region']}/status", json={"replica_status": status}, timeout=5
            )
            if r.status_code == 200:
                self._reported = status
        except httpx.HTTPError:
            pass

    async def _checkpoint(self, pend: Pending, resp: dict) -> None:
        """Persist progress. Reached only after the receiver acknowledged this batch. Retried until it sticks."""
        f, attempt = self.flow, 0
        while True:
            try:
                await self.store.advance(
                    source_cluster=f["source_cluster"],
                    target_cluster=f["target_cluster"],
                    source_region=f["source_region"],
                    target_region=f["target_region"],
                    topic=f["topic"],
                    partition=self.p,
                    acked_source_offset=pend.batch["last_offset"],
                    destination_offset=resp["last_destination_offset"],
                    generation=f["generation"],
                    policy_version=f["policy_version"],
                )
                return
            except Exception as exc:  # noqa: BLE001 - e.g. PostgreSQL is down: stall, never skip the checkpoint
                self.status.state, self.status.last_error, self.status.last_error_code = "CHECKPOINT_STORE_UNAVAILABLE", repr(exc)[:200], "CHECKPOINT"
                await asyncio.sleep(backoff_delay(attempt, self.cfg.backoff_base_s, self.cfg.backoff_cap_s))
                attempt += 1

    # ---- main loop -------------------------------------------------------------------------------------------
    async def run(self) -> None:
        cfg, f = self.cfg, self.flow
        ck = await self.store.get(*self._key())
        if ck and ck.replication_generation > f["generation"]:
            self.status.state, self.status.last_error_code = "STALE_FLOW", "STALE_GENERATION"
            return
        self.status.last_acked = ck.last_acknowledged_source_offset if ck else -1
        self.status.last_destination_offset = ck.last_known_destination_offset if ck else -1
        consumer = AIOKafkaConsumer(bootstrap_servers=cfg.bootstrap, group_id=None, enable_auto_commit=False, auto_offset_reset="earliest")
        tp = TopicPartition(f["topic"], self.p)
        pending: deque[Pending] = deque()
        inflight: deque[asyncio.Task] = deque()
        read_task: Optional[asyncio.Task] = None
        last_read = self.status.last_acked
        fails = 0
        try:
            await consumer.start()
            consumer.assign([tp])
            begin = (await consumer.beginning_offsets([tp]))[tp]
            consumer.seek(tp, max(self.status.last_acked + 1, begin))
            self.status.state = "RUNNING"
            while True:
                if self.stop.is_set() and not inflight:
                    break
                await self._refresh_backlog(consumer, tp)
                has_route = self.route.current_path() is not None
                run_ok = not self.stop.is_set() and not self.paused and has_route
                while run_ok and len(inflight) < cfg.max_inflight and len(pending) > len(inflight):
                    inflight.append(asyncio.create_task(self._send(pending[len(inflight)])))
                if run_ok and read_task is None and len(pending) < cfg.queue_max_batches:
                    read_task = asyncio.create_task(self._read_batch(consumer, tp, last_read))
                waiters = set(inflight) | ({read_task} if read_task else set())
                if not waiters:
                    self.status.state = "PAUSED" if self.paused else ("STOPPING" if self.stop.is_set() else ("PAUSED_NO_ROUTE" if not has_route else "RUNNING"))
                    await asyncio.sleep(0.1)
                    continue
                done, _ = await asyncio.wait(waiters, timeout=0.5, return_when=asyncio.FIRST_COMPLETED)
                if read_task in done:
                    try:
                        b = read_task.result()
                    except (KafkaError, OSError) as exc:
                        b = None
                        self.status.last_error, self.status.last_error_code = repr(exc)[:200], "SOURCE_READ"
                        await asyncio.sleep(1)
                    read_task = None
                    if b:
                        pending.append(Pending(b))
                        last_read = b["last_offset"]
                    elif not pending and not inflight:
                        await self._maybe_caught_up(consumer, tp)
                while inflight and inflight[0].done():
                    head = inflight[0]
                    task_exc = head.exception()
                    if task_exc is None:
                        resp = head.result()
                        await self._checkpoint(pending[0], resp)  # <- only after the acknowledgement
                        s = self.status
                        s.last_acked, s.last_destination_offset = pending[0].batch["last_offset"], resp["last_destination_offset"]
                        s.acked_batches += 1
                        self.throughput.add(
                            time.monotonic(), sum(len(r["value"]) + len(r["key"] or "") for r in pending[0].batch["records"]), len(pending[0].batch["records"])
                        )
                        s.state, s.last_error, s.last_error_code, s.caught_up = "RUNNING", None, None, False
                        pending.popleft()
                        inflight.popleft()
                        fails = 0
                        asyncio.create_task(self._report_status("SYNCING"))
                    else:  # go-back-N: cancel the later batches, resend from the head
                        for t in list(inflight)[1:]:
                            t.cancel()
                        inflight.clear()
                        se = task_exc if isinstance(task_exc, SendError) else SendError("INTERNAL", repr(task_exc), True)
                        pending[0].attempt += 1
                        self.status.retries += 1
                        self.status.last_error, self.status.last_error_code = str(se)[:200], se.code
                        self.status.state = "RETRYING" if se.retryable else "BLOCKED"
                        if not se.immediate:
                            fails += 1
                            await self._sleep_unless_stopped(backoff_delay(fails, cfg.backoff_base_s, cfg.backoff_cap_s))
                        break
                self.status.pending, self.status.inflight = len(pending), len(inflight)
        finally:
            for t in list(inflight) + ([read_task] if read_task else []):
                t.cancel()
            await asyncio.gather(*(list(inflight) + ([read_task] if read_task else [])), return_exceptions=True)
            try:
                await consumer.stop()
            except Exception:  # noqa: BLE001
                pass
            if self.status.state not in ("STALE_FLOW",):
                self.status.state = "STOPPED"

    async def _refresh_backlog(self, consumer, tp) -> None:
        now = time.monotonic()
        if now - self._last_backlog < 1.0:
            return
        self._last_backlog = now
        try:
            end = (await consumer.end_offsets([tp]))[tp]
        except Exception:  # noqa: BLE001
            return
        bps, _ = self.throughput.rates(now)
        b = compute_backlog(end, self.status.last_acked, self.record_size.value, bps, None, None, None)
        self.status.backlog_records, self.status.backlog_bytes, self.status.throughput_bps = b.records, b.bytes_estimate, bps

    async def _maybe_caught_up(self, consumer, tp) -> None:
        try:
            end = (await consumer.end_offsets([tp]))[tp]
            if await consumer.position(tp) >= end and not self.status.caught_up:  # an idle (empty) partition is trivially in sync
                self.status.caught_up = True
                asyncio.create_task(self._report_status("IN_SYNC"))
        except Exception:  # noqa: BLE001
            pass

    async def _sleep_unless_stopped(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass


class FlowRunner:
    def __init__(self, cfg: Config, flow: dict, store, http, engine, route: FlowRoute):
        self.flow, self.route = flow, route
        self.pipes = [PartitionPipeline(cfg, flow, p, store, http, engine, route) for p in range(flow["partitions"])]
        self.tasks: list[asyncio.Task] = []

    def start(self) -> None:
        self.tasks = [asyncio.create_task(self._supervise(p), name=f"{self.flow['flow_id']}-p{p.p}") for p in self.pipes]

    @staticmethod
    async def _supervise(p: "PartitionPipeline") -> None:
        """A pipeline that dies (e.g. its source Kafka was still restarting) is restarted from its durable checkpoint, with
        backoff, until the flow is stopped. Without this a source-cluster restart would leave replication silently STOPPED."""
        attempt = 0
        while not p.stop.is_set():
            try:
                await p.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                p.status.state, p.status.last_error, p.status.last_error_code = "RESTARTING", repr(exc)[:200], "PIPELINE_CRASH"
                log.warning("pipeline %s p%s crashed (%r); restarting from its checkpoint", p.flow["flow_id"], p.p, exc)
            else:
                if p.status.state == "STALE_FLOW" or p.stop.is_set():
                    return
            attempt += 1
            await asyncio.sleep(min(10.0, 0.5 * 2 ** min(attempt, 5)))

    def set_paused(self, paused: bool) -> None:
        for p in self.pipes:
            p.paused = paused

    async def stop(self, timeout: float) -> None:
        for p in self.pipes:
            p.stop.set()
        done, pending = await asyncio.wait(self.tasks, timeout=timeout) if self.tasks else (set(), set())
        for t in pending:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


class Manager:
    def __init__(self, cfg: Config, store: CheckpointStore):
        self.cfg, self.store = cfg, store
        self.http = httpx.AsyncClient()
        self.engine = httpx.AsyncClient(base_url=cfg.engine_url, headers={"X-Service-Key": cfg.service_key}, timeout=5)
        self.runners: dict[int, FlowRunner] = {}
        self.monitors: dict[str, PairMonitor] = {}
        self._last_report: dict[int, float] = {}
        self.cutting_over: set[int] = set()
        self.blocked: list = []
        self.last_discovery_ok: Optional[float] = None
        self.stopping = asyncio.Event()

    async def discover(self) -> Optional[list[dict]]:
        try:
            r = await self.engine.get("/api/v1/replication/flows", params={"source_region": self.cfg.region})
            r.raise_for_status()
            j = r.json()
            self.blocked, self.last_discovery_ok = j["blocked"], time.time()
            return j["flows"]
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("flow discovery failed (keeping current flows): %r", exc)
            return None

    async def run(self) -> None:
        await asyncio.gather(self._discovery_loop(), self._probe_loop(), self._route_loop())

    async def _discovery_loop(self) -> None:
        while not self.stopping.is_set():
            flows = await self.discover()
            if flows is not None:
                await self.reconcile(flows)
            await self._wait(self.cfg.discovery_interval_s)

    async def _wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stopping.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    async def _probe_loop(self) -> None:
        while not self.stopping.is_set():
            await asyncio.gather(*(m.probe_once() for m in list(self.monitors.values())), return_exceptions=True)
            await self._wait(self.cfg.probe_interval_s)

    async def _route_loop(self) -> None:
        """Every interval: choose the route for each flow, audit any change, and report the flow's backlog."""
        while not self.stopping.is_set():
            for fid, run in list(self.runners.items()):
                try:
                    backlog_bytes = sum(p.status.backlog_bytes for p in run.pipes)
                    d = run.route.decide(backlog_bytes)
                    for p in run.pipes:
                        p.status.route = d.selected
                    if d.changed:
                        log.info("flow %s route %s -> %s (%s)", fid, d.previous, d.selected, d.reason)
                        asyncio.create_task(self._audit_route(run.flow, d))
                    if time.monotonic() - self._last_report.get(fid, 0.0) >= self.cfg.backlog_report_interval_s:
                        self._last_report[fid] = time.monotonic()
                        asyncio.create_task(self._report_backlog(fid, run, d.selected))
                except Exception:  # noqa: BLE001
                    log.exception("route loop error for flow %s", fid)
            await self._wait(self.cfg.route_interval_s)

    async def _audit_route(self, flow: dict, d) -> None:
        item = {
            "operation": "ROUTE_SWITCH",
            "enforcement_point": "ROUTING",
            "topic": flow["topic"],
            "policy_version": flow["policy_version"],
            "decision": "ALLOWED" if d.selected else "REJECTED",
            "reasons": [{"code": d.reason, "category": "ROUTING", "message": f"{d.previous or 'none'} -> {d.selected or 'none'}"}],
            "subject": {
                "source_region": flow["source_region"],
                "destination_region": flow["target_region"],
                "flow_id": flow["flow_id"],
                "route": [d.previous or "none", d.selected or "none"],
            },
            "requester": f"replicator:{self.cfg.region}",
            "request_id": None,
        }
        for _ in range(3):
            try:
                r = await self.engine.post("/api/v1/audit/decisions", json=[item], headers={"X-Audit-Source": self.cfg.region})
                if r.status_code == 202:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
        log.warning("route change audit record could not be delivered: %s", item["reasons"])

    async def _report_backlog(self, fid: int, run: FlowRunner, route_id: Optional[str]) -> None:
        f = run.flow
        recs = sum(p.status.backlog_records for p in run.pipes)
        nbytes = sum(p.status.backlog_bytes for p in run.pipes)
        bps = sum(p.status.throughput_bps for p in run.pipes)
        soft = f.get("backlog_soft_bytes") if f.get("backlog_soft_bytes") is not None else self.cfg.default_backlog_soft_bytes
        hard = f.get("backlog_hard_bytes") if f.get("backlog_hard_bytes") is not None else self.cfg.default_backlog_hard_bytes
        bw = (run.route.monitor.snapshot().get(route_id) or {}).get("bandwidth_bps") if route_id else None
        rate = bps if bps > 0 else (bw or 0.0)
        drain = (nbytes / rate) if rate > 0 and nbytes > 0 else (0.0 if nbytes == 0 else None)
        from .backlog import classify

        body = {
            "backlog_records": recs,
            "backlog_bytes": nbytes,
            "drain_seconds": drain,
            "throughput_bps": bps,
            "state": classify(nbytes, soft, hard),
            "route": route_id,
        }
        try:
            await self.engine.put(f"/api/v1/replication/flows/{fid}/backlog", json=body, timeout=5)
        except httpx.HTTPError:
            pass

    async def reconcile(self, flows: list[dict]) -> None:
        want = {f["flow_id"]: f for f in flows}
        for fid in [i for i in self.runners if i not in want or want[i]["state"] != "ENABLED"]:  # obsolete / disabled
            log.info("stopping flow %s (no longer enabled)", fid)
            await self.runners.pop(fid).stop(self.cfg.shutdown_timeout_s)
        for fid, f in want.items():
            if f["state"] == "PENDING_CUTOVER" and fid not in self.cutting_over:
                self.cutting_over.add(fid)
                asyncio.create_task(self._cutover(f))
            elif f["state"] == "ENABLED":
                run = self.runners.get(fid)
                mon = self.monitors.get(f["target_region"])
                if mon is None:
                    mon = self.monitors[f["target_region"]] = PairMonitor(
                        self.cfg.region, f["target_region"], self.cfg.agent_key, self.http, self.cfg.routing, self.cfg.probe_timeout_s, self.cfg.bw_probe_every
                    )
                mon.set_paths(f.get("paths", []))
                if run is None:
                    route = FlowRoute(mon, self.cfg.routing)
                    route.update_paths(f.get("paths", []))
                    run = self.runners[fid] = FlowRunner(self.cfg, f, self.store, self.http, self.engine, route)
                    run.start()
                    log.info("started flow %s %s -> %s", fid, f["source_region"], f["target_region"])
                run.flow.update(f)  # live policy_version / epoch for new batches
                run.route.update_paths(f.get("paths", []))  # legal eligibility is refreshed from the engine every cycle
                run.set_paused(f["paused"])

    # ---- cutover ------------------------------------------------------------------------------------------
    async def _receiver_bundle_version(self, region: str) -> Optional[int]:
        url = self.cfg.receiver_urls.get(region)
        try:
            r = await self.http.get(f"{url}/agent/v1/status", headers={"X-Service-Key": self.cfg.service_key}, timeout=5)
            return r.json().get("bundle_version")
        except (httpx.HTTPError, ValueError):
            return None

    async def _scan(self, topic: str, partition: int, old_cluster: str) -> tuple[list[Scanned], int]:
        c = AIOKafkaConsumer(bootstrap_servers=self.cfg.bootstrap, group_id=None, enable_auto_commit=False, auto_offset_reset="earliest")
        tp = TopicPartition(topic, partition)
        await c.start()
        try:
            c.assign([tp])
            await c.seek_to_beginning(tp)
            end = (await c.end_offsets([tp]))[tp]
            out: list[Scanned] = []
            while await c.position(tp) < end:
                for r in (await c.getmany(tp, timeout_ms=1000)).get(tp, []):
                    h = {k: v.decode() for k, v in (r.headers or [])}
                    out.append(Scanned(r.offset, h.get("ss-origin-cluster"), int(h["ss-origin-offset"]) if "ss-origin-offset" in h else None))
            return out, end
        finally:
            await c.stop()

    async def _cutover(self, f: dict) -> None:
        """New source validates where the target stands, seeds the new flow's checkpoints, then asks the engine to enable it."""
        fid = f["flow_id"]
        try:
            fence = f["fence_bundle_version"] or 0
            for region in (f["target_region"], self.cfg.region):  # old flows must be unable to deliver before we measure
                for _ in range(60):
                    v = await self._receiver_bundle_version(region)
                    if v is not None and v >= fence:
                        break
                    await asyncio.sleep(1)
                else:
                    raise RuntimeError(f"receiver in {region} has not reached bundle version {fence}")
            old = f["previous_active_cluster"]
            reports, rows = [], []
            for p in range(f["partitions"]):
                ck_t = await self.store.get(old, f["target_cluster"], f["topic"], p)
                ck_s = await self.store.get(old, f["source_cluster"], f["topic"], p)
                scanned, end = await self._scan(f["topic"], p, old)
                rep = compute_partition_cutover(
                    p,
                    scanned,
                    old,
                    ck_t.last_acknowledged_source_offset if ck_t else -1,
                    ck_s.last_acknowledged_source_offset if ck_s else -1,
                    end,
                    ck_s.last_known_destination_offset if ck_s else -1,
                )
                reports.append(rep)
                rows.append(
                    {
                        "source_cluster": f["source_cluster"],
                        "target_cluster": f["target_cluster"],
                        "topic": f["topic"],
                        "partition": p,
                        "source_region": f["source_region"],
                        "target_region": f["target_region"],
                        "acked_source_offset": rep["cutover_start_offset"] - 1,
                        "destination_offset": -1,
                        "generation": f["generation"],
                        "policy_version": f["policy_version"],
                    }
                )
            await self.store.seed(rows)  # atomic: all partitions or none
            r = await self.engine.post(f"/api/v1/replication/flows/{fid}/activate", json={"report": {"partitions": reports, "old_source_cluster": old}})
            r.raise_for_status()
            log.info("cutover for flow %s validated and activated", fid)
        except CutoverInvalid as exc:
            log.error("cutover for flow %s REJECTED: %s", fid, exc)
            self.blocked.append({"flow_id": fid, "cutover_error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            log.warning("cutover for flow %s failed, will retry: %r", fid, exc)
            self.cutting_over.discard(fid)

    # ---- lifecycle -------------------------------------------------------------------------------------------
    async def shutdown(self) -> None:
        self.stopping.set()
        await asyncio.gather(*(r.stop(self.cfg.shutdown_timeout_s) for r in self.runners.values()), return_exceptions=True)
        await self.http.aclose()
        await self.engine.aclose()

    def snapshot(self) -> dict:
        return {
            "region": self.cfg.region,
            "last_discovery_ok": self.last_discovery_ok,
            "blocked": self.blocked,
            "cutting_over": sorted(self.cutting_over),
            "flows": [
                {
                    "flow_id": fid,
                    "topic": r.flow["topic"],
                    "source": r.flow["source_region"],
                    "target": r.flow["target_region"],
                    "generation": r.flow["generation"],
                    "paused": r.flow["paused"],
                    "route": r.route.snapshot(),
                    "backlog": {
                        "records": sum(p.status.backlog_records for p in r.pipes),
                        "bytes_estimate": sum(p.status.backlog_bytes for p in r.pipes),
                        "max_pending_batches": max((p.status.pending for p in r.pipes), default=0),
                    },
                    "partitions": {p.p: vars(p.status) for p in r.pipes},
                }
                for fid, r in self.runners.items()
            ],
        }
