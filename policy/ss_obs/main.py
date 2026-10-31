"""Dashboard API (Phase 7): a read-only view of authoritative state, plus a small set of protected administrative operations that are
FORWARDED to the DR controller. It holds the controller's admin key; the browser never sees it. Roles: `viewer` reads, `operator`
reads and may request failover/failback. Every administrative request is audited (policy engine audit, operation DASH_*)."""

import asyncio
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from . import collect
from .metrics import Registry
from .sanitize import scrub

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("ss.obs")


def load_config() -> dict:
    e = os.environ.get
    j = lambda k, d: json.loads(e(k) or json.dumps(d))
    return {
        "engine_url": e("SS_ENGINE_URL", "http://ss-policy-engine:8090").rstrip("/"),
        "service_key": e("SS_SERVICE_KEY", ""),
        "coord_endpoints": [x for x in e("SS_COORD_ENDPOINTS", "").split(",") if x],
        "dr_urls": j("SS_DR_URLS", []),
        "dr_admin_key": e("SS_DR_ADMIN_KEY", ""),
        "replicator_urls": j("SS_REPLICATOR_URLS", {}),
        "gateway_urls": j("SS_GATEWAY_URLS", {}),
        "tokens": j("SS_DASH_TOKENS", {}),
        "interval_s": float(e("SS_DASH_INTERVAL_S", "2")),
        "static_dir": e("SS_DASH_STATIC", "/srv/dashboard"),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load_config()
    app.state.cfg = cfg
    app.state.sources = collect.Sources(cfg)
    app.state.snapshot = None
    app.state.raw = {}
    app.state.secrets = [cfg["service_key"], cfg["dr_admin_key"], *[t for t in cfg["tokens"]]]

    async def loop():
        while True:
            t0 = time.monotonic()
            try:
                raw = await app.state.sources.refresh()
                app.state.raw = raw
                app.state.snapshot = collect.build_snapshot(raw, cfg["interval_s"])
            except Exception:  # noqa: BLE001
                log.exception("refresh failed")
            await asyncio.sleep(max(0.2, cfg["interval_s"] - (time.monotonic() - t0)))

    task = asyncio.create_task(loop())
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await app.state.sources.close()


app = FastAPI(title="SovereignStream dashboard API", version="0.7.0", lifespan=lifespan)


# ---- auth ---------------------------------------------------------------------------------------------------------------------
def identity(request: Request, authorization: Optional[str] = Header(None)) -> dict:
    tokens = request.app.state.cfg["tokens"]
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "bearer token required")
    given = authorization[7:]
    for tok, who in tokens.items():
        if hmac.compare_digest(given, tok):
            return {"user": who["user"], "role": who["role"]}
    raise HTTPException(401, "unknown token")


def operator(who: dict = Depends(identity)) -> dict:
    if who["role"] != "operator":
        raise HTTPException(403, "this operation needs the operator role")
    return who


def clean(request: Request, obj):
    return scrub(obj, request.app.state.secrets)


# ---- read API -----------------------------------------------------------------------------------------------------------------
@app.get("/api/v1/health")
async def health():
    return {"status": "ok"}


@app.get("/api/v1/me")
async def me(who: dict = Depends(identity)):
    return {"user": who["user"], "role": who["role"], "can_administer": who["role"] == "operator"}


@app.get("/api/v1/snapshot")
async def snapshot(request: Request, who: dict = Depends(identity)):
    snap = request.app.state.snapshot
    if snap is None:
        return JSONResponse({"error": "NOT_READY", "detail": "the first collection has not finished"}, status_code=503)
    return clean(request, snap)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics(request: Request):
    snap = request.app.state.snapshot
    reg = Registry()
    if snap is not None:
        collect.build_metrics(request.app.state.raw, snap, reg)
    return PlainTextResponse(reg.render(), media_type="text/plain; version=0.0.4")


# ---- protected administrative operations -----------------------------------------------------------------------------------------
class FailoverIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    accept_degraded_rpo: bool = False
    reason: str = Field("requested from the dashboard", max_length=200)


class RegionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    region: str = Field(pattern=r"^region-[a-z]$")


async def forward(request: Request, who: dict, op: str, path: str, body: dict) -> JSONResponse:
    """Send the request to the DR controller and return ITS answer verbatim. The dashboard never reports success of its own."""
    cfg = request.app.state.cfg
    http: httpx.AsyncClient = request.app.state.sources.http
    answer, status = {"result": "UNREACHABLE", "detail": "no DR controller answered"}, 502
    for url in cfg["dr_urls"]:
        try:
            r = await http.post(f"{url}/dr/v1{path}", json=body, headers={"X-Admin-Key": cfg["dr_admin_key"]}, timeout=20.0)
            answer, status = r.json(), r.status_code
            break
        except (httpx.HTTPError, ValueError):
            continue
    try:  # audit who asked and what the controller answered (no secrets, no payloads)
        await http.post(
            f"{cfg['engine_url']}/api/v1/audit/decisions",
            headers={"X-Service-Key": cfg["service_key"], "X-Audit-Source": "dashboard"},
            json=[
                {
                    "operation": op,
                    "enforcement_point": "API",
                    "decision": "ALLOWED"
                    if str(answer.get("result")) in ("STARTED", "APPROVED", "PROMOTED", "ABORTED", "REGION_A_UNAVAILABLE", "ALREADY_IN_PROGRESS")
                    else "REJECTED",
                    "reasons": [{"code": str(answer.get("reason") or answer.get("result")), "message": "answer of the DR controller", "category": "SYSTEM"}],
                    "subject": {},
                    "requester": who["user"],
                }
            ],
        )
    except httpx.HTTPError:
        pass
    return JSONResponse({"requested_by": who["user"], "controller_answer": clean(request, answer)}, status_code=status if status >= 400 else 200)


@app.post("/api/v1/admin/failover")
async def admin_failover(body: FailoverIn, request: Request, who: dict = Depends(operator)):
    return await forward(
        request,
        who,
        "DASH_FAILOVER",
        "/failover",
        {"reason": body.reason, "accept_degraded_rpo": body.accept_degraded_rpo, "approved_by": who["user"] if body.accept_degraded_rpo else None},
    )


@app.post("/api/v1/admin/failback/start")
async def admin_fb_start(body: RegionIn, request: Request, who: dict = Depends(operator)):
    return await forward(request, who, "DASH_FAILBACK_START", "/failback/start", {"region": body.region, "requested_by": who["user"]})


@app.post("/api/v1/admin/failback/approve")
async def admin_fb_approve(request: Request, who: dict = Depends(operator)):
    return await forward(request, who, "DASH_FAILBACK_OK", "/failback/approve", {"approved_by": who["user"]})


@app.post("/api/v1/admin/failback/abort")
async def admin_fb_abort(request: Request, who: dict = Depends(operator)):
    return await forward(request, who, "DASH_FAILBACK_ABORT", "/failback/abort", {})


# ---- static UI (built by Vite) -----------------------------------------------------------------------------------------------------
_static = Path(os.environ.get("SS_DASH_STATIC", "/srv/dashboard"))
if _static.exists():
    app.mount("/assets", StaticFiles(directory=_static / "assets"), name="assets")

    @app.get("/{path:path}")
    async def spa(path: str):
        f = _static / path
        return FileResponse(f if path and f.is_file() else _static / "index.html")
