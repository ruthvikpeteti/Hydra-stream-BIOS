"""HTTP surface of the failover controller. Reads need no key (status carries no payloads or secrets); every operation that
can change who is active needs the admin key."""

import hmac
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from .adapters import Config
from .controller import DR
from .coord import CoordinationUnavailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = Config.from_env()
    if not cfg.admin_key:
        raise RuntimeError("SS_DR_ADMIN_KEY is required")
    app.state.dr = DR(cfg)
    await app.state.dr.start()
    yield
    await app.state.dr.stop()


app = FastAPI(title="SovereignStream DR controller", version="0.6.0", lifespan=lifespan)


def admin(request: Request, x_admin_key: Optional[str] = Header(None)) -> str:
    if not x_admin_key or not hmac.compare_digest(x_admin_key, request.app.state.dr.cfg.admin_key):
        raise HTTPException(401, "admin key required")
    return "admin"


@app.get("/dr/v1/health")
async def health():
    return {"status": "ok"}


@app.get("/dr/v1/status")
async def status(request: Request):
    return request.app.state.dr.snapshot()


@app.get("/dr/v1/audit")
async def audit(request: Request):
    try:
        return {"entries": await request.app.state.dr.coord.audit()}
    except CoordinationUnavailable as exc:
        raise HTTPException(503, f"COORDINATION_UNAVAILABLE: {exc}")


class FailoverIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target: Optional[str] = None
    reason: str = Field("operator request", max_length=200)
    approved_by: Optional[str] = Field(None, max_length=100)
    accept_degraded_rpo: bool = False


@app.post("/dr/v1/failover")
async def failover(body: FailoverIn, request: Request, _: str = Depends(admin)):
    return await request.app.state.dr.manual_failover(body.target, body.reason, body.approved_by, body.accept_degraded_rpo)


class CrashIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    step: Optional[str] = Field(None, pattern="^(AFTER_FENCE|AFTER_TOPOLOGY|AFTER_ACTIVATE|FAILBACK_AFTER_SEED|FAILBACK_AFTER_FENCE|FAILBACK_AFTER_ACTIVATE)$")


@app.post("/dr/v1/debug/crash")
async def arm_crash(body: CrashIn, request: Request, _: str = Depends(admin)):
    """TEST ONLY (SS_DR_FAULTS=1): the process exits abruptly right after the named step has been persisted."""
    dr = request.app.state.dr
    if not dr.cfg.faults:
        raise HTTPException(404)
    dr.crash_at = body.step
    return {"armed": body.step}


class DownIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    region: str
    down: bool = True


@app.post("/dr/v1/debug/declare-down")
async def declare_down(body: DownIn, request: Request, _: str = Depends(admin)):
    """TEST ONLY: simulate a false-positive failure detector (the region is alive but declared dead)."""
    dr = request.app.state.dr
    if not dr.cfg.faults:
        raise HTTPException(404)
    (dr.forced_down.add if body.down else dr.forced_down.discard)(body.region)
    return {"forced_down": sorted(dr.forced_down)}


class FailbackStart(BaseModel):
    model_config = ConfigDict(extra="forbid")
    region: str
    requested_by: str = Field(max_length=100)


class FailbackApprove(BaseModel):
    model_config = ConfigDict(extra="forbid")
    approved_by: str = Field(min_length=1, max_length=100)


@app.post("/dr/v1/failback/start")
async def failback_start(body: FailbackStart, request: Request, _: str = Depends(admin)):
    return await request.app.state.dr.failback_start(body.region, body.requested_by)


@app.post("/dr/v1/failback/approve")
async def failback_approve(body: FailbackApprove, request: Request, _: str = Depends(admin)):
    return await request.app.state.dr.failback_approve(body.approved_by)


@app.post("/dr/v1/failback/abort")
async def failback_abort(request: Request, _: str = Depends(admin)):
    return await request.app.state.dr.failback_abort("admin")
