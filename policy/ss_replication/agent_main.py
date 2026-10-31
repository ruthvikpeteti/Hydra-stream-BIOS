"""Source replication agent service (one per region). GET /health, /status."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .checkpoints import CheckpointStore
from .source_agent import Config, Manager

import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = Config.from_env()
    store = await CheckpointStore.connect(os.environ["SS_DATABASE_URL"])
    mgr = Manager(cfg, store)
    app.state.mgr = mgr
    task = asyncio.create_task(mgr.run())
    yield
    await mgr.shutdown()  # drains in-flight batches, checkpoints what was acknowledged
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await store.close()


app = FastAPI(title="SovereignStream Replication Agent", version="0.4.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    return JSONResponse(app.state.mgr.snapshot())
