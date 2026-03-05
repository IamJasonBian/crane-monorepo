"""Entry point for crane-manager.

Starts the FastAPI server with CRUD endpoints for strategies, targets,
budgets, and system health monitoring.
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI

from fastapi.staticfiles import StaticFiles

from crane_manager.api.strategies import router as strategies_router
from crane_manager.api.targets import router as targets_router
from crane_manager.api.budget import router as budget_router
from crane_manager.api.health import router as health_router
from crane_manager.api.market import router as market_router
from crane_manager.api.orders import router as orders_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("crane-manager")

app = FastAPI(title="Crane Manager", version="0.1.0")

app.include_router(strategies_router, prefix="/api/strategies", tags=["strategies"])
app.include_router(targets_router, prefix="/api/targets", tags=["targets"])
app.include_router(budget_router, prefix="/api/budget", tags=["budget"])
app.include_router(health_router, prefix="/api/health", tags=["health"])
app.include_router(market_router, prefix="/api/market", tags=["market"])
app.include_router(orders_router, prefix="/api/orders", tags=["orders"])


@app.get("/")
def root():
    return {"service": "crane-manager", "version": "0.1.0"}


def main():
    log.info("Starting crane-manager API")
    uvicorn.run("crane_manager.main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()
