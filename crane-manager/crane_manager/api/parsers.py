"""Parser gateway — isolated per-parser routing on a shared host.

Each parser-type system is mounted as its own sub-router under a fixed prefix
(`/api/parsers/<name>`), so distinct URLs route to distinct "services" that
nonetheless share one process. Isolation guarantees (anti wire-crossing):

  * unique name => unique mount prefix (enforced by the registry), so no two
    parsers can ever claim the same URL;
  * each parser gets its own APIRouter (no shared `{name}` dispatcher to cross);
  * every invocation runs inside a per-parser error boundary — a crash in one
    parser yields a scoped 500 and never leaks another parser's data;
  * config is namespaced per parser (see parsers/config.py).

    GET  /api/parsers              -> catalog of mounted parsers
    GET  /api/parsers/{name}/info  -> one parser's metadata + input schema
    POST /api/parsers/{name}/parse -> run that parser
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, HTTPException

from crane_manager.parsers import registry
from crane_manager.parsers.base import BaseParser, ParserError

log = logging.getLogger("crane-manager.parsers")


def _meta(p: BaseParser) -> dict[str, Any]:
    return {"name": p.name, "description": p.description, "input_schema": p.input_schema()}


def _invoke(parser: BaseParser, payload: dict[str, Any]) -> dict[str, Any]:
    """Run a parser inside its error boundary. Failures stay scoped to it."""
    try:
        return parser.parse(payload)
    except ParserError as e:
        raise HTTPException(status_code=422, detail={"parser": parser.name, "error": str(e)}) from e
    except HTTPException:
        raise
    except Exception:  # contain any parser bug — don't crash the host or bleed state
        log.exception("parser %r raised", parser.name)
        raise HTTPException(status_code=500, detail={"parser": parser.name, "error": "internal parser error"})


def _default_router(parser: BaseParser) -> APIRouter:
    """Default isolated route tree for a parser that doesn't define its own."""
    r = APIRouter()

    @r.get("/info")
    def info():  # noqa: ANN202
        return _meta(parser)

    @r.post("/parse")
    def parse(payload: dict[str, Any] = Body(default_factory=dict)):  # noqa: ANN202
        return {"parser": parser.name, "result": _invoke(parser, payload)}

    return r


def build_parsers_router() -> APIRouter:
    """Build the gateway: a catalog endpoint plus one isolated mount per parser.

    Called once at startup, after all parsers have registered.
    """
    root = APIRouter()

    @root.get("/")
    def catalog():  # noqa: ANN202
        return [_meta(p) for p in registry.all_parsers()]

    seen_prefixes: set[str] = set()
    for p in registry.all_parsers():
        prefix = f"/{p.name}"
        if prefix in seen_prefixes:  # defense-in-depth; registry already blocks dup names
            raise ValueError(f"parser prefix collision: {prefix}")
        seen_prefixes.add(prefix)
        sub = p.routes() or _default_router(p)
        root.include_router(sub, prefix=prefix)

    return root
