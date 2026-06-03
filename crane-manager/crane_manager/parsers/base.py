"""Common contract every parser-type system implements.

A "parser" is any system that turns an input payload into a normalized,
JSON-serializable record. The gateway (api/parsers.py) routes requests to
parsers purely through this interface, so new parser systems can be added by
dropping a module + registering it — no new Render service required.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from crane_manager.parsers.config import parser_env

if TYPE_CHECKING:
    from fastapi import APIRouter


class ParserError(Exception):
    """Raised for bad/missing input. The gateway maps this to HTTP 422."""


class BaseParser(ABC):
    #: URL-safe identifier, e.g. "email". Used as the route segment (mount prefix).
    name: str
    #: Human-readable one-liner shown in the parser catalog.
    description: str = ""

    @abstractmethod
    def parse(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Transform an input payload into a normalized record."""

    def input_schema(self) -> dict[str, Any]:
        """Optional: describe expected payload keys for the catalog/UI."""
        return {}

    def config(self) -> dict[str, str]:
        """This parser's own namespaced env (PARSER_<NAME>_*). Isolated per parser."""
        return parser_env(self.name)

    def routes(self) -> "APIRouter | None":
        """Optional: a parser's own isolated URL tree, mounted at /api/parsers/<name>.

        Return None to get the default routes (POST /parse, GET /info). Override
        to expose extra endpoints — they stay confined to this parser's prefix.
        """
        return None
