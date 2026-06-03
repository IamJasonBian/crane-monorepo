"""Regex parser — applies a pattern to text and returns structured matches.

A second parser type to demonstrate the gateway hosting more than one system.
Useful for ad-hoc field extraction (e.g. pulling identifiers out of logs).
"""

from __future__ import annotations

import re
from typing import Any

from crane_manager.parsers.base import BaseParser, ParserError
from crane_manager.parsers.registry import register

_FLAGS = {"i": re.IGNORECASE, "m": re.MULTILINE, "s": re.DOTALL, "x": re.VERBOSE}


@register
class RegexParser(BaseParser):
    name = "regex"
    description = "Apply a regex to text; returns named-group dicts (or full match) per occurrence."

    def input_schema(self) -> dict[str, Any]:
        return {
            "pattern": "string — Python regex; named groups (?P<x>...) become keys",
            "text": "string — input to scan",
            "flags": "optional string — any of i,m,s,x",
        }

    def parse(self, payload: dict[str, Any]) -> dict[str, Any]:
        pattern = payload.get("pattern")
        text = payload.get("text")
        if not pattern or not isinstance(pattern, str):
            raise ParserError("missing 'pattern'")
        if text is None or not isinstance(text, str):
            raise ParserError("missing 'text'")

        flag_val = 0
        for ch in (payload.get("flags") or ""):
            if ch not in _FLAGS:
                raise ParserError(f"unknown flag {ch!r}; allowed: {''.join(_FLAGS)}")
            flag_val |= _FLAGS[ch]

        try:
            rx = re.compile(pattern, flag_val)
        except re.error as e:
            raise ParserError(f"invalid regex: {e}") from e

        matches = [m.groupdict() or {"match": m.group(0)} for m in rx.finditer(text)]
        return {"count": len(matches), "matches": matches}
