"""Base for parsers that proxy to an upstream HTTP service.

Lets the gateway host a parser-type system that runs in another process/service
(e.g. the Java IMAP crawler) while keeping the same `/api/parsers/<name>`
contract. The upstream URL is read from the parser's namespaced config
(`PARSER_<NAME>_UPSTREAM_URL`), so it stays isolated from other parsers.

Uses stdlib urllib (no runtime dependency added). Parser routes are sync, so
FastAPI runs them in a threadpool — a blocking call here does not stall the
event loop.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from crane_manager.parsers.base import BaseParser, ParserError


class HttpProxyParser(BaseParser):
    #: Path appended to the upstream base URL for parse requests.
    upstream_path = "/parse"

    def input_schema(self) -> dict[str, Any]:
        return {
            "_proxy": f"payload is forwarded as JSON POST to "
                      f"PARSER_{self.name.upper()}_UPSTREAM_URL{self.upstream_path}",
        }

    def _upstream(self) -> tuple[str, float]:
        cfg = self.config()  # PARSER_<NAME>_* only — isolated from other parsers
        url = cfg.get("upstream_url")
        if not url:
            raise ParserError(
                f"{self.name} proxy not configured: set PARSER_{self.name.upper()}_UPSTREAM_URL"
            )
        try:
            timeout = float(cfg.get("timeout", "15"))
        except ValueError:
            raise ParserError(f"PARSER_{self.name.upper()}_TIMEOUT must be a number")
        return url.rstrip("/") + self.upstream_path, timeout

    def parse(self, payload: dict[str, Any]) -> dict[str, Any]:
        target, timeout = self._upstream()
        req = urllib.request.Request(
            target,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode()
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:200] if e.fp else ""
            raise ParserError(f"upstream returned HTTP {e.code}: {detail}") from e
        except urllib.error.URLError as e:
            raise ParserError(f"upstream unreachable: {e.reason}") from e

        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"raw": body}  # upstream returned non-JSON; pass it through
