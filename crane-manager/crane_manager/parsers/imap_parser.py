"""IMAP crawler parser — proxies to the Java IMAP crawler service.

The crawler (event_api/email-crawler) is a headless worker that crawls inboxes
into Postgres. When it exposes an HTTP face, point this parser at it:

    PARSER_IMAP_UPSTREAM_URL=https://<crawler-host>      # required
    PARSER_IMAP_TIMEOUT=15                                # optional, seconds

Until configured, /api/parsers/imap/parse returns a clear 422 ("not
configured"), so the route exists and is discoverable without a live upstream.
"""

from __future__ import annotations

from crane_manager.parsers.proxy import HttpProxyParser
from crane_manager.parsers.registry import register


@register
class ImapProxyParser(HttpProxyParser):
    name = "imap"
    description = "Proxy to the Java IMAP crawler service (parses/serves crawled mail)."
