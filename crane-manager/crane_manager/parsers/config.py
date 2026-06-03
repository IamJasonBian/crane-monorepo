"""Per-parser config isolation.

Each parser system only sees environment variables namespaced to its own name,
so one parser can never read another's secrets/config (a "wire-crossing" guard).

    PARSER_EMAIL_IMAP_HOST=...   -> email parser sees {"imap_host": "..."}
    PARSER_REGEX_MAX_MATCHES=... -> regex parser sees {"max_matches": "..."}
"""

from __future__ import annotations

import os


def parser_env(name: str) -> dict[str, str]:
    """Return env vars for one parser, prefix-stripped and lower-cased."""
    prefix = f"PARSER_{name.upper().replace('-', '_')}_"
    return {
        key[len(prefix):].lower(): value
        for key, value in os.environ.items()
        if key.startswith(prefix)
    }
