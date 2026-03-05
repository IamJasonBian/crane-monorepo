"""Configuration for crane-render, loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class RenderConfig:
    flush_interval_s: float = float(os.environ.get("RENDER_FLUSH_INTERVAL_S", "300"))
    output_dir: str = os.environ.get("RENDER_OUTPUT_DIR", "/data/crane-render")
    batch_size: int = int(os.environ.get("RENDER_BATCH_SIZE", "500"))
    block_ms: int = int(os.environ.get("RENDER_BLOCK_MS", "2000"))
    cursor_prefix: str = "crane:render:cursors"
    auto_compact: bool = os.environ.get("RENDER_AUTO_COMPACT", "false").lower() == "true"
    compact_hour_utc: int = int(os.environ.get("RENDER_COMPACT_HOUR_UTC", "0"))
