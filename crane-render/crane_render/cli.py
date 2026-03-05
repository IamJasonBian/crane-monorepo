"""CLI entry points for crane-render utilities."""

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

from crane_render.compaction.compactor import Compactor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")


def compact():
    parser = argparse.ArgumentParser(description="Compact hourly Parquet parts into daily files")
    parser.add_argument("--date", type=str, default=None,
                        help="Date to compact (YYYY-MM-DD). Defaults to yesterday.")
    parser.add_argument("--output-dir", type=str,
                        default=os.environ.get("RENDER_OUTPUT_DIR", "/data/crane-render"))
    args = parser.parse_args()

    date_str = args.date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    compactor = Compactor(args.output_dir)
    results = compactor.compact_all(date_str)

    for topic, count in results.items():
        print(f"  {topic}: {count} rows")

    if not results:
        print(f"No data to compact for {date_str}")
