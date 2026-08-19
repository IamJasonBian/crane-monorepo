"""Bill-of-materials comparison endpoints.

A BomSpec anchors on a physical chassis and lists the components that populate
it. /compare charts the chassis price over time and the whole-build total,
both derived from the per-day price series written by crane-feed.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from crane_shared.models import BomSpec, SearchTerm
from crane_manager.api.prices import read_series
from crane_manager.deps import get_redis

router = APIRouter()

BOM_INDEX = "crane:manager:boms:index"


def _load_bom(rc, bom_id: str) -> BomSpec | None:
    return rc.get_model(f"crane:manager:boms:{bom_id}", BomSpec)


def _term_label(rc, term_id: str) -> str:
    term = rc.get_model(f"crane:manager:terms:{term_id}", SearchTerm)
    return term.query if term else term_id


def _build_total(line_series: list[dict]) -> list[dict]:
    """Sum per-line cheapest price across the union of observed dates.

    Each line contributes low × qty on any given day; a line with no point yet
    for that day carries its most recent earlier point forward. Days before a
    line's first observation simply omit it (partial total).

    Args:
        line_series: One entry per BOM line, each {qty, points: [snapshot, ...]}.

    Returns:
        [{date, total, complete}] ordered oldest → newest, where `complete`
        marks days on which every line had a price.
    """
    all_dates = sorted({p["date"] for ls in line_series for p in ls["points"]})
    totals: list[dict] = []
    for date in all_dates:
        total = 0.0
        priced_lines = 0
        for ls in line_series:
            # latest point on-or-before `date`
            latest = None
            for p in ls["points"]:
                if p["date"] <= date:
                    latest = p
                else:
                    break
            if latest is not None:
                total += latest.get("low", 0.0) * ls["qty"]
                priced_lines += 1
        totals.append({
            "date": date,
            "total": round(total, 2),
            "complete": priced_lines == len(line_series),
        })
    return totals


@router.get("/boms")
def list_boms():
    """List all bill-of-materials specs."""
    rc = get_redis()
    bom_ids = rc.get_index(BOM_INDEX)
    boms = []
    for bid in sorted(bom_ids):
        bom = _load_bom(rc, bid)
        if bom:
            boms.append(bom.model_dump())
    return boms


@router.get("/bom/{bom_id}/history")
def get_bom_history(
    bom_id: str,
    source: str = "ebay",
    date_from: str | None = None,
    date_to: str | None = None,
):
    """Chassis series, per-line series, and whole-build total over time."""
    rc = get_redis()
    bom = _load_bom(rc, bom_id)
    if not bom:
        raise HTTPException(status_code=404, detail=f"BOM {bom_id} not found")

    lines_out = []
    line_series = []
    chassis_points: list[dict] = []
    for line in bom.lines:
        points = read_series(rc, line.term_id, source, date_from, date_to)
        lines_out.append({
            "role": line.role,
            "term_id": line.term_id,
            "label": _term_label(rc, line.term_id),
            "qty": line.qty,
            "series": points,
        })
        line_series.append({"qty": line.qty, "points": points})
        if line.term_id == bom.chassis_term_id:
            chassis_points = points

    return {
        "bom_id": bom.bom_id,
        "name": bom.name,
        "chassis_term_id": bom.chassis_term_id,
        "chassis": chassis_points,
        "lines": lines_out,
        "total": _build_total(line_series),
    }
