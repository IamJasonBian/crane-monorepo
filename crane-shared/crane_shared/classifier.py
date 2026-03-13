"""Hard classifier for eBay listing titles.

Determines whether a listing is actually the target product vs noise
returned by eBay search. Each classifier rule returns True/False.
"""

from __future__ import annotations

import re


def is_crucial_t705_2tb(title: str) -> bool:
    """Return True if the listing is genuinely a Crucial T705 2TB SSD.

    Rejects:
    - Other Crucial models (T500, P310, P3, P2, MX500)
    - Other brands (Samsung, WD, SK Hynix, Kioxia, Netac, Kingston, etc.)
    - Wrong capacity (1TB, 4TB, 500GB)
    - Accessories / heatsink-only listings
    """
    t = title.lower()

    # Must contain T705
    if "t705" not in t:
        return False

    # Must be 2TB capacity
    if not re.search(r"2\s*tb", t):
        return False

    # Reject if it's a multi-variant listing that includes non-2TB options
    # e.g. "1TB 2TB 4TB" — these are storefront listings, not a specific item
    # Only match standalone capacities like "1TB", "2TB", "4TB" — not "1200TBW" or "14500MB"
    capacity_matches = re.findall(r"(?<!\d)\d{1,2}\s*tb(?!\w)", t)
    if len(capacity_matches) > 1:
        return False

    # Reject heatsink-only / accessory listings
    accessory_patterns = [
        r"heatsink\s+only",
        r"heat\s*sink\s+for",
        r"cooler\s+for",
        r"replacement\s+heatsink",
    ]
    for pat in accessory_patterns:
        if re.search(pat, t):
            return False

    # Reject non-Crucial brands that somehow mention T705
    reject_brands = [
        "samsung", "western digital", "seagate", "kingston",
        "kioxia", "netac", "sk hynix", "hynix", "inland",
        "sabrent", "teamgroup", "team group", "adata",
        "patriot", "corsair", "pny",
    ]
    for brand in reject_brands:
        if brand in t:
            return False

    # Reject other Crucial models that might co-occur
    reject_models = ["t500", "p310", "p3 ", "p3+", "p2 ", "mx500", "bx500", "p5"]
    for model in reject_models:
        if model in t and "t705" in t:
            # Only reject if the other model appears as primary product
            # e.g. "P310 1TB" with "compatible with T705" in description
            t705_pos = t.index("t705")
            model_pos = t.index(model)
            if model_pos < t705_pos:
                return False

    return True


# Registry of classifiers keyed by search term query
CLASSIFIERS: dict[str, callable] = {
    "Crucial t705 2tb": is_crucial_t705_2tb,
    "crucial t705 2tb": is_crucial_t705_2tb,
}


def classify_listing(query: str, title: str) -> bool:
    """Check if a listing title matches the intended product for a search query.

    Returns True if the listing passes classification (is the real product),
    or True if no classifier exists for the query (passthrough).
    """
    classifier = CLASSIFIERS.get(query)
    if classifier is None:
        return True  # no classifier = allow all
    return classifier(title)
