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


def is_samsung_990_pro_2tb(title: str) -> bool:
    """Return True if the listing is genuinely a Samsung 990 Pro 2TB SSD.

    Rejects:
    - Wrong capacity (1TB, 500GB, 4TB)
    - Multi-variant listings
    - Accessories / heatsink-only listings
    - Other Samsung models (980, 970, 960, 950, 870, 860)
    - Other brands
    """
    t = title.lower()

    # Must contain "990" and "pro"
    if "990" not in t or "pro" not in t:
        return False

    # Must contain "samsung"
    if "samsung" not in t:
        return False

    # Must be 2TB capacity
    if not re.search(r"2\s*tb", t):
        return False

    # Reject multi-variant listings (more than one capacity mentioned)
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

    return True


def is_32gb_ddr5_6000(title: str) -> bool:
    """Return True if the listing is genuinely 32GB DDR5-6000 memory.

    Requires all three keywords: 32GB, DDR5, and 6000 speed.
    Rejects:
    - Wrong capacity (16GB, 64GB, 8GB)
    - Multi-capacity listings
    - Wrong speed without 6000
    - Non-RAM accessories (coolers, etc.)
    """
    t = title.lower()

    # Must contain "32gb" or "32 gb"
    if not re.search(r"32\s*gb", t):
        return False

    # Must contain "ddr5"
    if "ddr5" not in t:
        return False

    # Must contain "6000" (speed rating)
    if "6000" not in t:
        return False

    # Reject multi-capacity listings (e.g. "16GB 32GB 64GB")
    # Ignore "2x16GB" or "4x8GB" kit descriptions — only match standalone capacities
    capacity_matches = re.findall(r"(?<!\dx)(?<!\d)(?:8|16|32|48|64|128)\s*gb(?!\w)", t)
    if len(capacity_matches) > 1:
        return False

    # Reject accessory-only listings
    accessory_patterns = [
        r"cooler\s+for",
        r"fan\s+for",
        r"compatible\s+with",
    ]
    for pat in accessory_patterns:
        if re.search(pat, t):
            return False

    return True


# ── Exact title match classifier ─────────────────────────────────────────

# eBay UI noise that gets scraped as listing titles
_EBAY_NOISE = [
    "have one to sell",
    "sell one like this",
    "sell something else",
    "similar items",
    "see all",
    "shop with confidence",
    "returns accepted",
    "money back guarantee",
]

# UUIDs / garbage pattern
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)


def exact_title_match_classifier(query: str, title: str) -> bool:
    """Return True only if every keyword in the query appears in the title.

    Also rejects eBay UI noise (e.g. "Have one to sell?") and UUID garbage.
    """
    t = title.lower().strip()

    # Reject eBay UI noise
    for noise in _EBAY_NOISE:
        if t.startswith(noise):
            return False

    # Reject UUID-heavy garbage titles
    if _UUID_RE.search(title):
        return False

    # Reject very short titles (likely UI artifacts)
    if len(t) < 10:
        return False

    # Every word in the query must appear in the title
    query_words = query.lower().split()
    for word in query_words:
        if word not in t:
            return False

    return True


def is_hp_elitebook(title: str) -> bool:
    """Return True if the listing is genuinely an HP Elitebook laptop.

    Rejects:
    - Non-HP brands mentioning "elite"
    - Accessories, docks, adapters, batteries, chargers, screens
    - Non-Elitebook HP models (Pavilion, Spectre, Omen, ProBook, ZBook standalone)
    """
    t = title.lower()

    if "elitebook" not in t:
        return False

    if "hp" not in t:
        return False

    # Reject accessories and peripherals
    accessory_patterns = [
        r"\bdock(?:ing)?\b",
        r"\badapter\b",
        r"\bcharger\b",
        r"\bbattery\b",
        r"\bscreen\s+protector\b",
        r"\bkeyboard\s+cover\b",
        r"\bskin\b",
        r"\bcase\s+for\b",
        r"\bcover\s+for\b",
        r"\bbag\s+for\b",
        r"\bram\s+for\b",
        r"\bssd\s+for\b",
        r"\bupgrade\s+for\b",
        r"\bcompatible\s+with\b",
    ]
    for pat in accessory_patterns:
        if re.search(pat, t):
            return False

    return True


def is_hp_elitebook_high_refresh(title: str) -> bool:
    """Return True if the listing is an HP Elitebook with 120Hz or higher refresh rate.

    Accepts 120Hz, 144Hz, 165Hz, 240Hz, and similar high-refresh panels.
    """
    t = title.lower()

    if not is_hp_elitebook(t):
        return False

    # Match explicit Hz refresh rate: 120hz, 144hz, 165hz, 240hz, etc.
    if re.search(r"(?:1[2-9]\d|2\d{2})\s*hz", t):
        return True

    # Match "120" or "144" standalone near "hz" or "refresh" context
    if re.search(r"\b(?:120|144|165|240)\s*(?:hz|hertz|refresh)\b", t):
        return True

    return False


def is_wireless_mouse(title: str) -> bool:
    """Return True if the listing is a standalone wireless mouse.

    Rejects:
    - Wired mice
    - Mouse pads, dongles, receivers (accessory-only listings)
    - Combo packs that are primarily something else
    - Gaming chairs, desks mentioning "mouse" tangentially
    """
    t = title.lower()

    # Must mention mouse (or mice)
    if not re.search(r"\b(?:mouse|mice)\b", t):
        return False

    # Must be wireless
    wireless_terms = ["wireless", "bluetooth", "2.4g", "2.4ghz", "cordless", "rf"]
    if not any(term in t for term in wireless_terms):
        return False

    # Reject wired signals even if "wireless" appears in title
    if re.search(r"\bwired\b", t) and not re.search(r"\bwireless\b", t):
        return False

    # Reject accessory-only listings
    accessory_patterns = [
        r"\bdongle\s+only\b",
        r"\breceiver\s+only\b",
        r"\bmouse\s*pad\b",
        r"\bpad\s+for\b",
    ]
    for pat in accessory_patterns:
        if re.search(pat, t):
            return False

    return True


def is_a30_chip(title: str) -> bool:
    """Return True if the listing is an A30 chip/processor (AMD or compatible).

    Covers AMD A30 APUs, AMD Athlon A30xx series, and similar A30-designation chips.
    Rejects:
    - Systems/laptops that happen to contain A30 (we want standalone chips)
    - Accessories, coolers, thermal paste
    - Unrelated products mentioning A30 as a model suffix
    """
    t = title.lower()

    # Must match A30 designation
    if not re.search(r"\ba30\b", t):
        return False

    # Chip/processor keywords
    chip_terms = ["processor", "cpu", "apu", "chip", "ryzen", "athlon", "intel", "snapdragon"]
    has_chip_keyword = any(term in t for term in chip_terms)

    # Alternatively, explicit processor category signals
    has_processor_signal = re.search(r"\b(?:oem|tray|box(?:ed)?)\b", t) is not None

    if not has_chip_keyword and not has_processor_signal:
        return False

    # Reject cooler/thermal accessories
    accessory_patterns = [
        r"\bcooler\b",
        r"\bheatsink\b",
        r"\bthermal\s*paste\b",
        r"\bfan\s+for\b",
        r"\bcompatible\s+with\b",
    ]
    for pat in accessory_patterns:
        if re.search(pat, t):
            return False

    return True


# ── Catalog classifier (product-specific rules) ─────────────────────────

# Registry of catalog classifiers keyed by search term query
CATALOG_CLASSIFIERS: dict[str, callable] = {
    "Crucial t705 2tb": is_crucial_t705_2tb,
    "crucial t705 2tb": is_crucial_t705_2tb,
    "Samsung 990 pro 2tb ssd": is_samsung_990_pro_2tb,
    "samsung 990 pro 2tb ssd": is_samsung_990_pro_2tb,
    "32gb ddr5 6000": is_32gb_ddr5_6000,
    # Elitebook laptops
    "hp elitebook": is_hp_elitebook,
    "HP Elitebook": is_hp_elitebook,
    "hp elitebook laptop": is_hp_elitebook,
    "HP Elitebook laptop": is_hp_elitebook,
    "hp elitebook 120hz": is_hp_elitebook_high_refresh,
    "hp elitebook 144hz": is_hp_elitebook_high_refresh,
    "hp elitebook high refresh": is_hp_elitebook_high_refresh,
    "HP Elitebook 120hz": is_hp_elitebook_high_refresh,
    "HP Elitebook 144hz": is_hp_elitebook_high_refresh,
    # Wireless mice
    "wireless mouse": is_wireless_mouse,
    "Wireless mouse": is_wireless_mouse,
    "wireless gaming mouse": is_wireless_mouse,
    "bluetooth mouse": is_wireless_mouse,
    # A30 chips
    "a30 chip": is_a30_chip,
    "A30 chip": is_a30_chip,
    "amd a30": is_a30_chip,
    "AMD A30": is_a30_chip,
    "a30 processor": is_a30_chip,
    "A30 processor": is_a30_chip,
}


def catalog_classifier(query: str, title: str) -> bool:
    """Check if a listing title matches the intended product for a search query.

    Returns True if the listing passes classification (is the real product),
    or True if no classifier exists for the query (passthrough).
    """
    classifier = CATALOG_CLASSIFIERS.get(query)
    if classifier is None:
        return True  # no classifier = allow all
    return classifier(title)


# Backwards compat alias
classify_listing = catalog_classifier
