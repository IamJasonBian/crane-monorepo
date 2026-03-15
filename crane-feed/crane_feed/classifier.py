"""Re-export classifier from crane-shared for backward compatibility."""

from crane_shared.classifier import (
    classify_listing,
    catalog_classifier,
    exact_title_match_classifier,
    is_crucial_t705_2tb,
    CATALOG_CLASSIFIERS,
)

# Backwards compat
CLASSIFIERS = CATALOG_CLASSIFIERS

__all__ = [
    "classify_listing",
    "catalog_classifier",
    "exact_title_match_classifier",
    "is_crucial_t705_2tb",
    "CATALOG_CLASSIFIERS",
    "CLASSIFIERS",
]
