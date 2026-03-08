"""Re-export classifier from crane-shared for backward compatibility."""

from crane_shared.classifier import classify_listing, is_crucial_t705_2tb, CLASSIFIERS

__all__ = ["classify_listing", "is_crucial_t705_2tb", "CLASSIFIERS"]
