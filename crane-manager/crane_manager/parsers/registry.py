"""In-process registry of parser-type systems.

Each parser self-registers via the @register decorator at import time. The
gateway looks parsers up here by name. This is what lets crane-manager host
"many parser type systems" behind one service.
"""

from __future__ import annotations

from crane_manager.parsers.base import BaseParser

_REGISTRY: dict[str, BaseParser] = {}


def register(parser_cls: type[BaseParser]) -> type[BaseParser]:
    """Class decorator: instantiate and register a parser by its `name`."""
    inst = parser_cls()
    if not getattr(inst, "name", None):
        raise ValueError(f"{parser_cls.__name__} must define a non-empty `name`")
    if inst.name in _REGISTRY:
        raise ValueError(f"duplicate parser name: {inst.name!r}")
    _REGISTRY[inst.name] = inst
    return parser_cls


def get(name: str) -> BaseParser | None:
    return _REGISTRY.get(name)


def all_parsers() -> list[BaseParser]:
    return sorted(_REGISTRY.values(), key=lambda p: p.name)
