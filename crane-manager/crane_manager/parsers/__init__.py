"""Parser-type systems hosted by the crane gateway.

Importing this package registers all built-in parsers. To add a new parser
system: create a module here, subclass BaseParser, decorate with @register,
and import it below.
"""

from __future__ import annotations

from crane_manager.parsers import registry
from crane_manager.parsers.base import BaseParser, ParserError

# Import side effects register each parser in the registry.
from crane_manager.parsers import email_parser  # noqa: F401
from crane_manager.parsers import regex_parser  # noqa: F401
from crane_manager.parsers import imap_parser  # noqa: F401

__all__ = ["registry", "BaseParser", "ParserError"]
