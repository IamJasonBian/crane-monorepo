"""Email parser — normalizes a raw RFC822 message into a flat record.

This is the Python counterpart of the Java IMAP crawler's MessageParser: same
output shape (from/to/cc, subject, dates, text+html bodies, attachment
metadata), so an IMAP worker and this gateway endpoint agree on the schema.
"""

from __future__ import annotations

import email
import email.policy
from email.message import EmailMessage
from typing import Any

from crane_manager.parsers.base import BaseParser, ParserError
from crane_manager.parsers.registry import register


@register
class EmailParser(BaseParser):
    name = "email"
    description = "Parse a raw RFC822 email into normalized fields and attachment metadata."

    def input_schema(self) -> dict[str, Any]:
        return {"raw": "string — full RFC822 message text (headers + body)"}

    def parse(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw = payload.get("raw")
        if not raw or not isinstance(raw, str):
            raise ParserError("missing 'raw': full RFC822 message text is required")

        msg: EmailMessage = email.message_from_string(raw, policy=email.policy.default)

        text_parts: list[str] = []
        html_parts: list[str] = []
        attachments: list[dict[str, Any]] = []

        for part in msg.walk():
            if part.is_multipart():
                continue
            filename = part.get_filename()
            disposition = (part.get_content_disposition() or "")
            ctype = part.get_content_type()

            if disposition == "attachment" or (filename and not ctype.startswith("text/")):
                payload_bytes = part.get_payload(decode=True) or b""
                attachments.append({
                    "filename": filename,
                    "content_type": ctype,
                    "size_bytes": len(payload_bytes),
                })
                continue

            if ctype == "text/plain":
                text_parts.append(part.get_content())
            elif ctype == "text/html":
                html_parts.append(part.get_content())

        return {
            "message_id": msg.get("Message-ID"),
            "in_reply_to": msg.get("In-Reply-To"),
            "from_addr": msg.get("From"),
            "to_addrs": msg.get("To"),
            "cc_addrs": msg.get("Cc"),
            "subject": msg.get("Subject"),
            "sent_at": msg.get("Date"),
            "body_text": "".join(text_parts) or None,
            "body_html": "".join(html_parts) or None,
            "attachments": attachments,
        }
