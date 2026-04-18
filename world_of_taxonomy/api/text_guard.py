"""Lightweight input guard for free-text fields.

Today /classify runs Postgres full-text search only, so prompt-injection
is not an active attack vector against our DB. But the roadmap calls
for adding an LLM leg (Anthropic Claude) for fallback when FTS scores
are low; we harden the input now so the same path is safe then.

Also used by /contact and other user-submitted free-text fields to
strip control characters and reject obvious payloads.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Tuple

from prometheus_client import Counter

INJECTION_REJECTIONS = Counter(
    "wot_text_guard_rejections_total",
    "Inputs rejected by the text guard.",
    ["reason"],
)

# Obvious prompt-injection patterns. Kept conservative: most taxonomy
# queries are short industry/occupation descriptions, so these phrases
# should never appear in a legitimate query.
_INJECTION_PATTERNS = [
    re.compile(r"\bignore\s+(the\s+|all\s+)?(previous|prior|above)\b", re.I),
    re.compile(r"\bdisregard\s+(the\s+|all\s+)?(previous|prior|above)\b", re.I),
    re.compile(r"\bforget\s+(everything|all|previous|your\s+instructions)\b", re.I),
    re.compile(r"\b(you\s+are\s+now|act\s+as|pretend\s+to\s+be)\b", re.I),
    re.compile(r"\bsystem\s*(prompt|message|role)\b", re.I),
    re.compile(r"<\|?(im_start|im_end|system|endoftext)\|?>", re.I),
    re.compile(r"\[\[\s*system\s*\]\]", re.I),
    re.compile(r"```\s*system\b", re.I),
    # ChatML role markers
    re.compile(r"\brole\s*[:=]\s*(system|assistant|user)\b", re.I),
    # Common exfiltration attempts
    re.compile(r"\b(reveal|print|repeat|show)\s+(your\s+)?(system\s+)?(prompt|instructions)\b", re.I),
]

_CTRL_RX = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")


class TextGuardError(ValueError):
    """Raised when user input trips a guard rule."""

    def __init__(self, reason: str, public_message: str):
        super().__init__(reason)
        self.reason = reason
        self.public_message = public_message


def sanitize(text: str, *, max_length: int = 500) -> str:
    """Normalize and strip unsafe chars.

    - NFKC normalise
    - strip control chars
    - collapse runs of whitespace
    - enforce max_length
    """
    if not isinstance(text, str):
        raise TextGuardError("not_a_string", "Invalid input type.")
    t = unicodedata.normalize("NFKC", text)
    t = _CTRL_RX.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > max_length:
        INJECTION_REJECTIONS.labels(reason="too_long").inc()
        raise TextGuardError(
            "too_long",
            f"Input exceeds maximum length of {max_length} characters.",
        )
    if len(t) == 0:
        INJECTION_REJECTIONS.labels(reason="empty").inc()
        raise TextGuardError("empty", "Input is empty after normalisation.")
    return t


def check_injection(text: str) -> None:
    """Raise TextGuardError if text trips an injection pattern."""
    for pattern in _INJECTION_PATTERNS:
        if pattern.search(text):
            INJECTION_REJECTIONS.labels(reason="injection_pattern").inc()
            raise TextGuardError(
                "injection_pattern",
                "Input was rejected as unsafe. Please rephrase your query.",
            )


def guard(text: str, *, max_length: int = 500) -> Tuple[str, None]:
    """Sanitize + injection check in one call.

    Returns (clean_text, None) on success. Raises TextGuardError on any
    failure so FastAPI handlers can convert it to an HTTP 400.
    """
    clean = sanitize(text, max_length=max_length)
    check_injection(clean)
    return clean, None
