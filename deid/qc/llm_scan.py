"""Residual-PII scanner for QC notes — pluggable backend (regex | none).

Replaces the old Presidio dependency in the QC unstructured detector. Backends:

- ``regex`` (default, dependency-free, runs anywhere) — pattern-matches phone / email / SSN /
  URL / 5-digit ZIP. The portable choice for Windows / Linux / macOS / CI workers.
- ``none`` — disable the residual scan (rely on the master exact-match only).

The scanner is master-agnostic: it finds *residual patterns* regardless of the PHI master.
The master exact/partial match (``deid.qc.master_phi``) is the complementary, ground-truth check.

``scan(text)`` returns ``[{"type": ..., "text": ...}]``.
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger("deid.qc.llm_scan")

# Regex backend patterns (kept minimal + high-precision to limit false positives).
_PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_URL_RE = re.compile(r"https?://\S+|\bwww\.\S+", re.IGNORECASE)
_ZIP5_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")

_REGEX_ENTITIES = (
    ("PHONE_NUMBER", _PHONE_RE),
    ("EMAIL_ADDRESS", _EMAIL_RE),
    ("US_SSN", _SSN_RE),
    ("URL", _URL_RE),
    ("ZIP_CODE", _ZIP5_RE),
)


def regex_scan(text: str) -> list[dict]:
    """Dependency-free residual-PII scan. Returns [{'type', 'text'}]."""
    hits: list[dict] = []
    seen = set()
    for etype, rx in _REGEX_ENTITIES:
        for m in rx.finditer(text or ""):
            frag = m.group(0)
            key = (etype, frag)
            if key not in seen:
                seen.add(key)
                hits.append({"type": etype, "text": frag})
    return hits


class ResidualPIIScanner:
    """Backend-pluggable residual-PII scanner. ``backend`` ∈ {'regex', 'none'}."""

    def __init__(self, backend: str = "regex"):
        backend = (backend or "regex").lower()
        if backend not in ("regex", "none"):
            logger.warning("[QC] residual-PII backend %r not supported — using 'regex'.", backend)
            backend = "regex"
        self.backend = backend

    def scan(self, text: str) -> list[dict]:
        if not text or self.backend == "none":
            return []
        return regex_scan(text)

    @classmethod
    def from_qc_config(cls, qc_config: dict | None) -> "ResidualPIIScanner":
        c = qc_config or {}
        return cls(backend=c.get("residual_pii_backend", "regex"))
