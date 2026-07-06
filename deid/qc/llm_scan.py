"""Residual-PII scanner for QC notes — pluggable backend (regex | mlx | none).

Replaces the old Presidio dependency in the QC unstructured detector. Backends:

- ``auto`` (default) — use ``mlx`` when running on Apple Silicon with ``mlx-lm`` installed, else
  ``regex``. One shared config then does the right thing across a mixed Apple/Windows fleet.
- ``regex`` (dependency-free, runs anywhere) — pattern-matches phone / email / SSN / URL /
  5-digit ZIP. The portable choice for Windows / Linux / CI workers.
- ``mlx``  (**Apple Silicon only**) — a local LLM via ``mlx-lm`` that reads each note and flags
  residual PHI an entity NER or regex would miss (unknown person names, obfuscated numbers,
  addresses). Better recall on names; needs a Mac + ``pip install -e '.[mlx]'``.
- ``none`` — disable the residual scan (rely on the master exact-match only).

The scanner is master-agnostic: it finds *residual patterns/entities* regardless of the PHI master.
The master exact/partial match (``deid.qc.master_phi``) is the complementary, ground-truth check.

``ResidualPIIScanner`` caches the mlx model process-wide; ``scan(text)`` returns
``[{"type": ..., "text": ...}]``. On any mlx/import/parse failure it degrades to the regex backend
(fail-open to the portable path) and logs — QC must never crash because a model is unavailable.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import platform
import re
from typing import Optional

logger = logging.getLogger("deid.qc.llm_scan")


def _auto_backend() -> str:
    """Resolve ``auto`` → ``mlx`` on Apple Silicon with mlx-lm installed, else ``regex``."""
    if (
        platform.system() == "Darwin"
        and platform.machine() == "arm64"
        and importlib.util.find_spec("mlx_lm") is not None
    ):
        return "mlx"
    return "regex"

DEFAULT_MLX_MODEL = "mlx-community/Llama-3.2-3B-Instruct-4bit"

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

_MLX_PROMPT = (
    "You are a PHI de-identification QA reviewer. The text below is a clinical note that has ALREADY "
    "been de-identified. Identify any REMAINING real PHI that was not removed: person names, phone/fax "
    "numbers, email addresses, street addresses, MRNs or other identifiers, and full calendar dates.\n"
    "Rules:\n"
    "- Placeholder tokens such as <<PATIENT_NAME>>, [NAME], <REDACTED> are CORRECT de-identification; "
    "do NOT flag them.\n"
    "- A year alone (e.g. 2019) is acceptable; do NOT flag it.\n"
    "- Only report text that literally appears in the note.\n\n"
    "Respond ONLY with JSON, no prose:\n"
    '{"phi_found": true, "entities": [{"type": "PERSON", "text": "..."}]}\n\n'
    "Clinical note:\n{text}"
)

# Process-wide cache of loaded mlx (model, tokenizer) tuples, keyed by model name.
_MLX_CACHE: dict = {}


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


def parse_llm_entities(raw: str) -> list[dict]:
    """Parse the LLM's JSON reply into [{'type','text'}]; tolerant of surrounding prose/fences."""
    if not raw:
        return []
    text = raw.strip()
    # Grab the first {...} block if the model wrapped JSON in prose or code fences.
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start:end + 1]
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not data.get("phi_found"):
        return []
    out = []
    for ent in data.get("entities", []) or []:
        if isinstance(ent, dict) and ent.get("text"):
            out.append({"type": str(ent.get("type", "PHI")), "text": str(ent["text"])})
    return out


def _load_mlx(model_name: str):
    if model_name not in _MLX_CACHE:
        from mlx_lm import load  # lazy — only imported on Apple Silicon when backend='mlx'
        logger.info("[QC/mlx] loading model %s ...", model_name)
        _MLX_CACHE[model_name] = load(model_name)
    return _MLX_CACHE[model_name]


def mlx_scan(text: str, model_name: str = DEFAULT_MLX_MODEL, max_tokens: int = 256,
             temperature: float = 0.0) -> list[dict]:
    """Run one note through a local mlx-lm model and parse residual-PHI entities."""
    from mlx_lm import generate
    model, tokenizer = _load_mlx(model_name)
    prompt = tokenizer.apply_chat_template(
        [{"role": "user", "content": _MLX_PROMPT.format(text=text)}],
        add_generation_prompt=True,
    )
    kwargs = {"max_tokens": max_tokens}
    try:  # greedy/low-temp sampling when the helper is available; else default sampler
        from mlx_lm.sample_utils import make_sampler
        kwargs["sampler"] = make_sampler(temp=temperature)
    except Exception:
        pass
    raw = generate(model, tokenizer, prompt=prompt, **kwargs)
    return parse_llm_entities(raw)


class ResidualPIIScanner:
    """Backend-pluggable residual-PII scanner. ``backend`` ∈ {'regex','mlx','none'}."""

    def __init__(self, backend: str = "auto", model: str = DEFAULT_MLX_MODEL,
                 max_tokens: int = 256, temperature: float = 0.0):
        backend = (backend or "auto").lower()
        if backend == "auto":
            backend = _auto_backend()
            logger.info("[QC] residual-PII backend 'auto' resolved to '%s'.", backend)
        self.backend = backend
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self._mlx_failed = False  # once mlx errors, stick to regex for the rest of the run

    def scan(self, text: str) -> list[dict]:
        if not text or self.backend == "none":
            return []
        if self.backend == "mlx" and not self._mlx_failed:
            try:
                return mlx_scan(text, self.model, self.max_tokens, self.temperature)
            except Exception as exc:  # missing mlx / non-Apple / model error → fall back
                self._mlx_failed = True
                logger.warning("[QC] mlx backend unavailable (%s) — falling back to regex.", exc)
        return regex_scan(text)

    @classmethod
    def from_qc_config(cls, qc_config: dict | None) -> "ResidualPIIScanner":
        c = qc_config or {}
        return cls(
            backend=c.get("residual_pii_backend", "auto"),
            model=c.get("mlx_model", DEFAULT_MLX_MODEL),
            max_tokens=int(c.get("mlx_max_tokens", 256)),
            temperature=float(c.get("mlx_temperature", 0.0)),
        )
