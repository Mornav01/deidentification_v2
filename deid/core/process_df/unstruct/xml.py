try:
    import regex as re  # type: ignore[no-redef]
except ImportError:
    pass  # stdlib re already available
import sys
import xml.etree.ElementTree as ET
from datetime import datetime as _datetime
from dateutil import parser as date_parser
from deid.core.logger import nd_logger

# optional lxml import for recovery parsing
try:
    from lxml import etree as LET
    HAS_LXML = True
except Exception:
    HAS_LXML = False

# ---------------- regex helpers ----------------
XML_DECLARATION_RE = re.compile(r"(?i)<\?xml[^>]*\?>")
XML_STYLESHEET_RE = re.compile(r"(?i)<\?xml-stylesheet[^>]*\?>")
PI_RE = re.compile(r"(?s)<\?.*?\?>")  # (?s) = DOTALL; RE2 supports inline flag
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
# Lookahead required — RE2 doesn't support it; use `regex` (middle tier).
BARE_AMP_RE = re.compile(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9A-Fa-f]+;)')
_ZIP_5_RE = re.compile(r"\d{5}")
_BR_RE = re.compile(r"(?i)<br\s*>")

# ---------------- cleaning helpers (no @validate_call — called in hot loop) ----------------
def remove_control_chars(text: str) -> str:
    return CONTROL_CHARS_RE.sub("", text)

def remove_processing_instructions(text: str) -> str:
    return PI_RE.sub("", text)

def escape_bare_ampersands(text: str) -> str:
    return BARE_AMP_RE.sub("&amp;", text)

def normalize_br(text: str) -> str:
    return _BR_RE.sub("<br />", text)

def wrap_with_root_if_needed(text: str) -> str:
    s = text.strip()
    if not s:
        return s
    m = re.match(r"\s*<([A-Za-z0-9_:.-]+)(\s|>)", s)
    if not m:
        return f"<root>{s}</root>"
    root_tag = m.group(1)
    close_idx = s.find(f"</{root_tag}>")
    if close_idx == -1:
        return f"<root>{s}</root>"
    after = s[close_idx + len(root_tag) + 3 : ].strip()
    if after:
        return f"<root>{s}</root>"
    return s

# ---------------- robust XML parse ----------------
def try_lxml_recover_parse(text: str):
    if not HAS_LXML:
        return None
    try:
        parser = LET.XMLParser(recover=True, ns_clean=True, huge_tree=True, encoding="utf-8")
        root = LET.fromstring(text.encode("utf-8"), parser=parser)
        return root
    except Exception:
        return None

def try_et_parse(text: str):
    try:
        return ET.fromstring(text)
    except Exception:
        return None

def robust_xml_parse(raw_xml: str):
    """Try multiple repair strategies until we get a parsed XML root."""
    if raw_xml is None or not isinstance(raw_xml, str):
        return None

    s = raw_xml.strip()
    s = XML_DECLARATION_RE.sub("", s)
    s = XML_STYLESHEET_RE.sub("", s)

    # 1: Wrap multi-root fragments FIRST so all top-level elements are preserved.
    # lxml's fromstring(recover=True) silently discards every element after the
    # first top-level tag, so we must never call it on an unwrapped fragment.
    wrapped = wrap_with_root_if_needed(s)

    # 2: Try stdlib ET on the (possibly wrapped) content — no data loss.
    root = try_et_parse(wrapped)
    if root is not None:
        return root

    # 3: Apply cleaning steps progressively, re-try ET after each.
    steps = [
        remove_control_chars,
        remove_processing_instructions,
        escape_bare_ampersands,
        normalize_br,
    ]
    current = wrapped
    for func in steps:
        try:
            current = func(current)
        except Exception:
            continue
        root = try_et_parse(current)
        if root is not None:
            return root

    # 4: ET could not parse the content even after cleaning.
    # This typically means the content is HTML (unquoted attributes, void
    # elements, etc.) rather than XML.  lxml recovery mode can parse it but
    # silently drops sibling elements, causing data loss.  Return None so the
    # caller returns the original text unchanged.
    return None

# ---------------- main deid ----------------
def deidentify_xml_tags(text: str, tag_replacements: dict) -> str:
    """
    De-identify XML string values based on tag names with special handling for DOB and ZIP.
    Uses robust XML cleaning/repair before parsing.
    """
    if not isinstance(text, str):
        return text
    # Use lstrip()[:1] instead of strip().startswith() — avoids full-string scan
    if text.lstrip()[:1] != "<":
        return text

    root = robust_xml_parse(text)
    if root is None:
        nd_logger.debug("[XMLUtils] Could not parse XML after repair attempts.")
        return text

    for tag in list(root.iter()):
        # Skip comment or non-element nodes
        if not isinstance(tag.tag, str):
            continue

        tag_name = tag.tag.split("}")[-1]  # strip namespace
        val = tag.text.strip() if tag.text else ""

        # --- Special Rule: DOB ---
        if tag_name.lower() in ["dob", "dateofbirth", "ptdob"]:
            try:
                parsed = date_parser.parse(val)
                # 2-digit year fix: "1/7/44" → dateutil gives 2044, should be 1944
                if parsed.year > _datetime.now().year:
                    parsed = parsed.replace(year=parsed.year - 100)
                tag.text = str(parsed.year)  # keep only year
            except Exception:
                tag.text = val

        # --- Special Rule: ZIP ---
        elif tag_name.lower() in ["zip", "zipcode", "postalcode"]:
            tag.text = _ZIP_5_RE.sub(lambda m: m.group(0)[:3], val)

        # --- General Replacements ---
        elif tag_name in tag_replacements:
            tag.text = tag_replacements[tag_name]

    # Remove comment elements before serialization
    for elem in list(root):
        if not isinstance(elem.tag, str):
            root.remove(elem)

    # keep namespaces consistent
    ET.register_namespace("SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/")
    ET.register_namespace("xsd", "http://www.w3.org/2001/XMLSchema")
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

    def _remove_recursive_refs(element, seen=None):
        if seen is None:
            seen = set()
        if id(element) in seen:
            return  # prevent infinite recursion
        seen.add(id(element))
        for child in list(element):
            _remove_recursive_refs(child, seen)

    try:
        _remove_recursive_refs(root)
        old_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(max(old_limit, 5000))
        try:
            return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")
        finally:
            sys.setrecursionlimit(old_limit)
    except (TypeError, RecursionError) as e:
        nd_logger.error(f"[XMLUtils] XML serialization failed: {e}")
        return text
