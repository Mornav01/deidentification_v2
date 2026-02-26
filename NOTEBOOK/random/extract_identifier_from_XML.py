import re
import logging
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine, text

# optional lxml import (if installed it improves recovery)
try:
    from lxml import etree as LET
    HAS_LXML = True
except Exception:
    HAS_LXML = False

# ---------------- logging ----------------
logging.basicConfig(
    level=logging.INFO,  # set to DEBUG for more verboseness
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("XMLRepairExtractor")

# ---------------- regex helpers ----------------
CDATA_WRAP_RE = re.compile(r"<!\[CDATA\[(.*)\]\]>", re.DOTALL)
XML_DECLARATION_RE = re.compile(r"<\?xml[^>]*\?>", re.IGNORECASE)
PI_RE = re.compile(r"<\?.*?\?>", re.DOTALL)  # generic processing instruction removal
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
BARE_AMP_RE = re.compile(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9A-Fa-f]+;)')
HREF_VALUE_RE = re.compile(r'href\s*=\s*"([^"]*)"', re.IGNORECASE)

# regex fallback patterns:
TAG_ID_RE = re.compile(r'<\s*(EncounterId|PatientId)\s*>\s*(.*?)\s*<\s*/\s*\1\s*>', re.IGNORECASE | re.DOTALL)
ATTR_ID_RE = re.compile(r'(?i)\b(EncounterId|PatientId)\s*=\s*["\'](.*?)["\']')

# helper to get localname
def local_name(tag):
    if not isinstance(tag, str):
        return ""
    return tag.split("}")[-1].lower()

# ---------------- cleaning functions ----------------
def unwrap_cdata(text: str) -> str:
    if not text or not isinstance(text, str):
        return ""
    m = CDATA_WRAP_RE.search(text)
    if m:
        return m.group(1).strip()
    return text

def remove_control_chars(text: str) -> str:
    return CONTROL_CHARS_RE.sub("", text)

def remove_processing_instructions(text: str) -> str:
    return PI_RE.sub("", text)

def escape_bare_ampersands(text: str) -> str:
    return BARE_AMP_RE.sub("&amp;", text)

def percent_encode_spaces_in_href(text: str) -> str:
    def repl(m):
        val = m.group(1)
        if "%20" in val:
            safe = val
        else:
            safe = val.replace(" ", "%20")
        return f'href="{safe}"'
    return HREF_VALUE_RE.sub(repl, text)

def normalize_br(text: str) -> str:
    return re.sub(r"<br\s*>", "<br />", text, flags=re.IGNORECASE)

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

# ---------------- parsing & extraction ----------------
def try_lxml_recover_parse(text: str):
    if not HAS_LXML:
        return None, "lxml not installed"
    try:
        parser = LET.XMLParser(recover=True, ns_clean=True, huge_tree=True, encoding="utf-8")
        root = LET.fromstring(text.encode("utf-8"), parser=parser)
        return root, None
    except Exception as e:
        return None, str(e)

def try_et_parse(text: str):
    try:
        root = ET.fromstring(text)
        return root, None
    except ET.ParseError as e:
        return None, str(e)
    except Exception as e:
        return None, str(e)

def extract_ids_from_element_tree(root):
    encounter_id = None
    patient_id = None
    for elem in root.iter():
        name = local_name(elem.tag)
        if name == "encounterid" and (elem.text is not None):
            if not encounter_id:
                encounter_id = elem.text.strip()
        elif name == "patientid" and (elem.text is not None):
            if not patient_id:
                patient_id = elem.text.strip()
        if hasattr(elem, "attrib"):
            for k, v in elem.attrib.items():
                if not v:
                    continue
                key = k.lower()
                if key == "encounterid" and not encounter_id:
                    encounter_id = v.strip()
                elif key == "patientid" and not patient_id:
                    patient_id = v.strip()
                elif key == "id":
                    if "encounter" in local_name(elem.tag) and not encounter_id:
                        encounter_id = v.strip()
                    if "patient" in local_name(elem.tag) and not patient_id:
                        patient_id = v.strip()
    return encounter_id, patient_id

def regex_fallback_extract(text: str):
    encounter_id = None
    patient_id = None
    for m in TAG_ID_RE.findall(text):
        tagname = m[0].lower()
        val = m[1].strip()
        if tagname == "encounterid" and not encounter_id:
            encounter_id = val
        if tagname == "patientid" and not patient_id:
            patient_id = val
    for m in ATTR_ID_RE.findall(text):
        tagname = m[0].lower()
        val = m[1].strip()
        if tagname == "encounterid" and not encounter_id:
            encounter_id = val
        if tagname == "patientid" and not patient_id:
            patient_id = val
    return encounter_id, patient_id

def repair_and_extract_ids(raw_xml: str, row_id=None):
    if raw_xml is None:
        return None, None
    if not isinstance(raw_xml, str):
        try:
            raw_xml = str(raw_xml)
        except Exception:
            return None, None
    original = raw_xml
    s = unwrap_cdata(original)

    if HAS_LXML:
        root, _ = try_lxml_recover_parse(s)
        if root is not None:
            try:
                return extract_ids_from_element_tree(root)
            except Exception:
                try:
                    text_back = LET.tostring(root, encoding="utf-8").decode("utf-8")
                    et_root, _ = try_et_parse(text_back)
                    if et_root is not None:
                        return extract_ids_from_element_tree(et_root)
                except Exception:
                    pass

    root, _ = try_et_parse(s)
    if root is not None:
        return extract_ids_from_element_tree(root)

    steps = [
        remove_control_chars,
        remove_processing_instructions,
        escape_bare_ampersands,
        percent_encode_spaces_in_href,
        normalize_br,
    ]

    current = s
    for func in steps:
        try:
            current = func(current)
        except Exception:
            continue
        root, _ = try_et_parse(current)
        if root is not None:
            return extract_ids_from_element_tree(root)

    wrapped = wrap_with_root_if_needed(current)
    if wrapped != current:
        root, _ = try_et_parse(wrapped)
        if root is not None:
            return extract_ids_from_element_tree(root)

    eid, pid = regex_fallback_extract(current)
    if eid or pid:
        return eid, pid

    eid, pid = regex_fallback_extract(original)
    if eid or pid:
        return eid, pid

    if HAS_LXML:
        try:
            wrapped_bytes = ("<root>" + current + "</root>").encode("utf-8")
            parser = LET.XMLParser(recover=True, ns_clean=True, huge_tree=True, encoding="utf-8")
            root = LET.fromstring(wrapped_bytes, parser=parser)
            as_text = LET.tostring(root, encoding="utf-8").decode("utf-8")
            et_root, _ = try_et_parse(as_text)
            if et_root is not None:
                return extract_ids_from_element_tree(et_root)
        except Exception:
            pass

    return None, None

# ---------------- main ETL ----------------
def process_data(conn_str: str, primary_col: str = "progressnotes", fallback_col: str = "ccr"):
    engine = create_engine(conn_str)
    query = "SELECT * FROM emrereferralattachments"
    logger.info("Reading source table...")
    df = pd.read_sql(query, engine)
    logger.info(f"Fetched {len(df)} rows.")

    encounter_ids, patient_ids = [], []
    for idx, row in df.iterrows():
        rid = row.get("id", idx)

        # Try primary column first
        raw_xml = row.get(primary_col)
        eid, pid = repair_and_extract_ids(raw_xml, row_id=rid)

        # If nothing found, try fallback column
        if (eid is None and pid is None) and fallback_col in row:
            raw_xml = row.get(fallback_col)
            eid, pid = repair_and_extract_ids(raw_xml, row_id=rid)

        encounter_ids.append(eid)
        patient_ids.append(pid)

    df = df.copy()
    df["encounterid"] = encounter_ids
    df["patientid"] = patient_ids

    logger.info("Dropping existing target (if any) and writing results...")
    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('emrereferralattachments_extracted', 'U') IS NOT NULL
                DROP TABLE emrereferralattachments_extracted;
        """))

    df.to_sql("emrereferralattachments_extracted", engine, if_exists="replace", index=False)
    logger.info("Wrote emrereferralattachments_extracted (all columns + extracted columns).")

if __name__ == "__main__":
    conn_string = "mssql+pymssql://sa:ndADMIN2025@localhost:1433/mobiledoc"
    process_data(conn_string, primary_col="progressnotes", fallback_col="ccr")
