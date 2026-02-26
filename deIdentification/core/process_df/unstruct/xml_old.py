import re
import xml.etree.ElementTree as ET
from dateutil import parser as date_parser
from deIdentification.nd_logger import nd_logger


def deidentify_xml_tags(text: str, tag_replacements: dict) -> str:
    """
    De-identify XML string values based on tag names with special handling for DOB and ZIP.
    Preserves namespace prefixes and XML declaration.
    """
    if not isinstance(text, str) or not text.strip().startswith("<"):  
        return text
    
    try:
        root = ET.fromstring(text)
    except Exception as e:
        nd_logger.debug(f"[XMLUtils] Skipping invalid XML: {e}")
        return text
    
    for tag in root.iter():
        tag_name = tag.tag.split("}")[-1]  # strip namespace if present
        val = tag.text.strip() if tag.text else ""

        #nd_logger.info(f"[XMLUtils] Processing tag <{tag_name}> with value: '{val}'")

        # --- Special Rule: DOB ---
        if tag_name.lower() in ["dob", "dateofbirth", "ptdob"]:
            try:
                parsed = date_parser.parse(val)
                tag.text = str(parsed.year)  # only keep year
                #nd_logger.info(f"[XMLUtils] Masked DOB '{val}' → '{tag.text}'")
            except Exception as e:
                nd_logger.debug(f"[XMLUtils] DOB parse failed: {val} ({e})")
                tag.text = val

        # --- Special Rule: ZIP / ADDRESS ---
        elif tag_name.lower() in ["zip", "zipcode", "postalcode"]:
            def mask_zip(match):
                zip5 = match.group(0)  # full 5-digit match
                masked = zip5[:3]     # keep only first 3 digits
                #nd_logger.info(f"[XMLUtils] Masking ZIP inside '{val}' → '{masked}'")
                return masked
            masked_val = re.sub(r"\d{5}", mask_zip, val)
            if masked_val != val:
                #nd_logger.info(f"[XMLUtils] Updated {tag_name}: '{val}' → '{masked_val}'")
                pass
            tag.text = masked_val

        # --- General Replacements ---
        elif tag_name in tag_replacements:
            #nd_logger.info(f"[XMLUtils] Replacing <{tag_name}> value '{val}' → '{tag_replacements[tag_name]}'")
            tag.text = tag_replacements[tag_name]

    # ✅ Re-register namespaces so prefixes stay the same instead of ns0/ns1
    ET.register_namespace("SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/")
    ET.register_namespace("xsd", "http://www.w3.org/2001/XMLSchema")
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

    # ✅ Return with XML declaration
    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")
