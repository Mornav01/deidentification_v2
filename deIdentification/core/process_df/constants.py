try:
    import re2 as re  # google-re2: RE2 engine, no catastrophic backtracking, Python 3.13 safe
except ImportError:
    import re  # type: ignore[no-redef]

DATE_PATTERN_NOTES = (
    # 1. ISO style: YYYY-MM-DD with optional time and fractional seconds
    r"\b\d{4}-(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[12]\d|3[01])"
    r"(?:\s\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?)?\b|"

    # 2. US style and common separators: MM/DD/YYYY or MM-DD-YYYY or MM.DD.YYYY
    r"\b(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])[/-]\d{4}\b|"
    r"\b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{4}\b|"

    # 3. Two-digit year versions: MM/DD/YY or MM-DD-YY
    r"\b(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])[/-]\d{2}\b|"

    # 4. Day first (DD/MM/YYYY or DD-MM-YYYY)
    r"\b(?:0?[1-9]|[12]\d|3[01])[/-](?:0?[1-9]|1[0-2])[/-]\d{4}\b|"

    # 5. Month name based formats
    r"\b\d{1,2}\s(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|"
    r"May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|"
    r"Oct(?:ober)?|Nov(?:ember)|Dec(?:ember)?)\s\d{4}\b|"

    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|"
    r"May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|"
    r"Oct(?:ober)?|Nov(?:ember)|Dec(?:ember)?)\s\d{1,2}"
    r"(?:st|nd|rd|th)?[,/\s-]?\s?\d{4}\b|"

    r"\b(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s\d{1,2},?\s\d{4}\b|"

    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\s\d{1,2}\.\d{4}\b|"

    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{1,2}-\d{4}\b|"

    # 6. YYYY/MM/DD.HH.MM.SS
    r"\b\d{4}/\d{2}/\d{2}\.\d{2}:\d{2}:\d{2}\b"

    #7. Compact numeric formats — risky, keep at the end
    #r"\b\d{4}\d{2}\d{2}(?!\d)\b|"  # YYYYMMDD
    #r"\b(?:0?[1-9]|[12]\d|3[01])(?:0?[1-9]|1[0-2])\d{4}(?!\d)\b|"  # DDMMYYYY
    #r"\b(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{4}(?!\d)\b"  # MMDDYYYY
)

DATE_PATTERN_GENERAL = (
    # 1. ISO style: YYYY-MM-DD with optional time and fractional seconds
    r"\b\d{4}-(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[12]\d|3[01])"
    r"(?:\s\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?)?\b|"

    # 2. US style and common separators: MM/DD/YYYY or MM-DD-YYYY or MM.DD.YYYY
    r"\b(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])[/-]\d{4}\b|"
    r"\b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{4}\b|"

    # 3. Two-digit year versions: MM/DD/YY or MM-DD-YY
    r"\b(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])[/-]\d{2}\b|"

    # 4. Day first (DD/MM/YYYY or DD-MM-YYYY)
    r"\b(?:0?[1-9]|[12]\d|3[01])[/-](?:0?[1-9]|1[0-2])[/-]\d{4}\b|"

    # 5. Month name based formats
    r"\b\d{1,2}\s(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|"
    r"May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|"
    r"Oct(?:ober)?|Nov(?:ember)|Dec(?:ember)?)\s\d{4}\b|"

    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|"
    r"May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|"
    r"Oct(?:ober)?|Nov(?:ember)|Dec(?:ember)?)\s\d{1,2}"
    r"(?:st|nd|rd|th)?[,/\s-]?\s?\d{4}\b|"

    r"\b(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s\d{1,2},?\s\d{4}\b|"

    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\s\d{1,2}\.\d{4}\b|"

    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{1,2}-\d{4}\b|"

    # 6. YYYY/MM/DD.HH.MM.SS
    r"\b\d{4}/\d{2}/\d{2}\.\d{2}:\d{2}:\d{2}\b|"

    #7. Compact numeric formats — risky, keep at the end
    r"\b(?:0?[1-9]|1[0-2])(?:0?[1-9]|[12]\d|3[01])\d{4}\b|"   # MMDDYYYY (01252024 or 1252024)
    r"\b\d{4}(?:0?[1-9]|1[0-2])(?:0?[1-9]|[12]\d|3[01])\b|"   # YYYYMMDD (20240125 or 2024125)
    r"\b(?:0?[1-9]|[12]\d|3[01])(?:0?[1-9]|1[0-2])\d{4}\b"    # DDMMYYYY (25012024 or 2512024)
)




ZIP_CODE_PATTERNS = {
    "US": re.compile(r'^(\d{3})(\d{2})(?:-\d{4})?$'),  # Matches 12345 and 12345-6789,
    # add more countries her, e.g.
    "CA": re.compile(r'^([A-Z]\d[A-Z]) ?\d[A-Z]\d$')
}


