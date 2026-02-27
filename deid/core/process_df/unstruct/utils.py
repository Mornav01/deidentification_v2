import re          # standard lib – re.Match type hint + fallback
try:
    import re2
except ImportError:
    import re as re2  # type: ignore[no-redef]



# Must start with (?x) at position 0 (no leading newline) for Python 3.13 re.compile
ADDRESS_REGEX = (
    r"(?x)"
    r"(?P<number>\d+[A-Za-z]{0,3})     # House number "
    r"[\s,]*"
    r"(?P<street>[A-Za-z0-9\s.,'-]+)   # Street name "
    r"[,\s]+"
    r"(?P<city>[A-Za-z\s]+?)"
    r"[,\s]+"
    r"(?P<state>AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)"
    r"[\s,]*"
    r"(?P<zip>\d{5}(?:-\d{4})?)"
)
r'''
ADDRESS_REGEX = [
    r"""
    (?P<number>\d+[A-Za-z]{0,3})[\s,]*                          
    (?P<street>[A-Za-z0-9\s.,'-]+)[,\s]+
    (?P<city>[A-Za-z\s]+?)[,\s]+
    (?P<state>AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|
            MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|
            SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)[\s,]*
    (?P<zip>\d{5}(?:-\d{4})?)
    """,

    r"""
    (?P<number>\d+[A-Za-z]{0,3})[\s,]*                          
    (?P<street>[A-Za-z0-9\s.,'-]+)[,\s]+
    (?P<city>[A-Za-z\s]+?)[,\s]+
    (?P<state>AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|
            MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|
            SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)
    """,

    r"""
    (?P<number>\d+[A-Za-z]{0,3})[\s,]+
    (?P<street>[A-Za-z0-9\s.,'-]+(?:Street|St|Avenue|Ave|Road|Rd|Lane|Ln|Blvd|Drive|Dr|Way|Court|Ct)?)
    """,

    r"""
    (?P<number>\d+[A-Za-z]{0,3})[\s,]+
    (?P<street>[A-Za-z0-9\s.,'-]+(?:Street|St|Avenue|Ave|Road|Rd|Lane|Ln|Blvd|Drive|Dr|Way|Court|Ct)?)
    [\s,]*(?:Apt|Suite|Unit|\#)\s*\d+[A-Za-z]*
    """,

    r"""
    (?P<city>[A-Za-z\s]+)[,\s]+
    (?P<state>AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|
            MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|
            SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)
    """
]
r'''
DRIVER_LICENSE_PATTERNS = [
    # Context-based matches (e.g., "DL:", "License No:", etc.)
    r"(?i)\b(?:DL|DL#|DL No\.?|License No\.?|Driver'?s License)\s*[:#]?\s*[A-Z]?\d{5,13}\b",

    # State-specific patterns (approximate formats)
    r"\b[A-Z]\d{7}\b",              # California (e.g., A1234567)
    #r"\b\d{8}\b",                   # Texas (e.g., 12345678)
    #r"\b\d{9}\b",                   # New York (alt format, e.g., 123456789)
    r"\b[A-Z]\d{11}\b",             # Illinois (e.g., A12345678901)
    r"\b[A-Z]\d{12}\b",             # Florida (e.g., F123456789012)
    #r"\b\d{7,9}\b",                 # General fallback (safe range)

    # Optional: compound alphanumeric (some states use longer strings)
    r"\b[A-Z]{1,2}\d{6,12}[A-Z]?\b"
]


GENERIC_REGEX_DICT = {
    "ip": {
        "masking_value": "((IPADDRESS))",
        "regex": [
            r"\b((25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)\b"  # IPv4 pattern
        ],
        "processing_func": None,
    },
    "url": {
        "masking_value": "((URL))",
        "regex": [
            r'\b(?:https?:/+[\w-]+\.[^\s<>"\']+|w{3,4}\.?\S+)'
        ],
        "processing_func": None,
    },
    "phone_number": {
        "masking_value": "((PHONE_NUMBER))",
        "regex": [
            # Requires at least one separator (space / dash / dot) between EVERY
            # pair of digit groups so that plain numeric IDs (encounter IDs,
            # patient IDs) are never matched.
            #
            # Matches  : (123) 456-7890  123-456-7890  123.456.7890  123 456 7890
            #            +1-123-456-7890  +1 (123) 456-7890  123-4567 (local 7-digit)
            # Rejects  : 1234567  1234567890  any run of digits without separators
            #
            # Pattern breakdown:
            #   (?:\+?1[\s.-]?)?          – optional country code  (+1, 1-, 1 )
            #   (?:\(\d{3}\)[\s.-]        – area code in parens: (123) or (123)-
            #    |\d{3}[\s.-])            – OR plain 3-digit area code WITH separator
            #   \d{3}[\s.-]\d{4}         – middle-3 + last-4 WITH separator between them
            r"(?:\+?1[\s.-]?)?(?:\(\d{3}\)[\s.-]|\d{3}[\s.-])\d{3}[\s.-]\d{4}\b"
        ],
        "processing_func": None,
    },
    "date": {
        "masking_value": None,
        "regex": r"""
    (?:
        # ISO Format (YYYY-MM-DD)
        \b\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])(?!\w)|

        # ISO-like Format (YYYY/MM/DD)
        \b\d{4}/(?:0[1-9]|1[0-2])/(?:0[1-9]|[12]\d|3[01])(?!\w)|

        # Common American Format (MM/DD/YYYY or MM-DD-YYYY)
        \b(?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12]\d|3[01])[/-]\d{4}(?!\w)|

        # European Format (DD/MM/YYYY or DD-MM-YYYY)
        \b(?:0[1-9]|[12]\d|3[01])[/-](?:0[1-9]|1[0-2])[/-]\d{4}(?!\w)|

        # Short Year Formats (MM/DD/YY or DD/MM/YY)
        \b(?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12]\d|3[01])[/-]\d{2}(?!\w)|
        \b(?:0[1-9]|[12]\d|3[01])[/-](?:0[1-9]|1[0-2])[/-]\d{2}(?!\w)|

        # Textual Formats with Full Year
        # DD Month YYYY
        \b(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?\s+
        (?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        (?:,?\s+)\d{4}(?!\w)|

        # Month DD, YYYY
        \b(?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        \s+(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?(?:,?\s+)\d{4}(?!\w)|

        # Month YYYY
        \b(?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        \s+\d{4}(?!\w)|

        # YYYY Month
        \b\d{4}\s+
        (?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)(?!\w)|

        # Abbreviated Formats (without year)
        # Month DD
        \b(?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)
        \s+(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?(?!\w)|

        # MM DD YYYY
        \b(?:0?[1-9]|1[0-2])/(?:0?[1-9]|[12]\d|3[01])/\d{4}(?!\w)|
        
        # MM-DD-YYYY
        \b(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[12]\d|3[01])-\d{4}(?!\w)|

        # MM.DD.YYYY
        \b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{4}(?!\w)|

        # DD.MM.YYYY
        \b(?:0?[1-9]|[12]\d|3[01])\.(?:0?[1-9]|1[0-2])\.\d{4}(?!\w) |
        
        # YYYY.MM.DD
        \b\d{4}\.(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])(?!\w) |
        

        # MM-DD-YYYY
        \b(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])-\d{4}(?!\w) |

        # Short Format (M/D/YY or MM/DD/YY)
        \b(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])[/-]\d{2}(?!\w)|

        # "%m.%d.%y"
        \b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{2}(?!\w) |

        # DD Month
        \b(?:0[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?\s+
        (?:January|February|March|April|May|June|July|August|September|October|November|December|
        Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)(?!\w)

    )
""",
        "processing_func": None
    },
    "driver_license": {
        "masking_value": "((DRIVERSLICENSE))",
        "regex": DRIVER_LICENSE_PATTERNS,
        "processing_func": None,
    },
    "address": {
        "masking_value": "((Address))",
        # Temporarily disabled generic address regex because it was too aggressive
        # and caused false positives (e.g., masking non-address text like filenames).
        # Structured PII configs (pii_config['regex']) still handle address masking.
        "regex": [],
        "processing_func": None,
    },
    "facility_location": {
        "masking_value": "((FacilityLocation))",
        "regex": [
            r"\b(?:Amherst|Buffalo|gatescircle|gates circle|OrchardPark|Orchard Park|Batavia)\b"
        ],
        "processing_func": None,
    }
}



GENERIC_DATE_REGEX = {
    "masking_value": None,
    "regex": r"""
    (?:
        # Textual Formats (Require a Year) - Put these first 

        # 1. DD Month YYYY with optional space (e.g., "17 July 2023" or "17July2023")
        \b(?P<day1>\d{1,2})(?:st|nd|rd|th)?\s*
        (?P<month1>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        (?:,\s*|\s*)(?P<year1>\d{4})(?!\w)|

        # 2. Month DD, YYYY or Month DD YYYY with optional space (e.g., "July17, 2023" or "July17 2023")
        \b(?P<month2>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<day2>\d{1,2})(?:st|nd|rd|th)?
        (?:,\s*|\s*)(?P<year2>\d{4})(?!\w)|

        # 3. Month DD YYYY with optional space (e.g., "Mar15 2023" or "Mar 15 2023")
        \b(?P<month3>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<day3>\d{1,2})(?:st|nd|rd|th)?
        \s*(?P<year3>\d{4})(?!\w)|

        # 4. Month YYYY with optional space (e.g., "March2024", "Jan 2015")
        \b(?P<month4>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<year4>\d{4})(?!\w)|

        # 5. Year Month with optional space (e.g., "2024June" or "2024, June")
        \b(?P<year5>\d{4})\s*(?:,\s*)?
        (?P<month5>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        (?!\w)|

        # 6. Month@Year (e.g., "June@2013")
        \b(?P<month6>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        @(?P<year6>\d{4})(?!\w)|

        # m.d.y
        \b(?:0?[1-9]|1[0-2])\.(?:0?[1-9]|[12]\d|3[01])\.\d{2}(?!\w) |

        # Numeric Formats (Require Year - 2 or 4 digits)
        \b(?P<day4>(?:0?[1-9]|1[0-2]))/(?P<month7>(?:0?[1-9]|[12]\d|3[0-1]))/(?P<year7>(?:\d{2}|\d{4}))(?!\w)|
        \b(?P<day5>(?:0?[1-9]|[12]\d|3[0-1]))/(?P<month8>(?:0?[1-9]|1[0-2]))/(?P<year8>(?:\d{2}|\d{4}))(?!\w)|
        \b(?P<day6>(?:0?[1-9]|[12]\d|3[0-1]))-(?P<month9>(?:0?[1-9]|1[0-2]))-(?P<year9>(?:\d{2}|\d{4}))(?!\w)|
        \b(?P<day7>(?:0?[1-9]|[12]\d|3[0-1]))-(?P<month10>(?:0?[1-9]|1[0-2]))-(?P<year10>(?:\d{2}|\d{4}))(?!\w)|

        # Textual Formats (No Year) - Put these last 

        # Month DD with optional space (e.g., "Sept23" or "Sept 23")
        \b(?P<month11>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))
        \s*(?P<day8>\d{1,2})(?:st|nd|rd|th)?\b|

        # DD Month with optional space (e.g., "23Sept" or "23 Sept")
        \b(?P<day9>\d{1,2})(?:st|nd|rd|th)?\s*
        (?P<month12>(?:January|February|March|April|May|June|July|August|September|October|November|December|
            Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec))\b
    )
    """,
    "processing_func": None,
}