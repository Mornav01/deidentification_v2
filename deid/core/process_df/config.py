pii_config = {
    "dob": {
        "PATIENT_DOB": {
            "regex": None,
            "masking_value": "((PATIENT_DOB))",
            "processing_func": None,
        }
    },
    "mask": {
        "PATIENT_ZIP": {
            "regex": None,
            "masking_value": "((ZIP_CODE))",
            "processing_func": None,
        },
        "PATIENT_CITY": {
            "regex": None,
            "masking_value": "((CITY))",
            "processing_func": None,
        },
        "PATIENT_EMAIL": {
            "regex": None,
            "masking_value": "((EMAIL_ID))",
            "processing_func": None,
        },
        "PATIENT_ADDRESS": {
            "regex": None,
            "masking_value": "((PO_BOX_STREET))",
            "processing_func": None,
        },
        "PATIENT_ADDRESS2": {
            "regex": None,
            "masking_value": "((ADDRESS2))",
            "processing_func": None,
        },
        "PATIENT_LASTNAME": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "PATIENT_FIRSTNAME": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "PATIENT_WORKPHONE": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_NAMESUFFIX": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "PATIENT_PATIENTSSN": {
            "regex": None,
            "masking_value": "((SSN))",
            "processing_func": None,
        },
        "PATIENT_CONTEXTNAME": {
            "regex": None,
            "masking_value": "((FacitlyName))",
            "processing_func": None,
        },
        "PATIENT_MOBILEPHONE": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORDOB": {
            "regex": None,
            "masking_value": "((GUARANTORDOB))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORSSN": {
            "regex": None,
            "masking_value": "((SSN))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORZIP": {
            "regex": None,
            "masking_value": "((ZIP_CODE))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORCITY": {
            "regex": None,
            "masking_value": "((CITY))",
            "processing_func": None,
        },
        "PATIENT_MIDDLEINITIAL": {
            "regex": None,
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "PATIENT_TESTPATIENTYN": {
            "regex": None,
            "masking_value": "((TESTPATIENTYN))",
            "processing_func": None,
        },
        "PATIENT_GUARANTOREMAIL": {
            "regex": None,
            "masking_value": "((EMAIL_ID))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORPHONE": {
            "regex": None,
            "masking_value": "((GUARANTORPHONE))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORADDRESS": {
            "regex": None,
            "masking_value": "((PO_BOX_STREET))",
            "processing_func": None,
        },
        "PATIENT_GUARDIANLASTNAME": {
            "regex": None,
            "masking_value": "((GUARDIANLASTNAME))",
            "processing_func": None,
        },
        "PATIENT_PATIENTHOMEPHONE": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_CONTACTPREFERENCE": {
            "regex": None,
            "masking_value": "((CONTACTPREFERENCE))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORADDRESS2": {
            "regex": None,
            "masking_value": "((GUARANTORADDRESS2))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORLASTNAME": {
            "regex": None,
            "masking_value": "((GUARANTORLASTNAME))",
            "processing_func": None,
        },
        "PATIENT_GUARDIANFIRSTNAME": {
            "regex": None,
            "masking_value": "((GUARDIANFIRSTNAME))",
            "processing_func": None,
        },
        "PATIENT_PATIENTEMPLOYERID": {
            "regex": None,
            "masking_value": "((PATIENTEMPLOYERID))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORFIRSTNAME": {
            "regex": None,
            "masking_value": "((GUARANTORFIRSTNAME))",
            "processing_func": None,
        },
        "PATIENT_GUARANTOREMPLOYERID": {
            "regex": None,
            "masking_value": "((EMPLOYERID))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORNAMESUFFIX": {
            "regex": None,
            "masking_value": "((GUARANTORNAMESUFFIX))",
            "processing_func": None,
        },
        "PATIENT_EMERGENCYCONTACTNAME": {
            "regex": None,
            "masking_value": "((EMERGENCYCONTACTNAME))",
            "processing_func": None,
        },
        "PATIENT_EMERGENCYCONTACTPHONE": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_GUARDIANMIDDLEINITIAL": {
            "regex": None,
            "masking_value": "((GUARDIANMIDDLEINITIAL))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORMIDDLEINITIAL": {
            "regex": None,
            "masking_value": "((GUARANTORMIDDLEINITIAL))",
            "processing_func": None,
        },
        "PATIENT_TRANSLATEDHOMEPHONEINDEX": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_TRANSLATEDWORKPHONEINDEX": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_TRANSLATEDMOBILEPHONEINDEX": {
            "regex": None,
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
    },
    "regex": {
        "PATIENT_EMAIL": {
            "regex": ["\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b"],
            "masking_value": "((EMAIL_ID))",
            "processing_func": None,
        },
        "PATIENT_WORKPHONE": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_PATIENTSSN": {
            "regex": ["\\b\\d{3}[- ]\\d{2}[- ]\\d{4}\\b"],
            "masking_value": "((SSN))",
            "processing_func": None,
        },
        "PATIENT_MOBILEPHONE": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORSSN": {
            "regex": ["\\b\\d{3}[- ]\\d{2}[- ]\\d{4}\\b"],
            "masking_value": "((SSN))",
            "processing_func": None,
        },
        "PATIENT_GUARANTOREMAIL": {
            "regex": ["\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b"],
            "masking_value": "((EMAIL_ID))",
            "processing_func": None,
        },
        "PATIENT_GUARANTORPHONE": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_PATIENTHOMEPHONE": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_TRANSLATEDHOMEPHONEINDEX": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_TRANSLATEDWORKPHONEINDEX": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
        "PATIENT_TRANSLATEDMOBILEPHONEINDEX": {
            "regex": [
                "\\(\\d{3}\\)\\s*\\d{3}-\\d{4}",
                "\\b\\d{3}-\\d{3}-\\d{4}\\b",
                "\\+\\d{1,3}\\s\\d{1,4}[- ]\\d{3}[- ]\\d{4}\\b",
                "\\b\\d{3} \\d{3} \\d{4}\\b",
            ],
            "masking_value": "((PHONE_NUMBER))",
            "processing_func": None,
        },
    },
    "combine": {
        "patientfullname": {
            "regex": None,
            "combine": [
                "PATIENT_FIRSTNAME",
                "PATIENT_MIDDLEINITIAL",
                "PATIENT_GUARANTORLASTNAME",
            ],
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
        "guarantorfullname": {
            "regex": None,
            "combine": [
                "PATIENT_GUARANTORFIRSTNAME",
                "PATIENT_GUARANTORMIDDLEINITIAL",
                "PATIENT_LASTNAME",
            ],
            "masking_value": "((PATIENT_NAME))",
            "processing_func": None,
        },
    },
    "replace_value": [
        {
            "new_value": "((FACILITYNAME))",
            "old_value": "MD - THE NEUROLOGY CENTER, P.A.",
        },
        {"new_value": "((FACILITYNAME))", "old_value": "THE NEUROLOGY CENTER P.A."},
        {"new_value": "((FACILITYNAME))", "old_value": "THE NEUROLOGY CENTER, P.A."},
    ],
}


secondary_pii = [
    {   "table_name" : "",
        "config" : {}
        },
        
    {   "table_name" : "",
        "config" : {}
        }
]