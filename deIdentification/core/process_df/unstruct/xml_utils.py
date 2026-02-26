xml_tag_replacements = {
    # ── IMPORTANT: DO NOT add EncounterId / PatientId / GuarantorId / ControlNo
    # ──            / reqNo here.  Those ID values are replaced by the ND values
    # ──            from the mapping table in Step 1 (de_identify_key_phi_columns)
    # ──            using a \b{original_value}\b regex.  Adding them here would
    # ──            OVERWRITE the ND ID with a static placeholder, losing the
    # ──            consistent anonymised identifier.
    #
    # This dict is only for TEXT / NAME / non-mapping fields that have no
    # per-row ND value — they get a static placeholder instead.

    # ── Patient name / demographics ──────────────────────────────────────────
    "patient":         "((PATIENT_NAME))",
    "Patient":         "((PATIENT_NAME))",
    "patientName":     "((PATIENT_NAME))",
    "PatientName":     "((PATIENT_NAME))",
    "hl7PtName":       "((PATIENT_NAME))",
    "patientname":     "((PATIENT_NAME))",
    "ptName":          "((PATIENT_NAME))",
    "GuarantorName":   "((GUARANTORNAME))",

    # ── Provider / facility ──────────────────────────────────────────────────
    # ProviderId / NPI are not in the mapping table → static placeholder is fine
    "ProviderId":      "((PROVIDER_ID))",
    "providerId":      "((PROVIDER_ID))",
    "ProviderName":    "((PROVIDER_NAME))",
    "providerName":    "((PROVIDER_NAME))",
    "AttendingName":   "((PROVIDER_NAME))",
    "ReferringName":   "((PROVIDER_NAME))",
    "facName":         "((FACILITYNAME))",
    "NPI":             "((NPI))",

    # ── Contact / insurance ──────────────────────────────────────────────────
    "SSN":             "((SSN))",
    "address":         "((ADDRESS))",
    "Address":         "((ADDRESS))",
    "phone":           "((PHONE_NUMBER))",
    "Phone":           "((PHONE_NUMBER))",
    "phoneNumber":     "((PHONE_NUMBER))",
    "PayorID":         "((PAYORID))",
    "InsuranceId":     "((INSURANCEID))",
    "InsuranceName":   "((INSURANCE_NAME))",
}
