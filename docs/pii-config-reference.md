# pii_config Reference

`pii_config` is the configuration consumed by the `NOTES` de-identification rule. It tells the pipeline what patient-specific PII values to find and replace inside free-text clinical notes, and defines any additional custom regex or static replacements to apply.

It is distinct from `rules.csv`, which maps columns to rules. `pii_config` only governs what happens *inside* a column that already has the `NOTES` rule applied.

---

## How It Is Generated

`pii_config` is generated automatically by `deid pii-table` based on `pii_tables_config`. It is written to a YAML file (default: `pii_config.yaml` next to `config.yaml`) and loaded at runtime via `pii_config_path` in `config.yaml`.

You can also write `pii_config` by hand or embed it inline in `config.yaml` under the `pii_config` key.

---

## Top-Level Keys

```yaml
mask:      # exact-match string replacements (names, SSN, phone, etc.)
dob:       # date-of-birth → birth year replacement
combine:   # multi-field combinations (first + last name permutations)
regex:     # custom regex patterns (applied after patient-specific steps)
replace_value:  # static string substitutions (applied after regex)
```

All keys are optional. `NOTES` processing runs whichever keys are present.

---

## `mask`

Patient-specific exact-match masking. For each patient, the value of the named column is looked up from the PII table and replaced wherever it appears in that patient's notes (case-insensitive, word-boundary match).

```yaml
mask:
  patients_first_name:
    masking_value: "((FIRST_NAME))"
    min_length: 2
  patients_last_name:
    masking_value: "((LAST_NAME))"
    min_length: 2
  patients_ssn:
    masking_value: "((SSN))"
    min_length: 2
  patients_phone:
    masking_value: "((PHONE))"
    min_length: 2
```

**Key naming convention:** `{source_table}_{column_name}` — matches the column names in the `pii_data_table` (which are prefixed with the source table name when the PII table is created).

| Field | Type | Description |
|-------|------|-------------|
| `masking_value` | string | The replacement string inserted into the note text. |
| `min_length` | integer | Minimum character length a value must have to be included. Prevents short strings (e.g. initials, single letters) from generating too many false-positive matches. Default: `2`. |

**Matching behavior:** For each row, the patient's actual value (e.g. `"John"`) is retrieved from the PII table and compiled into the pattern `(?i)\bJohn\b`. All occurrences in that patient's note text are replaced with `((FIRST_NAME))`.

---

## `dob`

Patient-specific date-of-birth masking. Any date found in the note that matches the patient's DOB is replaced with just the birth year.

```yaml
dob:
  patients_date_of_birth: {}
```

**Key naming convention:** Same as `mask` — `{source_table}_{column_name}`.

The value is an empty dict `{}` (no additional options). The DOB is parsed from the PII table, and any date pattern in the note that `dateutil` can parse to the same date is replaced with the 4-digit birth year (e.g. `1975`).

---

## `combine`

Builds regex patterns from combinations of multiple PII columns — primarily used for full-name matching. For a patient with `first_name="John"` and `last_name="Smith"`, the combine rule generates all permutations: `"john smith"`, `"smith john"`, `"john"`, `"smith"`. All are compiled into a single alternation regex (longest patterns first) and replaced.

This catches full names that neither the individual `first_name` nor `last_name` mask entries would catch on their own (because word-boundary matching on short names can cause false positives).

```yaml
combine:
  patients_full_name:
    combine:
      - patients_first_name
      - patients_last_name
    masking_value: "((PATIENT_NAME))"
```

| Field | Type | Description |
|-------|------|-------------|
| `combine` | list of strings | Column keys (matching names in `pii_data_table`) whose values are combined. |
| `masking_value` | string | The replacement string for any matched combination. |

If a patient has multiple PII records (e.g. multiple insurance records with different name aliases), permutations are generated across all records.

---

## `regex`

Custom regex patterns applied to *all* rows of the notes column, regardless of patient identity. Used for structured PII patterns that are the same for all patients (e.g. custom address formats, provider IDs, facility-specific identifiers).

```yaml
regex:
  mrn_number:
    regex: "MRN[:\\s]*\\d{6,10}"
    masking_value: "((MRN))"
  custom_address:
    regex:
      - "\\d{1,5}\\s+\\w+\\s+(Street|St|Avenue|Ave|Road|Rd|Drive|Dr)\\.?"
      - "P\\.?O\\.?\\s*Box\\s+\\d+"
    masking_value: "((ADDRESS))"
```

| Field | Type | Description |
|-------|------|-------------|
| `regex` | string or list of strings | One or more regex patterns. Each is applied independently. |
| `masking_value` | string | Replacement string for all matches. |

Patterns are applied using Polars' `str.replace_all` (RE2 engine). If a pattern uses features unsupported by RE2 (e.g. lookahead/lookbehind), it falls back to Python's `re` module.

---

## `replace_value`

Static string replacements applied to *all* rows. Used for known exact strings that should always be replaced (e.g. facility names, provider names, department names).

```yaml
replace_value:
  - old_value: "General Hospital"
    new_value: "((FACILITY))"
  - old_value: "Dr. Jane Doe"
    new_value: "((PROVIDER))"
```

Each entry:

| Field | Type | Description |
|-------|------|-------------|
| `old_value` | string | The literal string to find (case-insensitive, word-boundary match). |
| `new_value` | string | The replacement string. |

---

## Processing Order

When `NOTES` is applied to a column, the steps run in this order:

1. **Key PHI** — replace raw patient/encounter/appointment IDs in text with their anonymized counterparts (from mapping table). Runs unconditionally.
2. **XML tag masking** — static replacements for XML-tagged PHI fields (e.g. `<PatientId>`, `<GuarantorName>`).
3. **`mask`** — patient-specific exact-match name/SSN/etc. replacements from PII table.
4. **`dob`** — patient-specific DOB → birth year.
5. **`combine`** — patient-specific full-name permutation matching.
6. **Secondary PII** — same as steps 3–5 but from `secondary_pii_configs` (if configured).
7. **`regex`** — custom regex patterns (all rows).
8. **`replace_value`** — static string replacements (all rows).
9. **Generic rules** — built-in patterns for phone numbers, IP addresses, URLs, driver's licenses, dates (shifted by per-patient offset), and facility locations. These run automatically for all `NOTES` columns regardless of `pii_config`.

Steps 3–6 require the PII table to be populated and `_resolved_patient_id` to be present in the batch DataFrame. If no patient ID is available, those steps are skipped.

---

## Generic Rules (Always Applied)

The following patterns are applied automatically to every `NOTES` column without any configuration needed:

| Type | Replacement |
|------|-------------|
| IPv4 addresses | `((IPADDRESS))` |
| URLs | `((URL))` |
| Phone numbers (with separators) | `((PHONE_NUMBER))` |
| Dates (all common formats) | Shifted by per-patient offset days |
| Driver's license numbers | `((DRIVERSLICENSE))` |
| Facility location names | `((FacilityLocation))` |

---

## Full Example

```yaml
mask:
  patients_first_name:
    masking_value: "((FIRST_NAME))"
    min_length: 2
  patients_last_name:
    masking_value: "((LAST_NAME))"
    min_length: 2
  patients_middle_name:
    masking_value: "((MIDDLE_NAME))"
    min_length: 2
  patients_ssn:
    masking_value: "((SSN))"
    min_length: 4
  patients_phone:
    masking_value: "((PHONE))"
    min_length: 7
  patients_email:
    masking_value: "((EMAIL))"
    min_length: 5

dob:
  patients_date_of_birth: {}

combine:
  patients_full_name:
    combine:
      - patients_first_name
      - patients_last_name
    masking_value: "((PATIENT_NAME))"

regex:
  mrn:
    regex: "(?i)MRN[:\\s#]*\\d{5,10}"
    masking_value: "((MRN))"

replace_value:
  - old_value: "Western General Hospital"
    new_value: "((FACILITY))"
```

---

## Relationship to `pii_tables_config`

`pii_config` and `pii_tables_config` work together:

- **`pii_tables_config`** defines which source tables and columns to extract into the `pii_data_table` (the physical PII lookup table in the PII database).
- **`pii_config`** defines how columns in `pii_data_table` are used for masking during `NOTES` processing.

`deid pii-table` generates both from your source DB schema. The column keys in `pii_config.mask`, `pii_config.dob`, and `pii_config.combine` must match actual column names in `pii_data_table` (using the `{source_table}_{column}` naming convention).
