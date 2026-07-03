from .base import Detector
from .structured import (
    SAppointmentIdDetector,
    SChartIdDetector,
    SDateOffestDetector,
    SDobDetector,
    SEncounterIDDetector,
    SMaskDetector,
    SPatientIdDetector,
    SZipCodeDetector,
    SStaticOffestDetector,
    SReferencePIDDetector,
)
from .unstructured import UnstructuredDetector

DectorMapping = {
    "PATIENT_ID": SPatientIdDetector,
    "ENCOUNTER_ID": SEncounterIDDetector,
    "APPOINTMENT_ID": SAppointmentIdDetector,
    "CHART_ID": SChartIdDetector,
    "PATIENT_DOB": SDobDetector,
    "MASK": SMaskDetector,
    "ZIP_CODE": SZipCodeDetector,
    "DATE_OFFSET": SDateOffestDetector,
    "NOTES": UnstructuredDetector,
    "GENERIC_NOTES": UnstructuredDetector,
    "REFERENCE_PID": SReferencePIDDetector,
    "STATIC_OFFSET": SStaticOffestDetector,
}
