import traceback
from rest_framework.views import APIView
from django.http import HttpResponse
from nd_api.models import DbDetailsModel
from nd_api.schemas.table_config import TableDetailsForUI
from nd_api.models import TableDetailsModel
import csv
import io
from datetime import datetime
from rest_framework.response import Response
from rest_framework import status
from deIdentification.nd_logger import nd_logger


CSV_HEADERS = [
    "TABLE_NAME",
    "COLUMN_NAME",
    "IS_PHI",
    "DE_IDENTIFICATION_RULE",
    "MASK_VALUE",
    "REFERENCE_PATIENT_ID",
    "REFERENCE_ENCOUNTER_ID",
]


class DownloadConfigAsCSV(APIView):
    # authentication_classes = []

    def get(self, request, db_id: int):
        try:
            db_details = DbDetailsModel.objects.get(id=db_id)
            all_tables = db_details.tables_details.all()

            # Create CSV buffer
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(CSV_HEADERS)

            for table in all_tables:
                table_details_for_ui: TableDetailsForUI = table.table_details_for_ui

                # Get reference columns
                ref_patient_id = table_details_for_ui.get(
                    "reference_patient_id_column", None
                )
                ref_encounter_id = table_details_for_ui.get("reference_enc_id_column", None)

                # Write each column's configuration
                for column in table_details_for_ui.get("columns_details", []):
                    writer.writerow(
                        [
                            table.table_name,
                            column["column_name"],
                            column.get("is_phi", False),
                            column.get("de_identification_rule", None),
                            column.get("mask_value", None),
                            ref_patient_id,
                            ref_encounter_id,
                        ]
                    )

            # Prepare response
            output.seek(0)
            response = HttpResponse(output.getvalue(), content_type="text/csv")
            response["Content-Disposition"] = (
                f'attachment; filename="table_configuration_db_{db_id}_{datetime.now().strftime("%d_%m_%Y")}.csv"'
            )

            return response
        except Exception as e:
            message = f"ViewTableDataView.get: Internal server error : {e}, for user: {request.user}, db_id: {db_id}"
            nd_logger.info(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class UploadConfigFromCSV(APIView):
    # authentication_classes = []
    
    def post(self, request, db_id: int):
        try:
            db_details = DbDetailsModel.objects.get(id=db_id)
            if db_details.is_phi_marking_locked:
                return Response(
                    {
                        "success": False,
                        "message": "Failed in uploading csv, Phi Marking locked for this DB",
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )
            uploaded_file = request.FILES.get("file")
            if not uploaded_file:
                return Response(
                    {"success": False, "message": "No file provided."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            file_data = uploaded_file.read().decode("utf-8")
            csv_reader = csv.DictReader(io.StringIO(file_data))

            if csv_reader.fieldnames != CSV_HEADERS:
                return Response(
                    {"success": False, "message": "Invalid CSV headers."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Group rows by table name
            table_configs = {}
            for row in csv_reader:
                table_name = row.get(
                    "TABLE_NAME", ""
                ).strip()  # Strip whitespace from table name
                if not table_name:  # Skip empty table names
                    continue
                if table_name not in table_configs:
                    table_configs[table_name] = {
                        "columns": [],
                        "ref_patient_id": row.get("REFERENCE_PATIENT_ID"),
                        "ref_encounter_id": row.get("REFERENCE_ENCOUNTER_ID"),
                    }

                column_name = row.get("COLUMN_NAME")
                existing_column = next(
                    (
                        col
                        for col in table_configs[table_name]["columns"]
                        if col["column_name"] == column_name
                    ),
                    None,
                )
                if existing_column:
                    # Update existing column values
                    existing_column["is_phi"] = row.get("IS_PHI", "False").lower() in [
                        "true",
                        "1",
                        "yes"
                    ]
                    existing_column["de_identification_rule"] = row.get(
                        "DE_IDENTIFICATION_RULE"
                    )
                    existing_column["mask_value"] = row.get("MASK_VALUE")
                else:
                    # Add new column if it doesn't exist
                    table_configs[table_name]["columns"].append(
                        {
                            "column_name": column_name,
                            "is_phi": row.get("IS_PHI", "False").lower()
                            in ["true", "1", "yes"],
                            "de_identification_rule": row.get("DE_IDENTIFICATION_RULE"),
                            "mask_value": row.get("MASK_VALUE"),
                        }
                    )

            # Process each table's configuration
            for table_name, config in table_configs.items():
                # Fetch the table details
                table: TableDetailsModel = db_details.tables_details.filter(table_name=table_name).first()
                if not table:
                    message = f"Table '{table_name}' not found in database."
                    nd_logger.error(message)
                    return Response(
                        {"success": False, "message": message},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                if table.is_phi_marking_locked:
                    nd_logger.info(f"phi-makring is locked for table {table.table_name}, not updating config, {request.user}")
                table_details_for_ui = table.table_details_for_ui
                columns_details = table_details_for_ui.get("columns_details", [])

                # Update all columns for this table
                for new_column in config["columns"]:
                    column = next(
                        (
                            col
                            for col in columns_details
                            if col["column_name"].lower() == new_column["column_name"].lower()
                        ),
                        None,
                    )
                    if not column:
                        message = f"Column '{new_column['column_name']}' not found in table '{table_name}'."
                        nd_logger.error(message)
                        return Response(
                            {
                                "success": False,
                                "message": message,
                            },
                            status=status.HTTP_400_BAD_REQUEST,
                        )

                    column["is_phi"] = new_column["is_phi"]
                    column["de_identification_rule"] = new_column[
                        "de_identification_rule"
                    ]
                    column["mask_value"] = new_column["mask_value"]

                # Update reference columns if provided
                if config["ref_patient_id"]:
                    table_details_for_ui["reference_patient_id_column"] = config[
                        "ref_patient_id"
                    ]
                if config["ref_encounter_id"]:
                    table_details_for_ui["reference_enc_id_column"] = config[
                        "ref_encounter_id"
                    ]

                # Save changes to the table
                table.table_details_for_ui = table_details_for_ui
                table.save()
            nd_logger.info("Configuration uploaded successfully.")
            return Response(
                {"success": True, "message": "Configuration uploaded successfully."},
                status=status.HTTP_200_OK,
            )

        except DbDetailsModel.DoesNotExist:
            return Response(
                {"success": True, "message": "Database details not found."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        except Exception as e:
            nd_logger.error(traceback.format_exc())
            return Response(
                {"success": False, "message": f"{e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
