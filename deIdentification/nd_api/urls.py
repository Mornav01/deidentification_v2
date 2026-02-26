from django.urls import path

from .views import (
    TablesConfigForUIView,
    DeIdentifyTableView,
    StopDeIdentificationView,
    DownloadConfigAsCSV,
    UpdateTableProgress,
    UpdateDbProgress,
    UploadConfigFromCSV,

    RegisterNewDbView,
    GetAllDbsView,
    GetDbDetailsView,
    TablesForUIView,
    TablesConfigForUIView,
    ViewTableDataView,

    DbStatsView,
    UserPermissions,
    CloudMovement,
    DumpDataView,
    StartDumpView,
    DumpRestoreView,
    StartDumpRestoreView,
    TableQCView,
    TablesQCListView,
    TableQCStatusView,
    QCResultView
)

urlpatterns = [
    path("register_new_db/", RegisterNewDbView.as_view(), name="register_new_db"),
    path("get_all_dbs/", GetAllDbsView.as_view(), name="get_all_dbs"),
    path("get_db_details/<int:db_id>/", GetDbDetailsView.as_view(), name="get_db_details"),
    
    path("get_tables/<int:db_id>/", TablesForUIView.as_view(), name="get_tables"),
    path(
        "tables_details_for_ui/<int:table_id>/",
        TablesConfigForUIView.as_view(),
        name="tables_details_for_ui",
    ),
    path(
        "download_config_as_csv/<int:db_id>/",
        DownloadConfigAsCSV.as_view(),
        name="download_config_as_csv",
    ),
    path(
        "upload_config_from_csv/<int:db_id>/",
        UploadConfigFromCSV.as_view(),
        name="upload_config_from_csv",
    ),
    path("view_table_data/<int:table_id>/", ViewTableDataView.as_view(), name="view_table_data"),

    path(
        "start_de_identification/<int:table_id>/",
        DeIdentifyTableView.as_view(),
        name="start_de_identification",
    ),
    path(
        "stop_de_identification/<int:table_id>/",
        StopDeIdentificationView.as_view(),
        name="stop_de_identification",
    ),

    path(
        "update_table_progress/",
        UpdateTableProgress.as_view(),
        name="update_table_progress",
    ),
    path("update_db_progress/", UpdateDbProgress.as_view(), name="update_db_progress"),

    path("stats_view/<int:db_id>/", DbStatsView.as_view(), name="stats_view"),
    path("user_permissions/", UserPermissions.as_view(), name="user_permissions"),
    path("cloudmove/<int:table_id>/", CloudMovement.as_view(), name="cloudmove"),
    
    path("dump/", DumpDataView.as_view(), name="datadump"),
    path("start_dump_creation/<int:dump_id>/", StartDumpView.as_view(), name="datadump"),
    path("restore_dump/<int:restore_dump_id>/", StartDumpRestoreView.as_view(), name="restore_dump"),
    path("restore_details/", DumpRestoreView.as_view(), name="restore_details"),

    # Add apis for QC
    path("qc/tables/", TablesQCListView.as_view(), name="qc_tables"),
    path("qc/start/", TableQCView.as_view(), name="qc_start"),
    path("qc/<int:table_id>/status/", TableQCStatusView.as_view(), name="qc_status"),
    path("qc/<int:table_id>/result/", QCResultView.as_view(), name="qc_result"),


]
