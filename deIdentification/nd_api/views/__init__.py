# from .stats_generation import StatsGenerationView
from .de_identification_task import StopDeIdentificationView, DeIdentifyTableView
from .db_views import RegisterNewDbView, GetDbDetailsView, GetAllDbsView
from .table_details_for_ui import TablesConfigForUIView, TablesForUIView
from .config import DownloadConfigAsCSV, UploadConfigFromCSV
from .table_progress import UpdateTableProgress, UpdateDbProgress
from .view_table_data import ViewTableDataView
from .permission import UserPermissions
from .stats_view import DbStatsView
from .cloudmove import CloudMovement
from .datadump import DumpDataView, StartDumpView, DumpRestoreView, StartDumpRestoreView 
from .qc_view import TableQCView, TablesQCListView, QCResultView, TableQCStatusView
