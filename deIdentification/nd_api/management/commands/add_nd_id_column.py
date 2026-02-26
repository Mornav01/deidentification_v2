from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, text, MetaData, URL
from sqlalchemy.engine import Engine
from nd_api.models import DbDetailsModel
from urllib.parse import urlparse, parse_qs
from typing import List

import csv

only_tables = ['additionalnotes', 'apptblocks_bkp_111423', 'apptblocks_bkp_11152023', 'assessment_lab_history', 'assessment_notes_history', 'assessment_referral_history', 'assessment_rx_history', 'attachedpastorders', 'backuprform2', 'backuprwform', 'case_details', 'cirriscommunitymapping', 'descofservice', 'ebo_cptlevel_cascodes', 'ecliniforms', 'ecwloguri', 'edi_inspayments', 'edi_inv_cpt', 'edi_invoice', 'edi_invoice_bkp_03292024bf', 'electronichl7content', 'electronichl7content_archive2', 'electronichl7content_archive5', 'electroniclabresults_archive1', 'electroniclabresults_archive3', 'employer', 'enc', 'eptstmtstemp', 'immforecastingcache', 'immunizations', 'items', 'labattachment_archive', 'labdata', 'labdataex', 'laborders', 'laborders_archive2', 'laborders_archive4', 'labordersdetails', 'loincmaster', 'lsm_actions', 'measure_compendium', 'measure_encounterdata_410', 'measure_encounterdata_430', 'measure_encounterdata_440', 'measure_encounterdata_460', 'measure_encounterdata_49020', 'measure_encounterdata_540', 'measure_encounterdata_560', 'measure_reportdata_410', 'measure_reportdata_430', 'measure_reportdata_440', 'measure_reportdata_460', 'measure_reportdata_49020', 'measure_reportdata_540', 'measure_reportdata_560', 'mips_exception_report', 'nhxpatient', 'nhxreferral', 'patientinfo', 'patients', 'patients_cca', 'pmorders', 'psac_break_glass', 'ptspecificalert', 'questionnairedata', 'recelectroniclabresults', 'reconciliation', 'referral', 'regsitry_temp_sort', 'reminderdata', 'tblwebmsg', 'userfavoritetests', 'userprofile', 'users', 'vitals_registry', 'vitalshistory']
failed = []
class Command(BaseCommand):
    help = 'Add nd_auto_increament_id column to all tables in the database'

    def is_mysql(self, engine: Engine) -> bool:
        return engine.name.startswith('mysql')

    def is_postgresql(self, engine: Engine) -> bool:
        return engine.name.startswith('postgresql')

    def get_column_check_query(self, table: str, engine: Engine, config) -> str:
        if self.is_postgresql(engine):
            return f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='{table}' 
                AND column_name='nd_auto_increament_id'
                AND table_schema = current_schema()
            """
        elif engine.dialect.name == 'mssql':
            return f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE TABLE_CATALOG = DB_NAME()
            AND TABLE_SCHEMA = '{config["database"]}'
            AND TABLE_NAME = '{table}'
            AND column_name = 'nd_auto_increament_id'
            """
        else:
            return f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = DATABASE()
                AND table_name='{table}' 
                AND column_name='nd_auto_increament_id'
            """
    def get_update_sequence_query(self, table: str, engine: Engine) -> List[str]:
        if self.is_mysql(engine):
            return [
                "SET @row_number = 0;",
                f"""
                UPDATE {table} 
                SET nd_auto_increament_id = (@row_number:=@row_number + 1);
                """
            ]
        elif self.is_postgresql(engine):
            return [
                f"""
                UPDATE {table}
                SET nd_auto_increament_id = subquery.row_num
                FROM (
                    SELECT ctid, row_number() OVER () as row_num
                    FROM {table}
                ) AS subquery
                WHERE {table}.ctid = subquery.ctid;
                """
            ]
        elif engine.dialect.name == 'mssql':
            return [
                f"""
                WITH cte AS (
                    SELECT nd_auto_increament_id,
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
                    FROM {table}
                )
                UPDATE cte
                SET nd_auto_increament_id = row_num;
                """
            ]
        else:
            raise ValueError("Unsupported database type")

    def parse_connection_string(self, connection_string: str) -> dict:
        try:
            parsed = urlparse(connection_string)
            query_params = parse_qs(parsed.query)
            
            query = {k: v[0] if isinstance(v, list) and len(v) == 1 else v 
                    for k, v in query_params.items()}
            
            drivername = parsed.scheme
            if drivername.startswith('mysql'):
                drivername = 'mysql+pymysql'
            
            userpass = parsed.netloc.split('@')[0].split(':')
            username = userpass[0] if len(userpass) > 0 else None
            password = userpass[1] if len(userpass) > 1 else None
            
            hostport = parsed.netloc.split('@')[-1].split(':')
            host = hostport[0] if len(hostport) > 0 else None
            port = int(hostport[1]) if len(hostport) > 1 else None
            
            return {
                'drivername': drivername,
                'username': username,
                'password': password,
                'host': host,
                'port': port,
                'database': parsed.path.lstrip('/'),
                'query': query
            }
        except Exception as e:
            raise ValueError(f"Invalid connection string format: {str(e)}")

    def create_db_engine(self, connection_string: str) -> Engine:
        config = self.parse_connection_string(connection_string)
        
        if config['drivername'] == 'mysql+pymysql':
            config['query'].update({
                'charset': 'utf8mb4',
                'local_infile': '1'
            })
        engine_url = URL.create(
            drivername=config['drivername'],
            username=config['username'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            database=config['database'],
            query=config['query']
        )
        print("*"*60)
        print(engine_url)
        return create_engine(connection_string), config

    # def get_all_tables(self, engine: Engine) -> List[str]:
    #     if self.is_postgresql(engine):
    #         query = """
    #             SELECT tablename FROM pg_tables 
    #             WHERE schemaname = current_schema()
    #             AND tablename NOT LIKE 'pg_%'
    #             AND tablename NOT LIKE 'sql_%';
    #         """
    #     else:
    #         query = """
    #             SELECT table_name FROM information_schema.tables 
    #             WHERE table_schema = DATABASE()
    #             AND table_name NOT LIKE 'sql_%';
    #         """
        
    #     with engine.connect() as conn:
    #         result = conn.execute(text(query))
    #         return [row[0] for row in result]

    def get_all_tables(self, engine: Engine, config: dict) -> List[str]:
        # from sqlalchemy.engine import reflection
        # inspector = reflection.Inspector.from_engine(engine)
        # return inspector.get_table_names()
        if self.is_postgresql(engine):
            query = """
                SELECT tablename FROM pg_tables 
                WHERE schemaname = current_schema()
                AND tablename NOT LIKE 'pg_%'
                AND tablename NOT LIKE 'sql_%';
            """
        elif engine.dialect.name == 'mssql':  # Check if using MSSQL
            query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_CATALOG = DB_NAME()
            AND TABLE_TYPE = 'BASE TABLE'
            AND TABLE_NAME NOT LIKE 'sql_%';
            """
        else:  # MySQL
            query = """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                AND table_name NOT LIKE 'sql_%';
            """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"schema": config['database']})
            return [row[0] for row in result]

    def add_nd_id_column(self, connection_string: str):
        try:
            engine, config = self.create_db_engine(connection_string)
            tables = self.get_all_tables(engine, config)
            with engine.begin() as conn:
                for table in tables:
                    if table not in only_tables:
                        continue
                    try:
                        column_check = conn.execute(text(self.get_column_check_query(table, engine, config)))
                        if column_check.fetchone() is not None:
                            self.stdout.write(self.style.WARNING(
                                f'Skipping table {table} as nd_auto_increament_id column already exists.'
                            ))
                            continue

                        self.stdout.write(self.style.WARNING(
                            f'Adding `nd_auto_increament_id` column to table {table}.'
                        ))
                        if engine.dialect.name == 'mssql':
                            conn.execute(text(
                                f"ALTER TABLE {table} ADD nd_auto_increament_id INT"
                            ))
                        else:
                            conn.execute(text(
                                f"SET sql_mode = ''"
                            ))
                            conn.execute(text(
                                f"ALTER TABLE {table} ADD COLUMN nd_auto_increament_id INT"
                            ))

                        queries = self.get_update_sequence_query(table, engine)
                        for query in queries:
                            conn.execute(text(query))

                        # if self.is_postgresql(engine):
                        #     conn.execute(text(
                        #         f"ALTER TABLE {table} ALTER COLUMN nd_auto_increament_id SET NOT NULL"
                        #     ))
                        # elif engine.dialect.name == 'mssql':
                        #     conn.execute(text(
                        #         f"ALTER TABLE {table} ALTER COLUMN nd_auto_increament_id INT NOT NULL;"
                        #     ))
                        # else:
                        #     conn.execute(text(
                        #         f"ALTER TABLE {table} MODIFY nd_auto_increament_id INT NOT NULL"
                        #     ))

                        self.stdout.write(self.style.SUCCESS(
                            f'Successfully added and populated nd_auto_increament_id column for table {table}'
                        ))

                    except Exception as e:
                        # self.stdout.write(self.style.ERROR(
                        #     f'Error processing table {table}: {str(e)}'
                        # ))
                        # raise e
                        # breakpoint()
                        failed.append(table)
                

                print(failed)
                self.stdout.write(self.style.ERROR(failed))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Failed to process database: {str(e)}'))
            raise

    def handle(self, *args, **options):
        try:
            connection_string = options['connection_str']
            # db_detail = DbDetailsModel.objects.get(db_name=database_name)
            # connection_string = db_detail.source_db_config["connection_str"]
            self.add_nd_id_column(connection_string)
            self.stdout.write(self.style.SUCCESS('Successfully added nd_auto_increament_id column to all tables.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Command failed: {str(e)}'))
            raise

    def add_arguments(self, parser):
        parser.add_argument('connection_str', type=str, help='connection_str')

# python manage.py add_nd_id_column --connection_str "mysql+pymysql://root:123456789@localhost:3306/nddenttest"
# python manage.py add_nd_id_column "mysql+pymysql://root:123456789@localhost:3306/nddenttest"