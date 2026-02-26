from typing import TypedDict
from core.dbPkg.dbhandler import NDDBHandler
from threading import Lock

UNIVERSAL_PII_DATA = None
_UNIVERSAL_PII_LOCK = Lock()


class UniversalTableConfig(TypedDict):
    connection_str: str
    table_name: str
    column_mapping: dict


def get_universal_pii_data(tables_config: list[UniversalTableConfig]):
    global UNIVERSAL_PII_DATA
    
    if UNIVERSAL_PII_DATA is None:  
        with _UNIVERSAL_PII_LOCK:
            if UNIVERSAL_PII_DATA is None:
                loader = LoadUniversalTables(tables_config)
                UNIVERSAL_PII_DATA = loader.load()

    return UNIVERSAL_PII_DATA

class LoadUniversalTables:
    
    def __init__(self, tables_config: list[UniversalTableConfig]):
        self.tables_config = tables_config
    
    def _load_table(self, table_config: UniversalTableConfig):
        connection = NDDBHandler(table_config['connection_str'])
        try:
            all_rows = connection.get_all_rows(table_config['table_name'])
            return {"rows": all_rows, "metadata": table_config["column_mapping"]}
        finally:
            connection.close()
    
    def load(self):
        loaded_dict = {}
        for table_config in self.tables_config:
            loaded_dict[table_config["table_name"]] = self._load_table(table_config)
        return loaded_dict

