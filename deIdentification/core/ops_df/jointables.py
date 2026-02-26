import pandas as pd
from pandas import DataFrame
from sqlalchemy import Table, select, MetaData
from core.dbPkg import NDDBHandler
from deIdentification.nd_logger import nd_logger

class ReferenceMappingDataFrameJoiner:
    def __init__(self, sourcedb: NDDBHandler, df: DataFrame, table_config, key_phi_columns):
        self.sourcedb = sourcedb
        self.df = df
        self.table_config = table_config
        self.engine = sourcedb.engine
        self.metadata = MetaData()
        self.key_phi_columns = key_phi_columns


    def _load_reference_table(self, table_name: str, columns: list[str], filter_column: str, filter_values: list) -> DataFrame:
        """
        Load specific columns from a reference table, filtered by provided values.
        """
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        columns_expr = [table.c[col] for col in columns]
        stmt = select(*columns_expr).where(table.c[filter_column].in_(filter_values))
        with self.engine.connect() as conn:
            ref_df = pd.read_sql(stmt, conn).drop_duplicates(subset=columns)
        return ref_df

    def join_dataframe(self) -> DataFrame:
        mapping = self.table_config.get("reference_mapping", "")

        if not mapping:
            nd_logger.info("[ReferenceJoiner] No reference mapping found — returning original DataFrame.")
            return self.df, self.key_phi_columns

        original_row_count = len(self.df)
        join_result_df = self.df.copy()
        destination_col = mapping["destination_column"]
        destination_column_type = mapping['destination_column_type'].upper()



        for idx, condition in enumerate(mapping["conditions"]):
            source_col = condition["source_column"]
            join_col = condition["column_name"]
            ref_table = condition["reference_table"]

            # Determine the next column to fetch
            next_column = (
                mapping["conditions"][idx + 1]["source_column"]
                if idx + 1 < len(mapping["conditions"])
                else destination_col
            )

            columns_to_fetch = [join_col, next_column]

            # Only fetch values needed for current join
            filter_values = join_result_df[source_col].dropna().unique().tolist()
            if not filter_values:
                nd_logger.warning(f"[ReferenceJoiner] No values to join on for source column '{source_col}'. Skipping.")
                continue

            reference_df = self._load_reference_table(ref_table, columns_to_fetch, join_col, filter_values)
            reference_df = reference_df.rename(columns={col: f"{col}_ref" for col in reference_df.columns})


            left_col = source_col if idx == 0 else f"{source_col}_ref"
            right_col = f"{join_col}_ref"
    
            nd_logger.debug(f"[ReferenceJoiner] Joining on {left_col} = {right_col}")

            join_result_df = pd.merge(
                join_result_df,
                reference_df,
                how="left",
                left_on=left_col,
                right_on=right_col
            ).drop(columns=[join_col+"_ref"])


        destination_col = f"{destination_col}_ref"
        # Final column selection and deduplication
        final_columns = list(self.df.columns) + [destination_col]
        if destination_col not in join_result_df.columns:
            raise ValueError(f"[ReferenceJoiner] Destination column '{destination_col}' not found after join.")

        # Deduplicate using index group to retain only one row per original row
        deduped_df = join_result_df[final_columns].groupby(self.df.index).first().reset_index(drop=True)

        if len(deduped_df) != original_row_count:
            raise ValueError(
                f"[ReferenceJoiner] Row count mismatch: Expected {original_row_count}, got {len(deduped_df)}"
            )
        
        
        if destination_column_type == "ENCOUNTER_ID":
            self.key_phi_columns[0].insert(0, destination_col)

        if destination_column_type == "PATIENT_ID":
            self.key_phi_columns[1].insert(0, destination_col)
        
        if destination_column_type == "REFERENCE_PID":
            self.key_phi_columns[2].insert(0, destination_col)

        nd_logger.info("[ReferenceJoiner] Successfully joined reference data without row explosion.")
        return deduped_df, self.key_phi_columns
