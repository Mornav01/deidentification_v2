import polars as pl
from sqlalchemy import select
from deid.core.dbPkg import NDDBHandler
from deid.core.logger import nd_logger


class ReferenceMappingDataFrameJoiner:
    """Resolve indirect patient/encounter IDs via multi-hop reference table joins.

    For tables whose PHI column does not directly map to a patient_id / encounter_id
    in the mapping table, this class walks a chain of reference tables defined in
    ``table_config["reference_mapping"]`` and appends the resolved destination column
    to the DataFrame.

    All joins are performed against the source database (or a dedicated ``join_db``
    when configured) using Polars, which executes joins in parallel on multiple CPU
    cores — significantly faster than the previous pd.merge() path.
    """

    def __init__(self, sourcedb: NDDBHandler, df: pl.DataFrame, table_config, key_phi_columns,
                 join_db: NDDBHandler | None = None):
        self.sourcedb = sourcedb
        self.df = df
        self.table_config = table_config
        self.key_phi_columns = key_phi_columns
        # Use join_db for reference-table lookups when configured; fall back to sourcedb.
        self._ref_db = join_db if join_db is not None else sourcedb
        self.engine = self._ref_db.engine
        _db_url = str(self._ref_db.engine.url)
        if join_db is not None:
            nd_logger.info(f"[ReferenceJoiner] Using join_db for reference table lookups: {_db_url}")
        else:
            nd_logger.info(f"[ReferenceJoiner] No join_db configured — using source DB for reference table lookups: {_db_url}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    # SQL Server raises error 8632 when the IN (...) clause is too large.
    _IN_CLAUSE_CHUNK_SIZE = 1000

    def _load_reference_table(
        self,
        table_name: str,
        columns: list[str],
        filter_column: str,
        filter_values: list,
    ) -> pl.DataFrame:
        """Load *columns* from *table_name*, filtered by *filter_values*.

        Splits large filter_values lists into chunks to avoid SQL Server
        error 8632 (expression services limit reached on large IN clauses).
        """
        nd_logger.debug(f"[ReferenceJoiner] Loading '{table_name}' (cols={columns}) from {self._ref_db.engine.url}")
        table = self._ref_db._reflect_table(table_name)
        columns_expr = [table.c[col] for col in columns]

        chunk_size = self._IN_CLAUSE_CHUNK_SIZE
        chunks = [filter_values[i:i + chunk_size] for i in range(0, len(filter_values), chunk_size)]

        # Filter by nd_ActiveFlag = 'Y' if that column exists in the reference table.
        # This ensures deterministic results when a reference table has multiple rows
        # for the same join key (e.g. one active, one inactive record).
        active_flag_filter = table.c["nd_ActiveFlag"] == "Y" if "nd_ActiveFlag" in table.c else None

        rows = []
        col_names = None
        with self.engine.connect() as conn:
            for chunk in chunks:
                stmt = select(*columns_expr).where(table.c[filter_column].in_(chunk))
                if active_flag_filter is not None:
                    stmt = stmt.where(active_flag_filter)
                result = conn.execute(stmt)
                if col_names is None:
                    col_names = list(result.keys())
                rows.extend(result.fetchall())

        if not rows or col_names is None:
            return pl.DataFrame(schema={c: pl.Utf8 for c in (col_names or columns)})

        ref_df = pl.DataFrame(
            [list(r) for r in rows],
            schema=col_names,
            orient="row",
        ).unique(subset=columns)
        return ref_df

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def join_dataframe(self) -> tuple[pl.DataFrame, tuple]:
        """Walk the reference-mapping chain and append the destination column.

        Returns:
            (enriched_df, key_phi_columns)  — key_phi_columns is mutated in-place
            to prepend the destination column name at the correct index position
            (ENCOUNTER_ID → index 0, PATIENT_ID → index 1, REFERENCE_PID → index 2).
        """
        mapping = self.table_config.get("reference_mapping", "")
        if not mapping:
            nd_logger.info("[ReferenceJoiner] No reference mapping — returning original DataFrame.")
            return self.df, self.key_phi_columns

        original_row_count = self.df.height

        # Tag every row with a stable integer index so we can deduplicate
        # after the left-joins without relying on Pandas' integer index.
        join_result_df = self.df.with_row_index("_nd_row_idx")

        destination_col = mapping["destination_column"]
        destination_column_type = mapping["destination_column_type"].upper()

        for idx, condition in enumerate(mapping["conditions"]):
            source_col = condition["source_column"]
            join_col = condition["column_name"]
            ref_table = condition["reference_table"]

            # The column to carry forward to the next hop
            next_column = (
                mapping["conditions"][idx + 1]["source_column"]
                if idx + 1 < len(mapping["conditions"])
                else destination_col
            )

            columns_to_fetch = [join_col, next_column]

            # Only fetch rows relevant to current batch
            left_col = source_col if idx == 0 else f"{source_col}_ref"
            if left_col not in join_result_df.columns:
                nd_logger.warning(f"[ReferenceJoiner] Column '{left_col}' not found. Skipping hop.")
                continue

            filter_values = join_result_df[left_col].drop_nulls().unique().to_list()
            if not filter_values:
                nd_logger.warning(f"[ReferenceJoiner] No values to join on '{source_col}'. Skipping.")
                continue

            reference_df = self._load_reference_table(
                ref_table, columns_to_fetch, join_col, filter_values
            )
            # Rename ALL reference columns to avoid name collisions with source table.
            reference_df = reference_df.rename(
                {col: f"{col}_ref" for col in reference_df.columns}
            )

            right_col = f"{join_col}_ref"
            nd_logger.debug(f"[ReferenceJoiner] Joining on {left_col} = {right_col}")

            # Cast both join keys to a common type to avoid mismatches (e.g. f64 vs decimal[38,0]).
            left_dtype = join_result_df[left_col].dtype
            right_dtype = reference_df[right_col].dtype
            if left_dtype != right_dtype:
                nd_logger.debug(
                    f"[ReferenceJoiner] Type mismatch on join key: "
                    f"{left_col}={left_dtype} vs {right_col}={right_dtype}. Attempting cast."
                )
                try:
                    join_result_df = join_result_df.with_columns(pl.col(left_col).cast(pl.Int64))
                    reference_df = reference_df.with_columns(pl.col(right_col).cast(pl.Int64))
                    nd_logger.debug("[ReferenceJoiner] Cast both keys to Int64.")
                except Exception:
                    join_result_df = join_result_df.with_columns(pl.col(left_col).cast(pl.Utf8))
                    reference_df = reference_df.with_columns(pl.col(right_col).cast(pl.Utf8))
                    nd_logger.debug("[ReferenceJoiner] Int64 cast failed; fell back to Utf8.")

            # Polars left-join: right key column is excluded from output automatically.
            join_result_df = join_result_df.join(
                reference_df,
                left_on=left_col,
                right_on=right_col,
                how="left",
                suffix="_right",
            )

        # Compose the final column list: original df columns + resolved destination.
        destination_col_ref = f"{destination_col}_ref"
        final_columns = ["_nd_row_idx"] + list(self.df.columns) + [destination_col_ref]
        available = [c for c in final_columns if c in join_result_df.columns]

        if destination_col_ref not in join_result_df.columns:
            raise ValueError(
                f"[ReferenceJoiner] Destination column '{destination_col_ref}' not found after join."
            )

        # Deduplicate: keep the first match per original row (avoids row explosion
        # when a reference table has multiple matches for a single source value).
        deduped_df = (
            join_result_df.select(available)
            .unique(subset=["_nd_row_idx"], keep="first")
            .sort("_nd_row_idx")
            .drop("_nd_row_idx")
        )

        if deduped_df.height != original_row_count:
            raise ValueError(
                f"[ReferenceJoiner] Row count mismatch: "
                f"expected {original_row_count}, got {deduped_df.height}"
            )

        # Register the resolved column with the appropriate PHI-column list.
        if destination_column_type == "ENCOUNTER_ID":
            self.key_phi_columns[0].insert(0, destination_col_ref)
        elif destination_column_type.startswith("PATIENT_"):
            # key_phi_columns[1] is a dict; insert under the matching rule (or create entry).
            self.key_phi_columns[1].setdefault(destination_column_type, []).insert(0, destination_col_ref)
        elif destination_column_type == "REFERENCE_PID":
            self.key_phi_columns[2].insert(0, destination_col_ref)

        nd_logger.info("[ReferenceJoiner] Successfully joined reference data without row explosion.")
        return deduped_df, self.key_phi_columns
