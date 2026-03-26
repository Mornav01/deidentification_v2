import numpy as np
import random
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime
from typing import List, Dict, Tuple, Any
from pydantic import validate_call


class DataGenerator:
    def __init__(self, source_engine, dest_engine):
        self.source_engine = source_engine
        self.dest_engine = dest_engine
        current_seed = int(datetime.now().timestamp() * 1000000) % 2147483647
        np.random.seed(current_seed)
        random.seed(current_seed)

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_total_rows(self, table_name: str) -> int:
        with self.dest_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def calculate_sample_size(self, n: int) -> int:
        if n <= 300:
            return n
        elif n <= 1000:
            return random.randint(300, 500)
        elif n <= 5000:
            return random.randint(500, 800)
        elif n <= 10000:
            return random.randint(800, 1000)
        elif n <= 100000:
            return min(3000 + int((n - 10000) / 30000) * 1000, 5000)
        else:
            return 5000

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_random_sample(self, table_name: str, size: int) -> List[Dict[str, Any]]:
        """Sample rows efficiently using ID-based random selection.

        Avoids ``ORDER BY RAND()`` which forces a full-table filesort on MySQL
        (O(N log N) regardless of LIMIT), causing the worker to block on I/O
        with zero CPU usage for minutes on large tables.

        Instead: fetch the ID range, pick random IDs in Python, then fetch
        those specific rows via a keyset lookup — O(size) index seeks.
        """
        dialect = self.dest_engine.dialect.name
        with self.dest_engine.connect() as conn:
            id_col = "nd_auto_increment_id"
            bounds = conn.execute(
                text(f"SELECT MIN({id_col}), MAX({id_col}) FROM {table_name}")
            ).fetchone()
            min_id, max_id = bounds[0], bounds[1]

            if min_id is None or max_id is None:
                return []

            # Over-sample to account for gaps in the ID space.
            candidate_ids = random.sample(
                range(int(min_id), int(max_id) + 1),
                min(size * 3, int(max_id) - int(min_id) + 1),
            )
            # Fetch rows matching the random IDs.
            placeholders = ",".join(str(int(i)) for i in candidate_ids)
            if dialect == "mssql":
                query = text(
                    f"SELECT TOP :lim * FROM {table_name} "
                    f"WHERE {id_col} IN ({placeholders})"
                )
            else:
                query = text(
                    f"SELECT * FROM {table_name} "
                    f"WHERE {id_col} IN ({placeholders}) LIMIT :lim"
                )
            result = conn.execute(query, {"lim": size})
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result]
        
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_sample(self, table_name: str, size: int) -> List[Dict[str, Any]]:
        query = text(f"""SELECT * FROM {table_name} LIMIT {size}""")
        source_sample = []
        dest_sample = []
        with self.dest_engine.connect() as conn:
            # query = text(f"""SELECT * FROM {table_name} LIMIT {size}""")
            result = conn.execute(query)
            columns = result.keys()
            dest_sample = [dict(zip(columns, row)) for row in result]
    
        with self.source_engine.connect() as conn:
            # query = text(f"""SELECT TOP {size} * FROM [dbo].[{table_name}] """)
            result = conn.execute(query)
            columns = result.keys()
            source_sample = [dict(zip(columns, row)) for row in result]
        return source_sample, dest_sample
    
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_sample_for_nd_ids(self, table_name: str, nd_auto_incr_ids: list[int]) -> List[Dict[str, Any]]:
        query = text(f"""SELECT * FROM {table_name} where  nd_auto_increment_id in {nd_auto_incr_ids}""")
        source_sample = []
        dest_sample = []
        with self.dest_engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            dest_sample = [dict(zip(columns, row)) for row in result]
    
        with self.source_engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            source_sample = [dict(zip(columns, row)) for row in result]
        return source_sample, dest_sample

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def get_stratified_sample(self, table_name: str, sample_size: int, initial_data: List[Dict[str, Any]], important_columns: list[str]) -> List[Dict[str, Any]]:
        samples = initial_data.copy()
        remaining_size = sample_size - len(initial_data)
        
        if remaining_size <= 0:
            return initial_data
            
        for col in important_columns:
            col_values = [row[col] for row in initial_data if col in row]
            if not col_values:
                continue
            
            # filtered_values = [x for x in col_values if x is not None]
            is_numeric = all(isinstance(x, (int, float)) for x in col_values if x is not None)
            
            if is_numeric:
                sorted_values = sorted(col_values, key=lambda x: (x is not None, x))
                n = len(sorted_values)
                quartiles = {
                    0.25: sorted_values[int(n * 0.25)],
                    0.5: sorted_values[int(n * 0.5)],
                    0.75: sorted_values[int(n * 0.75)]
                }
                
                samples_per_range = remaining_size // (len(important_columns) * 4)
                
                for lower, upper in [
                    (None, quartiles[0.25]),
                    (quartiles[0.25], quartiles[0.5]),
                    (quartiles[0.5], quartiles[0.75]),
                    (quartiles[0.75], None)
                ]:
                    where_clause = ""
                    params: Dict[str, Any] = {"limit": samples_per_range}
                    
                    if lower is not None and upper is not None:
                        where_clause = f"WHERE {col} > :lower AND {col} <= :upper"
                        params.update({"lower": float(lower), "upper": float(upper)})
                    elif lower is not None:
                        where_clause = f"WHERE {col} > :lower"
                        params.update({"lower": float(lower)})
                    elif upper is not None:
                        where_clause = f"WHERE {col} <= :upper"
                        params.update({"upper": float(upper)})
                        
                    query = text(f"""
                        SELECT *
                        FROM {table_name}
                        {where_clause}
                        LIMIT :limit
                    """)

                    with self.dest_engine.connect() as conn:
                        result = conn.execute(query, params)
                        columns = result.keys()
                        strata_sample = [dict(zip(columns, row)) for row in result]
                        samples.extend(strata_sample)
            
            else:
                value_counts = {}
                for value in col_values:
                    value_counts[value] = value_counts.get(value, 0) + 1
                
                total = sum(value_counts.values())
                value_counts = {k: v/total for k, v in value_counts.items()}
                
                samples_per_category = remaining_size // (len(important_columns) * len(value_counts))
                
                for category in value_counts.keys():
                    query = text(f"""
                        SELECT *
                        FROM {table_name}
                        WHERE {col} = :category
                        LIMIT :limit
                    """)
                    
                    with self.dest_engine.connect() as conn:
                        result = conn.execute(query, {
                            "category": category,
                            "limit": samples_per_category
                        })
                        columns = result.keys()
                        strata_sample = [dict(zip(columns, row)) for row in result]
                        samples.extend(strata_sample)
        
        seen = set()
        unique_samples = []
        for item in samples:
            item_tuple = tuple(item.items())
            if item_tuple not in seen:
                seen.add(item_tuple)
                unique_samples.append(item)
        
        if len(unique_samples) < sample_size:
            remaining_needed = sample_size - len(unique_samples)
            additional_sample = self.get_random_sample(table_name, remaining_needed)
            unique_samples.extend(additional_sample)
            
            seen = set()
            final_samples = []
            for item in unique_samples:
                item_tuple = tuple(item.items())
                if item_tuple not in seen:
                    seen.add(item_tuple)
                    final_samples.append(item)
            
            return final_samples[:sample_size]
        
        return unique_samples[:sample_size]

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def generate_sample(self, table_name: str, important_columns: List[str], is_structured: bool = True) -> Tuple[int, List[Dict[str, Any]]]:
        total_rows = self.get_total_rows(table_name)
        sample_size = self.calculate_sample_size(total_rows)
        if is_structured:
            source_sample, dest_engine = self.get_sample(table_name, sample_size)
            return total_rows, source_sample, dest_engine
        
        initial_sample_size = min(100, sample_size)
        initial_sample = self.get_random_sample(table_name, initial_sample_size)
        
        final_sample = self.get_stratified_sample(table_name, sample_size, initial_sample, important_columns)
        
        nd_auto_ids = [row['nd_auto_increment_id'] for row in final_sample]
        source_sample = []
        return sample_size, source_sample, final_sample
