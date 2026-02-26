import numpy as np
import random
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime
from typing import List, Dict, Tuple, Any, Union

def get_smart_sample(
    table_name: str,
    engine: Engine,
    important_columns: List[str],
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Determines optimal sample size and loads sample data using SQLAlchemy
    Returns different random samples on each call
    
    Args:
        table_name (str): Name of the table to sample from
        engine (Engine): SQLAlchemy engine instance
        important_columns (List[str]): Columns that are crucial for QC verification
    
    Returns:
        Tuple[int, List[Dict[str, Any]]]: Sample size and sampled data as list of dicts
    """
    current_seed = int(datetime.now().timestamp() * 1000000) % 2147483647
    np.random.seed(current_seed)
    random.seed(current_seed)
    
    def get_total_rows() -> int:
        """Get total number of rows in the table"""
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()
    
    def calculate_sample_size(n: int) -> int:
        """Calculate sample size based on population size using a stepped approach"""
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

    def get_random_sample(size: int) -> List[Dict[str, Any]]:
        """Get random sample using MySQL's RAND() function"""
        query = text(f"""
            SELECT * 
            FROM {table_name}
            ORDER BY RAND()
            LIMIT :sample_size
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"sample_size": size})
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result]

    def get_stratified_sample(sample_size: int, initial_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get stratified sample based on important columns"""
        samples = initial_data.copy()
        remaining_size = sample_size - len(initial_data)
        
        if remaining_size <= 0:
            return initial_data
            
        for col in important_columns:
            # Get column values for analysis
            col_values = [row[col] for row in initial_data if col in row]
            if not col_values:
                continue
                
            # Check if column is numeric
            is_numeric = all(isinstance(x, (int, float)) for x in col_values if x is not None)
            
            if is_numeric:
                # Calculate quartiles
                sorted_values = sorted(col_values)
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
                        ORDER BY RAND()
                        LIMIT :limit
                    """)
                    
                    with engine.connect() as conn:
                        result = conn.execute(query, params)
                        columns = result.keys()
                        strata_sample = [dict(zip(columns, row)) for row in result]
                        samples.extend(strata_sample)
            
            else:
                # Handle categorical columns
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
                        ORDER BY RAND()
                        LIMIT :limit
                    """)
                    
                    with engine.connect() as conn:
                        result = conn.execute(query, {
                            "category": category,
                            "limit": samples_per_category
                        })
                        columns = result.keys()
                        strata_sample = [dict(zip(columns, row)) for row in result]
                        samples.extend(strata_sample)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_samples = []
        for item in samples:
            item_tuple = tuple(item.items())
            if item_tuple not in seen:
                seen.add(item_tuple)
                unique_samples.append(item)
        
        # If we still need more samples, get them randomly
        if len(unique_samples) < sample_size:
            remaining_needed = sample_size - len(unique_samples)
            additional_sample = get_random_sample(remaining_needed)
            unique_samples.extend(additional_sample)
            
            # Remove duplicates again
            seen = set()
            final_samples = []
            for item in unique_samples:
                item_tuple = tuple(item.items())
                if item_tuple not in seen:
                    seen.add(item_tuple)
                    final_samples.append(item)
            
            return final_samples[:sample_size]
        
        return unique_samples[:sample_size]

    # Main execution
    total_rows = get_total_rows()
    sample_size = calculate_sample_size(total_rows)
    
    # Get initial random sample
    initial_sample_size = min(100, sample_size)
    initial_sample = get_random_sample(initial_sample_size)
    
    # Get stratified sample based on initial sample
    final_sample = get_stratified_sample(sample_size, initial_sample)
    
    return sample_size, final_sample


from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:123456789@localhost:3306/nddenttest_mapping')
important_cols = ['patient_id']

# Get samples
sample_size1, sampled_data1 = get_smart_sample(
    table_name='patient_mapping_table',
    engine=engine,
    important_columns=important_cols
)

# Check the results
print(f"Sample size: {sample_size1}")
print(f"First few records: {sampled_data1[:3]}")