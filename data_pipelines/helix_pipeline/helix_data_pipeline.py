"""
Script for importing data from Helix database into DNAPROD.
"""

from sql_connector import *
from functions import incremental_load
import pandas as pd
import numpy as np

# connecting to both Helix and DNAPROD sql servers
sql_helix = sql_connector('HELIX', 'db_name')
sql_dnaprod = sql_connector('DNAPROD', 'Helix')

# tables names for which we want to apply incremental load
incremental_tables_names = [
    'table1',
    'table2'
]

# tables names which we want to import without incremental load
tables_names = [
    'table3',
    'table4'
]

# execute a sql query and save result into a variable x
x = sql_dnaprod.read_query('some_query')

# importing tables incrementally
for table in incremental_tables_names:
    new_table = incremental_load(
        sql_helix,
        table,
        sql_dnaprod,
        table,
        column_name
    )
    sql_dnaprod.to_sql(new_table)

# importing tables
for table in tables_names:
    sql_dnaprod.to_sql(table)
    
# execute some sql script after loading tables
sql_dnaprod.execute_sql_file('sql_file_path')