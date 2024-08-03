import sys, os

sys.path.append(
    os.path.join(os.path.dirname(os.getcwd()), 'functions')
)

from sql_connector import *
from functions import *
import pandas as pd
import numpy as np
from datetime import datetime


sql_helix = sql_connector('Empower', 'db_name')
sql_dnaprod = sql_connector('DNAPROD', 'Helix')

incremental_tables_names = [
    'table1',
    'table2'
]

tables_names = [
    'table3',
    'table4'
]

for table in incremental_tables_names:
    new_table = incremental_load(table)
    sql_dnaprod.to_sql(
        new_table
        ,if_exists = 'append'
    )
    
for table in tables_names:
    sql_dnaprod.to_sql(
        table
        ,if_exists = 'append'
    )
