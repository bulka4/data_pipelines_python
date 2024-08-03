import sys, os

sys.path.append(
    os.path.join(os.path.dirname(os.getcwd()), 'functions')
)

from sql_connector import *
from functions import *
import pandas as pd
import numpy as np
from datetime import datetime


dnaprod = sql_connector('DNAPROD', 'Stage')

for sql_file in [
    'sql_queries/create_table1.sql'
    ,'sql_queries/create_table2.sql'
    ,'sql_queries/create_table3.sql'
    ,'sql_queries/create_table4.sql'
    ,'sql_queries/create_table5.sql'
]:
    dnaprod.execute_sql_file(sql_file)
    
    
for dashboard in [
    'dashboard1'
    ,'dashboard2'
    ,'dashboard3'
]:
    sisense.build_dashboard(dashboard)
    