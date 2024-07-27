# function for incremental load for tables between two sql servers
# source and target sql connector are objects of the sql_connector class
# column_name argument indicates name of a column which will be used in order to identify new and modified rows

def incremental_load(
    source_sql_connector,
    source_table_name,
    target_sql_connector,
    target_table_name,
    column_name
):
    source = source_sql_connector.read_query(f'select * from {source_table_name}')
    target = target_sql_connector.read_query(f'select * from {target_table_name}')
    
    # find all rows in the source table which are not present in the target table
    changes = source[~source.apply(tuple, 1).isin(target.apply(tuple, 1))]
    
    # check which rows in the source were modified
    modified = changes[changes[column_name].isin(target[column_name])]

    # check which rows in the source were added
    added = changes[~changes[column_name].isin(target[column_name])]

    target = target.merge(modified, 'left', on = column_name)
    
    return target