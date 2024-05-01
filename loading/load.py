import utils.logger as Logger
import utils.truncate_table as truncate


# function to load to staging table
def load_to_stage(cur, table_name, stg_table):

    db = 'AAYAMDWH_DB'
    schema = 'STG'
    try:
        truncate.trunc(db, schema, stg_table, cur)  # first remove all data if exists in staging table
        cur.execute("USE SCHEMA STG")
        cur.execute(f"""
            Copy into STG.{stg_table}
            From @file_stg/{table_name.lower()}.csv.gz
            file_format=(
            type = csv,
            field_delimiter = ',',
            skip_header = 1)
            on_error= "continue";
        """)
        print(f'[Success] Loaded {table_name} to STG.')
        Logger.log_info(f'[Success] Loaded {table_name} to STG.')
    except Exception as e:
        print(f'[Error] Failed loading {table_name} to STG, {e}')
        Logger.log_info(f'[Error] Failed loading {table_name} to STG.')


# function to load data to temp table
def load_to_temp(cur, table_name, to, src):

    db = 'AAYAMDWH_DB'
    schema = 'TMP'
    try:
        truncate.trunc(db, schema, to, cur)  # first remove all data if exists in temporary table

        cur.execute(f"""
            Insert into TMP.{to}
            Select * from STG.{src}
        """)
        
        print(f'[Success] Loaded {table_name} to TMP.')
        Logger.log_info(f'[Success] Loaded {table_name} to TMP.')

    except Exception as e:
        print(f'[Error] Failed loading {table_name} to TMP, {e}')
    