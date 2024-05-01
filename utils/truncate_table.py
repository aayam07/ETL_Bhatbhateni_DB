import utils.logger as Logger

def trunc(db, schema, table_name, cur):
    cur.execute(f"USE DATABASE {db}")
    cur.execute(f"USE SCHEMA {schema}")
    cur.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

    Logger.log_info(f'{table_name} truncated successfully! \n')