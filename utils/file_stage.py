import os
import pathlib
import utils.logger as Logger


# extract file path of csv data files
file_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), "csv")


def file_stage(cur):
    try:
        cur.execute("USE SCHEMA STG")
        cur.execute("""
            CREATE OR REPLACE STAGE FILE_STG
        """)

        for file in os.listdir(file_path):
            cur.execute(f"PUT file://{file_path}\\{file} @file_stg;")

        Logger.log_info('[Success] File Staging Complete. \n')
   
    except Exception as e:
        print(f"[Error] Preliminary File Staging Error",e)
