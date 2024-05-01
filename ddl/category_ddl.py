import utils.logger as Logger

def create_tables(curr):
    try:
        # create stage table
        curr.execute("USE SCHEMA STG;")
        curr.execute("""
            create or replace TABLE STG_D_CATEGORY_LU (
            category_id NUMBER(38,0) NOT NULL,
            category_desc VARCHAR(1024),
            primary key (category_id)
        );
        """)

        # create temp table
        curr.execute("USE SCHEMA TMP;")
        curr.execute("""
            create or replace TABLE TMP_D_CATEGORY_LU LIKE STG.STG_D_CATEGORY_LU
        """)

        # create target table
        curr.execute("USE SCHEMA TGT;")
        curr.execute("""
            create or replace TABLE DWH_D_CATEGORY_LU (
            category_key NUMBER(38,0) NOT NULL,
            category_id NUMBER(38,0) UNIQUE NOT NULL,
            category_desc VARCHAR(1024),
            active_flag BOOLEAN DEFAULT TRUE,
            CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
	        UPDATED_TS TIMESTAMP DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59.99'),
            primary key (category_key)
        );
        """)
        print("Successfully created category tables in all schemas.")
        Logger.log_info("[Success] Created category tables in all schemas.")

    except Exception as e:
        print(f"Error creating category tables: {e}")
        Logger.log_info(f"[Error] Failed creating category tables : {e}")
