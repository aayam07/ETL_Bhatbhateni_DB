import utils.logger as Logger

def create_tables(curr):
    try:
        # create stage table
        curr.execute("USE SCHEMA STG;")
        curr.execute("""
            create or replace TABLE STG_D_STORE_LU (
                store_id NUMBER(38,0) NOT NULL,
                region_id NUMBER(38,0),
                store_desc VARCHAR(256),
                primary key (store_id),
                foreign key (region_id) references STG.STG_D_REGION_LU(region_id)
            );
        """)

        # create temp table
        curr.execute("USE SCHEMA TMP;")
        curr.execute("""
            create or replace TABLE TMP_D_STORE_LU (
                store_id NUMBER(38,0) NOT NULL,
                region_id NUMBER(38,0),
                store_desc VARCHAR(256),
                primary key (store_id),
                foreign key (region_id) references TMP.TMP_D_REGION_LU(region_id)
            );
        """)

        # create target table
        curr.execute("USE SCHEMA TGT;")
        curr.execute("""
            create or replace TABLE DWH_D_STORE_LU (
                store_key NUMBER(38,0) NOT NULL,
                store_id NUMBER(38,0) NOT NULL,
                region_key NUMBER(38,0),
                store_desc VARCHAR(256),
                active_flag BOOLEAN DEFAULT TRUE,
                CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS TIMESTAMP DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59.99'),
                primary key (store_key),
                foreign key (region_key) references TGT.DWH_D_REGION_LU(region_key)
            );
        """)

        print("Successfully created store tables in all schemas.")
        Logger.log_info("[Success] Created store tables in all schemas.")

    except Exception as e:
        print(f"Error creating store tables: {e}")
        Logger.log_info(f"[Error] Failed creating store tables : {e}")
