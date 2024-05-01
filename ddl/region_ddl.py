import utils.logger as Logger

def create_tables(curr):
    try:
        # create stage table
        curr.execute("USE SCHEMA STG;")
        curr.execute("""
            create or replace TABLE STG_D_REGION_LU (
                region_id NUMBER(38,0) NOT NULL,
                country_id NUMBER(38,0),
                region_desc VARCHAR(256),
                primary key (region_id),
                foreign key (country_id) references STG.STG_D_COUNTRY_LU(country_id)
            );
        """)

        # create temp table
        curr.execute("USE SCHEMA TMP;")
        curr.execute("""
            create or replace TABLE TMP_D_REGION_LU (
                region_id NUMBER(38,0) NOT NULL,
                country_id NUMBER(38,0),
                region_desc VARCHAR(256),
                primary key (region_id),
                foreign key (country_id) references TMP.TMP_D_COUNTRY_LU(country_id)
            );
        """)

        # create target table
        curr.execute("USE SCHEMA TGT;")
        curr.execute("""
            create or replace TABLE DWH_D_REGION_LU (
                region_key NUMBER(38,0) NOT NULL,
                region_id NUMBER(38,0) NOT NULL,
                country_key NUMBER(38,0),
                region_desc VARCHAR(256),
                active_flag BOOLEAN DEFAULT TRUE,
                CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS TIMESTAMP DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59.99'),
                primary key (region_key),
                foreign key (country_key) references TGT.DWH_D_COUNTRY_LU(country_key)
            );
        """)
        print("Successfully created region tables in all schemas.")
        Logger.log_info("[Success] Created region tables in all schemas.")

    except Exception as e:
        print(f"Error creating region tables: {e}")
        Logger.log_info(f"[Error] Failed creating region tables : {e}")
