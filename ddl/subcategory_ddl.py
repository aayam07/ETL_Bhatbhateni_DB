import utils.logger as Logger

def create_tables(curr):
    try:
        # create stage table
        curr.execute("USE SCHEMA STG;")
        curr.execute("""
            create or replace TABLE STG_D_SUBCATEGORY_LU (
                subcategory_id NUMBER(38,0) NOT NULL,
                category_id NUMBER(38,0),
                subcategory_desc VARCHAR(256),
                primary key (subcategory_id),
                foreign key (category_id) references STG.STG_D_CATEGORY_LU(category_id)
            );
        """)

        # create temp table
        curr.execute("USE SCHEMA TMP;")
        curr.execute("""
            create or replace TABLE TMP_D_SUBCATEGORY_LU (
                subcategory_id NUMBER(38,0) NOT NULL,
                category_id NUMBER(38,0),
                subcategory_desc VARCHAR(256),
                primary key (subcategory_id),
                foreign key (category_id) references TMP.TMP_D_CATEGORY_LU(category_id)
            );
        """)

        # create target table
        curr.execute("USE SCHEMA TGT;")
        curr.execute("""
            create or replace TABLE DWH_D_SUBCATEGORY_LU (
                subcategory_key NUMBER(38,0) NOT NULL,
                subcategory_id NUMBER(38,0) NOT NULL,
                category_key NUMBER(38,0),
                subcategory_desc VARCHAR(256),
                active_flag BOOLEAN DEFAULT TRUE,
                CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS TIMESTAMP DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59.99'),
                primary key (subcategory_key),
                foreign key (category_key) references TGT.DWH_D_CATEGORY_LU(category_key)
            );
        """)
        print("Successfully created subcategory tables in all schemas.")
        Logger.log_info("[Success] Created subcategory tables in all schemas.")

    except Exception as e:
        print(f"Error creating subcategory tables: {e}")
        Logger.log_info(f"[Error] Failed creating subcategory tables : {e}")
