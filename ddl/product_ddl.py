import utils.logger as Logger

def create_tables(curr):
    try:
        # create stage table
        curr.execute("USE SCHEMA STG;")
        curr.execute("""
            create or replace TABLE STG_D_PRODUCT_LU (
                product_id NUMBER(38,0) NOT NULL,
                subcategory_id NUMBER(38,0),
                product_desc VARCHAR(256),
                primary key (product_id),
                foreign key (subcategory_id) references STG.STG_D_SUBCATEGORY_LU(subcategory_id)
            );
        """)

        # create temp table
        curr.execute("USE SCHEMA TMP;")
        curr.execute("""
            create or replace TABLE TMP_D_PRODUCT_LU (
                product_id NUMBER(38,0) NOT NULL,
                subcategory_id NUMBER(38,0),
                product_desc VARCHAR(256),
                primary key (product_id),
                foreign key (subcategory_id) references TMP.TMP_D_SUBCATEGORY_LU(subcategory_id)
            );
        """)
    
        # create target table
        curr.execute("USE SCHEMA TGT;")
        curr.execute("""
            create or replace TABLE DWH_D_PRODUCT_LU (
                product_key NUMBER(38,0) NOT NULL,
                product_id NUMBER(38,0) NOT NULL,
                subcategory_key NUMBER(38,0),
                product_desc VARCHAR(256),
                active_flag BOOLEAN DEFAULT TRUE,
                CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS TIMESTAMP DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59.99'),
                primary key (product_key),
                foreign key (subcategory_key) references TGT.DWH_D_SUBCATEGORY_LU(subcategory_key)
            );
        """)
        print("Successfully created product tables in all schemas.")
        Logger.log_info("[Success] Created product tables in all schemas.")

    except Exception as e:
        print(f"Error creating product tables: {e}")
        Logger.log_info(f"[Error] Failed creating product tables : {e}")
