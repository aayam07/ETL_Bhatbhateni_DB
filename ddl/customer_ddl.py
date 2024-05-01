
import utils.logger as Logger

def create_tables(curr):
    try:
        # create stage table
        curr.execute("USE SCHEMA STG;")
        curr.execute("""
            create or replace TABLE STG_D_CUSTOMER_LU (
                customer_id NUMBER(38,0) NOT NULL,
                customer_first_name VARCHAR(256),
                customer_middle_name VARCHAR(256),
                customer_last_name VARCHAR(256),
                customer_address VARCHAR(256),
                primary key (customer_id)
            );
        """)

        # create temp table
        curr.execute("USE SCHEMA TMP;")
        curr.execute("""
            create or replace TABLE TMP_D_CUSTOMER_LU(
                customer_id NUMBER(38,0) NOT NULL,
                customer_first_name VARCHAR(256),
                customer_middle_name VARCHAR(256),
                customer_last_name VARCHAR(256),
                customer_address VARCHAR(256),
                primary key (customer_id)
            );
        """)

        # create target table
        curr.execute("USE SCHEMA TGT;")
        curr.execute("""
            create or replace TABLE DWH_D_CUSTOMER_LU (
                customer_key NUMBER(38,0) NOT NULL,
                customer_id NUMBER(38,0) NOT NULL,
                customer_first_name VARCHAR(256),
                customer_middle_name VARCHAR(256),
                customer_last_name VARCHAR(256),
                customer_address VARCHAR(256),
                active_flag BOOLEAN DEFAULT TRUE,
                CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS TIMESTAMP DEFAULT TO_TIMESTAMP('9999-12-31 23:59:59.99'),
                primary key (customer_key)
            );
        """)
        print("Successfully created customer tables in all schemas.")
        Logger.log_info("[Success] Created customer tables in all schemas.")

    except Exception as e:
        print(f"Error creating customer tables: {e}")
        Logger.log_info(f"[Error] Failed creating customer tables : {e}")
