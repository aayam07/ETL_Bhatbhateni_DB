import utils.logger as Logger

def create_tables(curr):
    try:
        # create stage table
        curr.execute("USE SCHEMA STG;")
        curr.execute("""
            create or replace TABLE STG_F_SALES (
                sales_id NUMBER,
                store_id NUMBER,
                product_id NUMBER,
                customer_id NUMBER,
                transaction_time TIMESTAMP,
                quantity NUMBER,
                amount NUMBER(20,2),
                discount NUMBER(20,2),
                primary key (sales_id),
                FOREIGN KEY (store_id) references STG.STG_D_STORE_LU(store_id),
                FOREIGN KEY (product_id) references STG.STG_D_PRODUCT_LU(product_id),
                FOREIGN KEY (customer_id) references STG.STG_D_CUSTOMER_LU(customer_id)
            );
        """)

        # create temp table
        curr.execute("USE SCHEMA TMP;")
        curr.execute("""
            create or replace TABLE TMP_F_SALES(
                sales_id NUMBER,
                store_id NUMBER,
                product_id NUMBER,
                customer_id NUMBER,
                transaction_time TIMESTAMP,
                quantity NUMBER,
                amount NUMBER(20,2),
                discount NUMBER(20,2),
                primary key (sales_id),
                FOREIGN KEY (store_id) references TMP.TMP_D_STORE_LU(store_id),
                FOREIGN KEY (product_id) references TMP.TMP_D_PRODUCT_LU(product_id),
                FOREIGN KEY (customer_id) references TMP.TMP_D_CUSTOMER_LU(customer_id)
            );
        """)

        # create target table
        curr.execute("USE SCHEMA TGT;")
        curr.execute("""
            create or replace TABLE DWH_F_SALES_TRXN_B (
                sales_key NUMBER,
                sales_id NUMBER,
                store_key NUMBER,
                product_key NUMBER,
                customer_key NUMBER,
                transaction_time TIMESTAMP,
                quantity NUMBER,
                amount NUMBER(20,2),
                discount NUMBER(20,2),
                primary key (sales_key),
                FOREIGN KEY (store_key) references TGT.DWH_D_STORE_LU(store_key),
                FOREIGN KEY (product_key) references TGT.DWH_D_PRODUCT_LU(product_key),
                FOREIGN KEY (customer_key) references TGT.DWH_D_CUSTOMER_LU(customer_key),
                CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """)

        curr.execute("""
            create or replace TABLE DWH_F_AGG_SLS_PLC_MONTH_T (
                key NUMBER AUTOINCREMENT,
                store_id NUMBER,
                month VARCHAR,
                amount NUMBER(20,2),
                discount NUMBER(20,2),
                primary key (key),
                CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """)

        print("Successfully created sales tables in all schemas.")
        Logger.log_info("[Success] Created sales tables in all schemas.")

    except Exception as e:
        print(f"Error creating sales tables: {e}")
        Logger.log_info(f"[Error] Failed creating sales tables : {e}")

