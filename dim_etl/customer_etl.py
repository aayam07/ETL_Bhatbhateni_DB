from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nCustomer ETL Started ...")
        Logger.log_info("Customer ETL Started ...")

        # extract the customer table to staging schema 
        load.load_to_stage(cur, "CUSTOMER", "STG_D_CUSTOMER_LU")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "CUSTOMER", "TMP_D_CUSTOMER_LU", "STG_D_CUSTOMER_LU")

        # load the customer table to target schema
        #get max value + 1 for surrogate key sequence
        res = cur.execute("""
            SELECT NVL(MAX(customer_key), 0) FROM TGT.DWH_D_CUSTOMER_LU
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.CUSTOMER_SEQUENCE START WITH {start}"
        )

        # New record added
        cur.execute("""
            INSERT INTO TGT.DWH_D_CUSTOMER_LU (customer_key, customer_id, customer_first_name, customer_middle_name, customer_last_name, customer_address)
            SELECT TGT.CUSTOMER_SEQUENCE.NEXTVAL, customer_id, customer_first_name, customer_middle_name, customer_last_name, customer_address
            FROM TMP.TMP_D_CUSTOMER_LU src
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_CUSTOMER_LU dest
                WHERE dest.customer_id = src.customer_id
            );
         """)

        # Record once removed added to the source again
        cur.execute("""
            UPDATE TGT.DWH_D_CUSTOMER_LU dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('9999-12-31 23:59:59.99')
            WHERE EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_CUSTOMER_LU src
                WHERE dest.customer_id = src.customer_id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute("""
            UPDATE TGT.DWH_D_CUSTOMER_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_CUSTOMER_LU src
                WHERE src.customer_id = dest.customer_id
            );
        """)

        # Minor changes
        cur.execute("""
            UPDATE TGT.DWH_D_CUSTOMER_LU dest
            SET customer_first_name = src.customer_first_name,
            customer_last_name = src.customer_last_name,
            customer_middle_name = src.customer_middle_name,
            customer_address = src.customer_address
            FROM TMP.TMP_D_CUSTOMER_LU src
            WHERE dest.customer_id = src.customer_id
            AND (dest.customer_first_name != src.customer_first_name
            OR dest.customer_last_name != src.customer_last_name
            OR dest.customer_middle_name != src.customer_middle_name
            OR dest.customer_address != src.customer_address);
        """)

        print(f'[Success] Loaded CUSTOMER to TGT.')
        Logger.log_info(f'[Success] Loaded CUSTOMER to TGT.')
        
        ##############################################################################


        msg = f"Customer ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Customer: {e}')