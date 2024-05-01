from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nSales ETL Started ...")
        Logger.log_info("Sales ETL Started ...")

        # extract the sales table to staging schema 
        load.load_to_stage(cur, "SALES", "STG_F_SALES")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "SALES", "TMP_F_SALES", "STG_F_SALES")

        # load the sales table to target schema
        #get max value + 1 for surrogate key sequence
        #base table
        res = cur.execute("""
            SELECT NVL(MAX(sales_key), 0) FROM TGT.DWH_F_SALES_TRXN_B
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.SALES_B_SEQUENCE START WITH {start}"
        )

        cur.execute("""
            INSERT INTO TGT.DWH_F_SALES_TRXN_B (sales_key, sales_id, store_key, product_key, customer_key, transaction_time, quantity, amount, discount)
            SELECT 
                TGT.SALES_B_SEQUENCE.NEXTVAL,
                sales.sales_id,
                store.store_key,
                product.product_key,
                customer.customer_key,
                transaction_time,
                quantity,
                amount,
                NVL(discount, 0)
            FROM TMP.TMP_F_SALES sales
            LEFT JOIN TGT.DWH_D_STORE_LU store
                ON sales.store_id = store.store_id
            LEFT JOIN TGT.DWH_D_PRODUCT_LU product
                ON sales.product_id = product.product_id
            LEFT JOIN TGT.DWH_D_CUSTOMER_LU customer
                ON sales.customer_id = customer.customer_id;
        """)

        #aggregate table

        cur.execute("""
            INSERT INTO TGT.DWH_F_AGG_SLS_PLC_MONTH_T (store_id, month, amount, discount)
            SELECT 
                store_id,
                TO_CHAR(transaction_time, 'Mon') AS month,
                SUM(amount),
                SUM(NVL(discount, 0))
            FROM TMP_F_SALES
            GROUP BY store_id, month
        """)

        print(f'[Success] Loaded SALES to TGT.')
        Logger.log_info(f'[Success] Loaded SALES to TGT.')

        msg = f"Sales ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Sales : {e}')