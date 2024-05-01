from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nProduct ETL Started ...")
        Logger.log_info("Product ETL Started ...")

        # extract the product table to staging schema 
        load.load_to_stage(cur, "PRODUCT", "STG_D_PRODUCT_LU")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "PRODUCT", "TMP_D_PRODUCT_LU", "STG_D_PRODUCT_LU")

        # load the product table to target schema
        #get max value + 1 for surrogate key sequence
        res = cur.execute("""
            SELECT NVL(MAX(product_key), 0) FROM TGT.DWH_D_PRODUCT_LU
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.PRODUCT_SEQUENCE START WITH {start}"
        )

        # New record added
        cur.execute("""
            INSERT INTO TGT.DWH_D_PRODUCT_LU (product_key, product_id, subcategory_key, product_desc)
            SELECT TGT.PRODUCT_SEQUENCE.NEXTVAL, product_id, subcategory_key, product_desc
            FROM TMP.TMP_D_PRODUCT_LU r
            JOIN TGT.DWH_D_SUBCATEGORY_LU c
            ON r.subcategory_id = c.subcategory_id
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_PRODUCT_LU dest
                WHERE dest.product_id = r.product_id
            );
        """)

        # Record once removed added to the source again
        cur.execute("""
            UPDATE TGT.DWH_D_PRODUCT_LU dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('9999-12-31 23:59:59.999999999')
            WHERE EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_PRODUCT_LU src
                WHERE dest.product_id = src.product_id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute("""
            UPDATE TGT.DWH_D_PRODUCT_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_PRODUCT_LU src
                WHERE src.product_id = dest.product_id
            );
        """)

        # Minor change
        cur.execute("""
            UPDATE TGT.DWH_D_PRODUCT_LU dest
            SET product_desc = src.product_desc
            FROM TMP.TMP_D_PRODUCT_LU src
            WHERE dest.product_id = src.product_id
            AND dest.product_desc != src.product_desc;
        """)

        # Major change
        cur.execute("""
            UPDATE TGT.DWH_D_PRODUCT_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE EXISTS (
                SELECT NULL
                FROM (
                    SELECT product_id, product_desc, subcategory_key
                    FROM TMP.TMP_D_PRODUCT_LU product
                    JOIN TGT.DWH_D_SUBCATEGORY_LU subcategory
                    ON product.subcategory_id = subcategory.subcategory_id
                ) src
                WHERE dest.product_id = src.product_id AND dest.subcategory_key != src.subcategory_key
            );
        """)

        cur.execute("""
            INSERT INTO TGT.DWH_D_PRODUCT_LU(product_key, product_id, product_desc, subcategory_key)
            SELECT TGT.PRODUCT_SEQUENCE.NEXTVAL, product_id, product_desc, subcategory_key
            FROM (
                SELECT product_id, product_desc, subcategory_key
                FROM TMP.TMP_D_PRODUCT_LU product
                JOIN TGT.DWH_D_SUBCATEGORY_LU subcategory
                ON product.subcategory_id = subcategory.subcategory_id
            ) src
            WHERE EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_PRODUCT_LU dest
                WHERE dest.product_id = src.product_id AND dest.subcategory_key != src.subcategory_key AND active_flag = TRUE
            );
        """)

        print(f'[Success] Loaded PRODUCT to TGT.')
        Logger.log_info(f'[Success] Loaded PRODUCT to TGT.')

        ##############################################################################
        

        msg = f"Product ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Product :{e}')