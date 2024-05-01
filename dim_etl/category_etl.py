from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nCategory ETL Started ...")
        Logger.log_info("Category ETL Started ...")

        # extract the category table to staging schema 
        load.load_to_stage(cur, "CATEGORY", "STG_D_CATEGORY_LU")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "CATEGORY", "TMP_D_CATEGORY_LU", "STG_D_CATEGORY_LU")

        # load the category table to target schema
        #get max value + 1 for surrogate key sequence
        res = cur.execute("""
            SELECT NVL(MAX(category_key), 0) FROM TGT.DWH_D_CATEGORY_LU
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.CATEGORY_SEQUENCE START WITH {start}"
        )

        # New record added
        cur.execute("""
            INSERT INTO TGT.DWH_D_CATEGORY_LU (category_key, category_id, category_desc)
            SELECT TGT.CATEGORY_SEQUENCE.NEXTVAL, category_id, category_desc
            FROM TMP.TMP_D_CATEGORY_LU src
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_CATEGORY_LU dest
                WHERE dest.category_id = src.category_id
            );
         """)

        # Record once removed added to the source again
        cur.execute("""
            UPDATE TGT.DWH_D_CATEGORY_LU dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('9999-12-31 23:59:59.99')
            WHERE EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_CATEGORY_LU src
                WHERE dest.category_id = src.category_id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute("""
            UPDATE TGT.DWH_D_CATEGORY_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_CATEGORY_LU src
                WHERE src.category_id = dest.category_id
            );
        """)

        # Minor change
        cur.execute("""
            UPDATE TGT.DWH_D_CATEGORY_LU dest
            SET category_desc = src.category_desc
            FROM TMP.TMP_D_CATEGORY_LU src
            WHERE dest.category_id = src.category_id
            AND dest.category_desc != src.category_desc;
        """)

        print(f'[Success] Loaded CATEGORY to TGT.')
        Logger.log_info(f'[Success] Loaded CATEGORY to TGT.')
        
        ##############################################################################

        msg = f"Category ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Category: {e}')