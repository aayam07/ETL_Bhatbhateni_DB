from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nSub-category ETL Started ...")
        Logger.log_info("Sub-category ETL Started ...")

        # extract the subcategory table to staging schema 
        load.load_to_stage(cur, "SUBCATEGORY", "STG_D_SUBCATEGORY_LU")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "SUBCATEGORY", "TMP_D_SUBCATEGORY_LU", "STG_D_SUBCATEGORY_LU")

        # load the subcategory table to target schema
        #get max value + 1 for surrogate key sequence
        res = cur.execute("""
            SELECT NVL(MAX(subcategory_key), 0) FROM TGT.DWH_D_SUBCATEGORY_LU
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.SUBCATEGORY_SEQUENCE START WITH {start}"
        )

        # New record added
        cur.execute("""
            INSERT INTO TGT.DWH_D_SUBCATEGORY_LU (subcategory_key, subcategory_id, category_key, subcategory_desc)
            SELECT TGT.SUBCATEGORY_SEQUENCE.NEXTVAL, subcategory_id, category_key, subcategory_desc
            FROM TMP.TMP_D_SUBCATEGORY_LU r
            JOIN TGT.DWH_D_CATEGORY_LU c
            ON r.category_id = c.category_id
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_SUBCATEGORY_LU dest
                WHERE dest.subcategory_id = r.subcategory_id
            );
        """)

        # Record once removed added to the source again
        cur.execute("""
            UPDATE TGT.DWH_D_SUBCATEGORY_LU dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('9999-12-31 23:59:59.999999999')
            WHERE EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_SUBCATEGORY_LU src
                WHERE dest.subcategory_id = src.subcategory_id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute("""
            UPDATE TGT.DWH_D_SUBCATEGORY_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_SUBCATEGORY_LU src
                WHERE src.subcategory_id = dest.subcategory_id
            );
        """)

        # Minor change
        cur.execute("""
            UPDATE TGT.DWH_D_SUBCATEGORY_LU dest
            SET subcategory_desc = src.subcategory_desc
            FROM TMP.TMP_D_SUBCATEGORY_LU src
            WHERE dest.subcategory_id = src.subcategory_id
            AND dest.subcategory_desc != src.subcategory_desc;
        """)

        # Major change
        cur.execute("""
            UPDATE TGT.DWH_D_SUBCATEGORY_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE EXISTS (
                SELECT NULL
                FROM (
                    SELECT subcategory_id, subcategory_desc, category_key
                    FROM TMP.TMP_D_SUBCATEGORY_LU subcategory
                    JOIN TGT.DWH_D_CATEGORY_LU category
                    ON subcategory.category_id = category.category_id
                ) src
                WHERE dest.subcategory_id = src.subcategory_id AND dest.category_key != src.category_key
            );
        """)

        cur.execute("""
            INSERT INTO TGT.DWH_D_SUBCATEGORY_LU(subcategory_key, subcategory_id, subcategory_desc, category_key)
            SELECT TGT.SUBCATEGORY_SEQUENCE.NEXTVAL, subcategory_id, subcategory_desc, category_key
            FROM (
                SELECT subcategory_id, subcategory_desc, category_key
                FROM TMP.TMP_D_SUBCATEGORY_LU subcategory
                JOIN TGT.DWH_D_CATEGORY_LU category
                ON subcategory.category_id = category.category_id
            ) src
            WHERE EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_SUBCATEGORY_LU dest
                WHERE dest.subcategory_id = src.subcategory_id AND dest.category_key != src.category_key AND active_flag = TRUE
            );
        """)

        print(f'[Success] Loaded SUB-CATEGORY to TGT.')
        Logger.log_info(f'[Success] Loaded SUB-CATEGORY to TGT.')

        ##############################################################################

        msg = f"Subcategory ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Subcategory : {e}')