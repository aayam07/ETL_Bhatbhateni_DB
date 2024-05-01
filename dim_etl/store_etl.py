from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nStore ETL Started ...")
        Logger.log_info("Store ETL Started ...")

        # extract the store table to staging schema 
        load.load_to_stage(cur, "STORE", "STG_D_STORE_LU")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "STORE", "TMP_D_STORE_LU", "STG_D_STORE_LU")
        # other temp transformation stuffs here

        # load the store table to target schema
        #get max value + 1 for surrogate key sequence
        res = cur.execute("""
            SELECT NVL(MAX(store_key), 0) FROM TGT.DWH_D_STORE_LU
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.STORE_SEQUENCE START WITH {start}"
        )

        # New record added
        cur.execute("""
            INSERT INTO TGT.DWH_D_STORE_LU (store_key, store_id, region_key, store_desc)
            SELECT TGT.STORE_SEQUENCE.NEXTVAL, store_id, region_key, store_desc
            FROM TMP.TMP_D_STORE_LU r
            JOIN TGT.DWH_D_REGION_LU c
            ON r.region_id = c.region_id
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_STORE_LU dest
                WHERE dest.store_id = r.store_id
            );
        """)

        # Record once removed added to the source again
        cur.execute("""
            UPDATE TGT.DWH_D_STORE_LU dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('9999-12-31 23:59:59.999999999')
            WHERE EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_STORE_LU src
                WHERE dest.store_id = src.store_id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute("""
            UPDATE TGT.DWH_D_STORE_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_STORE_LU src
                WHERE src.store_id = dest.store_id
            );
        """)

        # Minor change
        cur.execute("""
            UPDATE TGT.DWH_D_STORE_LU dest
            SET store_desc = src.store_desc
            FROM TMP.TMP_D_STORE_LU src
            WHERE dest.store_id = src.store_id
            AND dest.store_desc != src.store_desc;
        """)

        # Major change
        cur.execute("""
            UPDATE TGT.DWH_D_STORE_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE EXISTS (
                SELECT NULL
                FROM (
                    SELECT store_id, store_desc, region_key
                    FROM TMP.TMP_D_STORE_LU store
                    JOIN TGT.DWH_D_REGION_LU region
                    ON store.region_id = region.region_id
                ) src
                WHERE dest.store_id = src.store_id AND dest.region_key != src.region_key
            );
        """)

        cur.execute("""
            INSERT INTO TGT.DWH_D_STORE_LU(store_key, store_id, store_desc, region_key)
            SELECT TGT.STORE_SEQUENCE.NEXTVAL, store_id, store_desc, region_key
            FROM (
                SELECT store_id, store_desc, region_key
                FROM TMP.TMP_D_STORE_LU store
                JOIN TGT.DWH_D_REGION_LU region
                ON store.region_id = region.region_id
            ) src
            WHERE EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_STORE_LU dest
                WHERE dest.store_id = src.store_id AND dest.region_key != src.region_key AND active_flag = TRUE
            );
        """)

        print(f'[Success] Loaded STORE to TGT.')
        Logger.log_info(f'[Success] Loaded STORE to TGT.')

        ##############################################################################
        

        msg = f"Store ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Store : {e}')