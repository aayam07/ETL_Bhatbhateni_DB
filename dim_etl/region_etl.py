from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nRegion ETL Started ...")
        Logger.log_info("Region ETL Started ...")

        # extract the region table to staging schema 
        load.load_to_stage(cur, "REGION", "STG_D_REGION_LU")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "REGION", "TMP_D_REGION_LU", "STG_D_REGION_LU")

        # load the region table to target schema

        #get max value + 1 for surrogate key sequence
        res = cur.execute("""
            SELECT NVL(MAX(region_key), 0) FROM TGT.DWH_D_REGION_LU
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.REGION_SEQUENCE START WITH {start}"
        )

        # New record added
        cur.execute("""
            INSERT INTO TGT.DWH_D_REGION_LU (region_key, region_id, country_key, region_desc)
            SELECT TGT.REGION_SEQUENCE.NEXTVAL, region_id, country_key, region_desc
            FROM TMP.TMP_D_REGION_LU r
            JOIN TGT.DWH_D_COUNTRY_LU c
            ON r.country_id = c.country_id
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_REGION_LU dest
                WHERE dest.region_id = r.region_id
            );
        """)

        # Record once removed added to the source again
        cur.execute("""
            UPDATE TGT.DWH_D_REGION_LU dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('9999-12-31 23:59:59.999999999')
            WHERE EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_REGION_LU src
                WHERE dest.region_id = src.region_id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute("""
            UPDATE TGT.DWH_D_REGION_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_REGION_LU src
                WHERE src.region_id = dest.region_id
            );
        """)

        # Minor change
        cur.execute("""
            UPDATE TGT.DWH_D_REGION_LU dest
            SET region_desc = src.region_desc
            FROM TMP.TMP_D_REGION_LU src
            WHERE dest.region_id = src.region_id
            AND dest.region_desc != src.region_desc;
        """)

        # Major change
        cur.execute("""
            UPDATE TGT.DWH_D_REGION_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE EXISTS (
                SELECT NULL
                FROM (
                    SELECT region_id, region_desc, country_key
                    FROM TMP.TMP_D_REGION_LU region
                    JOIN TGT.DWH_D_COUNTRY_LU country
                    ON region.country_id = country.country_id
                ) src
                WHERE dest.region_id = src.region_id AND dest.country_key != src.country_key
            );
        """)

        cur.execute("""
            INSERT INTO TGT.DWH_D_REGION_LU(region_key, region_id, region_desc, country_key)
            SELECT TGT.REGION_SEQUENCE.NEXTVAL, region_id, region_desc, country_key
            FROM (
                SELECT region_id, region_desc, country_key
                FROM TMP.TMP_D_REGION_LU region
                JOIN TGT.DWH_D_COUNTRY_LU country
                ON region.country_id = country.country_id
            ) src
            WHERE EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_REGION_LU dest
                WHERE dest.region_id = src.region_id AND dest.country_key != src.country_key AND active_flag = TRUE
            );
        """)

        print(f'[Success] Loaded REGION to TGT.')
        Logger.log_info(f'[Success] Loaded REGION to TGT.')

        ##############################################################################
        
        msg = f"Region ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Region : {e}')