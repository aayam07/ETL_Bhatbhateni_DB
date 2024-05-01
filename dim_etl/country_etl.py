from loading import load
import utils.logger as Logger

def execute_etl(cur):
    try:
        print("\nCountry ETL Started ...")
        Logger.log_info("Country ETL Started ...")

        # extract the country table to staging schema 
        load.load_to_stage(cur,"COUNTRY", "STG_D_COUNTRY_LU")
        
        # load the table to temporary schema
        load.load_to_temp(cur, "COUNTRY", "TMP_D_COUNTRY_LU", "STG_D_COUNTRY_LU")

        # load the country table to target schema
        #get max value + 1 for surrogate key sequence
        res = cur.execute("""
            SELECT NVL(MAX(country_key), 0) FROM TGT.DWH_D_COUNTRY_LU
        """
        )
        start = res.fetchone()[0] + 1
        cur.execute(
            f"CREATE OR REPLACE SEQUENCE TGT.COUNTRY_SEQUENCE START WITH {start}"
        )

        # New record added
        cur.execute("""
            INSERT INTO TGT.DWH_D_COUNTRY_LU (country_key, country_id, country_desc)
            SELECT TGT.COUNTRY_SEQUENCE.NEXTVAL, country_id, country_desc
            FROM TMP.TMP_D_COUNTRY_LU src
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TGT.DWH_D_COUNTRY_LU dest
                WHERE dest.country_id = src.country_id
            );
         """)

        # Record once removed added to the source again
        cur.execute("""
            UPDATE TGT.DWH_D_COUNTRY_LU dest
            SET active_flag = TRUE,
            CREATED_TS = CURRENT_TIMESTAMP(),
            UPDATED_TS = TO_TIMESTAMP('9999-12-31 23:59:59.99')
            WHERE EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_COUNTRY_LU src
                WHERE dest.country_id = src.country_id AND dest.active_flag = FALSE
            );
        """)

        # Record removed from source
        cur.execute("""
            UPDATE TGT.DWH_D_COUNTRY_LU dest
            SET active_flag = FALSE,
            UPDATED_TS = CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT NULL
                FROM TMP.TMP_D_COUNTRY_LU src
                WHERE src.country_id = dest.country_id
            );
        """)

        # Minor change
        cur.execute("""
            UPDATE TGT.DWH_D_COUNTRY_LU dest
            SET country_desc = src.country_desc
            FROM TMP.TMP_D_COUNTRY_LU src
            WHERE dest.country_id = src.country_id
            AND dest.country_desc != src.country_desc;
        """)

        print(f'[Success] Loaded COUNTRY to TGT.')
        Logger.log_info(f'[Success] Loaded COUNTRY to TGT.')

        ##############################################################################

        msg = f"Country ETL Completed.\n"
        print(msg)
        Logger.log_info(msg)

    except Exception as e:
        Logger.log_error(f'[Error] Failed ETL Process for Country.{e}')