import connection.snowflake_connector as conn
import utils.file_stage as fs
from fact_etl import sales_etl
from dim_etl import country_etl, category_etl, customer_etl, product_etl, region_etl, store_etl, subcategory_etl
from ddl import country_ddl, category_ddl, customer_ddl, product_ddl, region_ddl, store_ddl, subcategory_ddl, sales_ddl


if __name__=='__main__':

    cur = conn.connection()  # returns a cursor which is used to execute queries

    cur.execute("CREATE DATABAsE AAYAMDWH_DB")
    cur.execute("USE DATABASE AAYAMDWH_DB")
    cur.execute("create or replace schema STG")
    cur.execute("create or replace schema TMP")
    cur.execute("create or replace schema TGT")
    fs.file_stage(cur)  # to create stage file in Snwoflake

    # ddl
    category_ddl.create_tables(cur)
    country_ddl.create_tables(cur)
    customer_ddl.create_tables(cur)
    subcategory_ddl.create_tables(cur)
    product_ddl.create_tables(cur)
    region_ddl.create_tables(cur)
    store_ddl.create_tables(cur)
    sales_ddl.create_tables(cur)

    # etl
    category_etl.execute_etl(cur)
    country_etl.execute_etl(cur)
    customer_etl.execute_etl(cur)
    subcategory_etl.execute_etl(cur)
    product_etl.execute_etl(cur)
    region_etl.execute_etl(cur)
    store_etl.execute_etl(cur)
    sales_etl.execute_etl(cur)

    cur.close()  # close the cursor object