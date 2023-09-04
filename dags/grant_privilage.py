import os
import snowflake.connector
from dotenv import load_dotenv


def grant_privilage_to_dbt_snow_user():
    # Load the .env file
    load_dotenv()

    #Get Snowflake connection from the environment variable
    SNOWFLAKE_ACCOUNT = os.environ.get('snowflake_account_name')
    SNOWFLAKE_USER = os.environ.get('snowflake_username')
    SNOWFLAKE_PASSWORD = os.environ.get('snowflake_password')


    conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
        )

    print("success in connecting")
    conn.cursor().execute("USE ROLE ACCOUNTADMIN")
    conn.cursor().execute("GRANT ALL ON DATABASE PC_DBT_DB TO ROLE PC_DBT_ROLE")
    conn.cursor().execute("CREATE OR REPLACE WAREHOUSE PC_DBT_WH WITH WAREHOUSE_SIZE = XSMALL")
    conn.cursor().execute("GRANT ALL ON  WAREHOUSE PC_DBT_WH TO ROLE PC_DBT_ROLE")
    #conn.cursor().execute("GRANT ALL ON SCHEMA PC_DBT_DB.GG_SCHOLAR TO ROLE PC_DBT_ROLE")
    conn.cursor().execute("GRANT CREATE VIEW ON SCHEMA PC_DBT_DB.GG_SCHOLAR TO ROLE PC_DBT_ROLE")
    conn.cursor().execute("GRANT CREATE TABLE ON SCHEMA PC_DBT_DB.GG_SCHOLAR TO ROLE PC_DBT_ROLE")
    conn.cursor().execute("GRANT USAGE ON SCHEMA PC_DBT_DB.GG_SCHOLAR TO ROLE PC_DBT_ROLE")
    conn.cursor().execute("GRANT SELECT ON FUTURE TABLES IN SCHEMA PC_DBT_DB.GG_SCHOLAR TO ROLE PC_DBT_ROLE")
    conn.cursor().execute("GRANT SELECT ON FUTURE VIEWS IN SCHEMA PC_DBT_DB.GG_SCHOLAR TO ROLE PC_DBT_ROLE")
    conn.cursor().execute("GRANT SELECT ON ALL TABLES IN SCHEMA PC_DBT_DB.GG_SCHOLAR TO ROLE PC_DBT_ROLE")
    

    print("Grant privilage sucessfully ")
    
    conn.close()

if __name__ == "__main__":
    grant_privilage_to_dbt_snow_user()



