import os
import snowflake.connector
from dotenv import load_dotenv


def stage_gg_scholar_author_metrics():
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
    conn.cursor().execute("USE ROLE SYSADMIN")

    conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS PC_DBT_WH")
    conn.cursor().execute("CREATE DATABASE IF NOT EXISTS PC_DBT_DB")
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS PC_DBT_DB.GG_SCHOLAR")

    conn.cursor().execute('USE WAREHOUSE PC_DBT_WH')
    conn.cursor().execute('USE DATABASE PC_DBT_DB')
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS GG_SCHOLAR")

    stage_name = "GG_SCHOLAR_CSV_STAGE"
    # AWS S3 credentials
    s3_bucket_name = 'mona-bucket-23'
    s3_file_path = 'gg_scholar_author_metrics.csv'
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


    # Create an S3 stage
    conn.cursor().execute(f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = 's3://{s3_bucket_name}/{s3_file_path}'
        CREDENTIALS = (AWS_KEY_ID='{aws_access_key_id}' AWS_SECRET_KEY='{aws_secret_access_key}')
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1);
    """)

    conn.cursor().execute('CREATE OR REPLACE TABLE PC_DBT_DB.GG_SCHOLAR.GG_SCHOLAR_METRICES ( AUTHOR_ID VARCHAR(12), YEAR VARCHAR(10), H_INDEX NUMBER(4,0), I10_INDEX NUMBER(4,0), CITATIONS NUMBER(10,0))')
    conn.cursor().execute(f"COPY INTO PC_DBT_DB.GG_SCHOLAR.GG_SCHOLAR_METRICES FROM @{stage_name}")

    print("stage gg_scholar_author_metrics sucessfully loaded")
    
    conn.close()

if __name__ == "__main__":
    stage_gg_scholar_author_metrics()



