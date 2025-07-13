create database snowflake_DB;

CREATE OR REPLACE SCHEMA BRONZE;

SHOW TABLES;

SHOW INTEGRATIONS;

list @s3_init;


-- create csv file format
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  PARSE_HEADER = TRUE                   -- note this step here.
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';


CREATE OR REPLACE STORAGE INTEGRATION DYNAMIC_INT
TYPE = EXTERNAL_STAGE
ENABLED=TRUE
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::552471838940:role/snowflake-datalake-connector' -- role arn 
STORAGE_ALLOWED_LOCATIONS =('s3://snowflake-datalake-gaurav/'); --bucket


DESC STORAGE INTEGRATION DYNAMIC_INT;


CREATE OR ALTER STAGE DYNAMICLOAD
FILE_FORMAT=my_csv_format
STORAGE_INTEGRATION=DYNAMIC_INT
URL='s3://snowflake-datalake-gaurav/'
DIRECTORY=(ENABLE=true
AUTO_REFRESH = TRUE) ;

DESC STAGE DYNAMICLOAD;

LIST @DYNAMICLOAD;

ALTER STAGE DYNAMICLOAD REFRESH;

SELECT RELATIVE_PATH FROM DIRECTORY(@DYNAMICLOAD);



--step 1 create audit table

CREATE OR REPLACE TABLE FILE_LOAD_LOG (
    FILE_NAME STRING,
    LOAD_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- drop table FILE_LOAD_LOG;

select * from FILE_LOAD_LOG;



--- Create proc to automate all this 


CREATE OR REPLACE PROCEDURE DYNAMIC_TABLE_LOAD_PY()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit

def main(session: Session) -> str:
    try:
        refresh_load = session.sql("ALTER STAGE DYNAMICLOAD REFRESH").collect()
        dir_df = session.sql("SELECT RELATIVE_PATH FROM DIRECTORY(@DYNAMICLOAD)").collect()
    
        for file in dir_df:
            file_path = file['RELATIVE_PATH']
            parts = file_path.split('/')
            if len(parts) != 2:
                continue  # Skip malformed paths
    
            folder_name, file_name = parts
            tablename = folder_name.upper()
            full_stage_path = f"@DYNAMICLOAD/{file_path}"
    
            # Check if file was already processed
            if session.table("FILE_LOAD_LOG").filter(col("FILE_NAME") == lit(file_path)).count() > 0:
                continue
    
            # Check if table exists
            check_sql = f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = CURRENT_SCHEMA() 
                AND TABLE_NAME = '{tablename}'
            """
            table_exists = session.sql(check_sql).collect()[0][0]
    
            if table_exists == 0:
                create_sql = f"""
                    CREATE OR REPLACE TABLE IDENTIFIER('{tablename}')
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '{full_stage_path}',
                                FILE_FORMAT => 'my_csv_format',
                                IGNORE_CASE => TRUE
                            )
                        )
                    )
                """
                session.sql(create_sql).collect()
    
            # Load the data
            copy_sql = f"""
                COPY INTO IDENTIFIER('{tablename}')
                FROM '{full_stage_path}'
                FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'SKIP_FILE'
            """
            session.sql(copy_sql).collect()
    
            # Insert into audit log
            session.sql(f"INSERT INTO FILE_LOAD_LOG (FILE_NAME) VALUES ('{file_path}')").collect()
        return '✅ Dynamic ingestion from S3 to Snowflake completed using Snowpark.'

    except Exception as e:
        return f"failed: {str(e)}"

$$;



CALL DYNAMIC_TABLE_LOAD_PY();


select * from hr;SNOWFLAKE_LEARNING_DB.BRONZE.HR


SHOW TABLES;

Select * from FILE_LOAD_LOG;
