import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import datetime
#Get S3 hook
hook=S3Hook('aws_connection')

def stage_tables(key,data,bucketname):


    # #Convert the json file into a dataframe
    staging_data=pd.read_json(data)
    csv_string = staging_data.to_csv(index=False)
    #laod the data into an S3 bucket
    hook.load_string(
        string_data=csv_string,
        key=key,
        bucket_name=bucketname,
        replace=True

    )

def upload_to_database(key,table,s3_bucket,primary_key):
    print(key)

    df=pd.read_csv(StringIO(hook.read_key(key, s3_bucket)))
    print(df.head(5))
    print('\n')
    columns=list(df.columns)
    # Define the insert/update SQL statement
    insert_sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))}) ON CONFLICT ({primary_key}) DO UPDATE SET {','.join([f'{col}=EXCLUDED.{col}' for col in columns if col != {primary_key}])}"
    print(insert_sql)
    postgres_hook = PostgresHook(postgres_conn_id='postgres')

    values = [tuple(row) for row in df.values]

    
    for value in values:
        postgres_hook.run(insert_sql, parameters=value)

