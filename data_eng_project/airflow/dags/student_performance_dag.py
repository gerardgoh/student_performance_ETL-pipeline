from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator #defining workflow tasks
import sys
import os
import pandas as pd

sys.path.append('/Users/gerardgoh/data_eng_project')

from student_etl import extract_data, transform_data, validate_data, load_data
from s3_operations import upload_to_s3

#parameters for DAG
default = {
    'owner': 'gerard',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#create DAG object
dag = DAG(
    'student_performance_pipeline',
    default_args=default,
    description="ETL pipeline for student performance data",
    schedule=timedelta(days=1), #run daily
    start_date=datetime(2025, 3, 10),
    catchup=False,
)

input_path = '/Users/gerardgoh/data_eng_project/data/StudentsPerformance.csv'
output_path = '/Users/gerardgoh/data_eng_project/data/student_performance_processed.csv'
bucket_name = 'studentperformance-gerard'
s3_key = 'data/student_performance.csv'

#extraction
def extract_task(**kwargs):
    """Extract data from source file."""
    print(f"Extracting data from {input_path}")
    df = extract_data(input_path)
    #store data for next steps using xcom
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    return f'Extracted {len(df)} records'

#transform
def transform_task(**kwargs):
    """Transform the extracted data."""
    ti = kwargs['ti']
    # get data from previous task
    df_json = ti.xcom_pull(task_ids='extract', key='raw_data')
    df = pd.read_json(df_json)
    
    print("Transforming data")
    transformed_df = transform_data(df)
    
    # move transformed data to the validation stage
    ti.xcom_push(key='transformed_data', value=transformed_df.to_json())
    return f"Transformed data. New shape: {transformed_df.shape}"

#validation
def validate_task(**kwargs):
    """Validate data quality."""
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='transform', key='transformed_data')
    df = pd.read_json(df_json)
    
    print("Validating data")
    validation_results = validate_data(df)
    
    # Check if validation passed
    if not validation_results.get('validation_passed', False):
        raise ValueError("Data validation failed. Pipeline stopping.")
    
    return f"Data validation passed: {validation_results}"

def load_local_task(**kwargs):
    """Load data to local storage."""
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='transform', key='transformed_data')
    df = pd.read_json(df_json)
    
    print(f"Loading data to local file: {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    
    return f"Data saved to {output_path}"

def load_s3_task(**kwargs):
    """Upload data to S3 bucket."""
    import boto3
    from botocore.config import Config
    from io import StringIO
    
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='transform', key='transformed_data')
    
    try:
        if df_json is None:
            print("WARNING: No data received from transform task!")
            print("Falling back to reading from local file...")
            df = pd.read_csv(output_path)
        else:
            df = pd.read_json(df_json)
        
        print(f"DataFrame loaded, shape: {df.shape}")
        print(f"Uploading data to S3: {bucket_name}/{s3_key}")
        
        # Create CSV buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        boto3_config = Config(
            region_name='ap-southeast-2',  
            signature_version='s3v4'
        )
        
        s3_client = boto3.client('s3', config=boto3_config)
        
        # Upload to S3
        print("Starting S3 upload...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print("S3 upload completed successfully")
        
        return f"Data uploaded to s3://{bucket_name}/{s3_key}"
    except Exception as e:
        print(f"S3 upload error: {e}")
        import traceback
        traceback.print_exc()
        raise

#create airflow operators to execute task functions
extract_op = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform_op =PythonOperator(
    task_id="transform",
    python_callable=transform_task,
    dag=dag,
)

validate_op = PythonOperator(
    task_id="validate",
    python_callable=validate_task,
    dag=dag,
)

load_local_op = PythonOperator(
    task_id='load_local',
    python_callable=load_local_task,
    dag=dag,
)

load_s3_op = PythonOperator(
    task_id='load_s3',
    python_callable=load_s3_task,
    dag=dag,
)

extract_op >> transform_op >> validate_op >> [load_local_op, load_s3_op]
