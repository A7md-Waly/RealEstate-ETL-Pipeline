from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from hdfs import InsecureClient
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# File paths
INPUT_FILE = '/opt/airflow/inputData/Real_Estate_Sales.csv'
OUTPUT_FILE = '/opt/airflow/outputData/Real_Estate_Sales_Cleaned.csv'
HDFS_PATH = '/user/hive/warehouse/real_estate/real_estate_sales.csv'

def extract_data(**kwargs):
    """Extract data from CSV file"""
    print("Starting data extraction...")
    
    df = pd.read_csv(INPUT_FILE)
    
    print(f"Extracted {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")
    
    # Save to XCom for next task
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    
    return f"Extracted {len(df)} rows successfully"


def transform_data(**kwargs):
    """Transform data: drop columns, remove nulls, add SaleID"""
    print("Starting data transformation...")
    
    # Pull data from previous task
    ti = kwargs['ti']
    raw_data_json = ti.xcom_pull(key='raw_data', task_ids='Extract_Data')
    
    if not raw_data_json:
        raise ValueError("No data received from Extract_Data task via XCom")
    
    df = pd.read_json(raw_data_json)
    print(f"Initial shape: {df.shape}")
    
    # Drop 'Residential Type' column if exists
    if 'Residential Type' in df.columns:
        df = df.drop(columns=['Residential Type'])
        print("Dropped 'Residential Type' column")
    
    # Drop null values
    initial_count = len(df)
    df = df.dropna()
    print(f"Dropped {initial_count - len(df)} rows with null values")
    
    # Add SaleID column
    df.insert(0, 'SaleID', range(1, len(df) + 1))
    print("Added SaleID column")
    
    print(f"Final shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"\nFirst 5 rows:\n{df.head()}")
    
    # Save transformed data
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved transformed data to {OUTPUT_FILE}")
    
    ti.xcom_push(key='transformed_data', value=df.to_json())
    
    return f"Transformed {len(df)} rows successfully"


def load_to_hdfs(**kwargs):
    """Load transformed data to HDFS"""
    print("Starting data load to HDFS...")
    
    hdfs_client = InsecureClient('http://namenode:9870', user='root')
    print(f"Uploading {OUTPUT_FILE} to HDFS path {HDFS_PATH}")
    
    hdfs_dir = os.path.dirname(HDFS_PATH)
    try:
        hdfs_client.makedirs(hdfs_dir)
        print(f"Created directory: {hdfs_dir}")
    except Exception as e:
        print(f"Directory {hdfs_dir} may already exist: {e}")
    
    try:
        with open(OUTPUT_FILE, 'rb') as f:
            hdfs_client.write(HDFS_PATH, f, overwrite=True)
        print(f"Successfully uploaded to HDFS: {HDFS_PATH}")
        
        file_status = hdfs_client.status(HDFS_PATH)
        print(f"File size in HDFS: {file_status['length']} bytes")
    except Exception as e:
        print(f"Error uploading to HDFS: {e}")
        raise
    
    return f"Loaded data to HDFS: {HDFS_PATH}"


# Create DAG
with DAG(
    'real_estate_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Real Estate Sales data',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'real_estate', 'hive'],
) as dag:
    
    # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='Extract_Data',
        python_callable=extract_data,
    )
    
    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='Transform_Data',
        python_callable=transform_data,
    )
    
    # Task 3: Load to HDFS
    load_hdfs_task = PythonOperator(
        task_id='Load_To_HDFS',
        python_callable=load_to_hdfs,
    )
    
    # Task 4: Create Hive table
    create_hive_table_task = BashOperator(
        task_id='Create_Hive_Table',
        bash_command="""
        cat > /tmp/create_table.sql << 'SQL'
CREATE TABLE IF NOT EXISTS real_estate_sales (
    SaleID INT,
    List_Year INT,
    Town STRING,
    Address STRING,
    Assessed_Value DOUBLE,
    Sale_Amount DOUBLE,
    Sales_Ratio DOUBLE,
    Property_Type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/real_estate/'
TBLPROPERTIES ('skip.header.line.count'='1');
SQL

        docker cp /tmp/create_table.sql hiveserver2:/tmp/create_table.sql
        docker exec hiveserver2 /opt/hive/bin/beeline \
            -u jdbc:hive2://localhost:10000 \
            -f /tmp/create_table.sql --silent=true
        echo "Hive table created successfully"
        """
    )
    
    # Task 5: Load data into Hive
    load_hive_table_task = BashOperator(
        task_id='Load_Hive_Table',
        bash_command="""
        cat > /tmp/load_data.sql << 'SQL'
LOAD DATA INPATH '/user/hive/warehouse/real_estate/real_estate_sales.csv' 
INTO TABLE real_estate_sales;

SELECT COUNT(*) as total_rows FROM real_estate_sales;
SQL

        docker cp /tmp/load_data.sql hiveserver2:/tmp/load_data.sql
        docker exec hiveserver2 /opt/hive/bin/beeline \
            -u jdbc:hive2://localhost:10000 \
            -f /tmp/load_data.sql
        echo "Data loaded into Hive table successfully"
        """
    )
    
    # DAG dependencies
    extract_task >> transform_task >> load_hdfs_task >> create_hive_table_task >> load_hive_table_task
