from function import Func as T
from airflow.models import BaseOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator, DataprocSubmitJobOperator
)  

t = T()

# Begin DAG
default_args={
    'owner':'Pang',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}

with DAG(
    dag_id = 'first_dag',
    default_args=default_args,
    description = 'GCP',
    start_date=days_ago(1), # Daily run
    schedule_interval='@once',

    # Set default config json arguments
    params = {
        "cryptocurrency" : "BTC",
        "frequency_partition" : "MONTH",
        "select_year" : []
    }
    
    ) as dag:
    
        # Startup task
        start = DummyOperator(
            task_id='start',
            dag=dag
        )
        
        # Clear datasets
        clear_data = PythonOperator(
            task_id='clear_datasets',
            python_callable=t.clear_tables_views,
            op_kwargs={'BIGQUERY_CLIENT':t.BIGQUERY_CLIENT, 'PARTITION_DATASET':t.PARTITION_DATASET, 
                       'DATASET_NAME':t.DATASET_NAME, 'PROJECT_ID':t.PROJECT_ID}
        ) 
        
        # Create dataset
        create_dataset = BigQueryCreateEmptyDatasetOperator(task_id='create_dataset', dataset_id=t.DATASET_NAME)
        
        # Create Dataproc cluster
        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=t.PROJECT_ID,
            cluster_config=t.CLUSTER_CONFIG,
            region=t.REGION,
            cluster_name=t.CLUSTER_NAME,
            use_if_exists=True
        )

        # Submit spark job
        submit_job = DataprocSubmitJobOperator(
            task_id="pyspark_task", 
            job=t.PYSPARK_JOB,  
            project_id=t.PROJECT_ID,
            region=t.REGION,

        )
            
        # # Create dataset for partitioned tables
        partition_dataset = BigQueryCreateEmptyDatasetOperator(task_id='create_partition_dataset', dataset_id=t.PARTITION_DATASET)
        
        # Create partition views
        partition_view = PythonOperator(
            task_id='create_partition_view',
            python_callable=t.create_partition_view,
            op_kwargs={'BIGQUERY_CLIENT':t.BIGQUERY_CLIENT, 'PARTITION_DATASET':t.PARTITION_DATASET, 
                       'DATASET_NAME':t.DATASET_NAME, 'PROJECT_ID':t.PROJECT_ID, 'FREQUENCY':t.FREQUENCY, 
                       'CRYPTOCURRENCY':t.CRYPTOCURRENCY, 'YEAR':t.YEAR}
        ) 
        
        start >> clear_data >> create_dataset >> create_cluster >> submit_job >> partition_dataset >> partition_view
        

        
        