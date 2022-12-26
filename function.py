from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os
from airflow.models import Variable
from google.protobuf import duration_pb2

class Func:
    def __init__(self):
        # Get variables from json config 
        self.CRYPTOCURRENCY = "{{params.cryptocurrency}}"
        self.FREQUENCY = "{{params.frequency_partition}}"
        self.YEAR = "{{params.select_year}}"
        
        # Get variable from airflow environment 
        self.PROJECT_ID = Variable.get("projectid")
        self.JSON_KEY = os.environ.get("key")
        
        # Create client 
        self.STORAGE_CLIENT = storage.Client(self.JSON_KEY)
        self.BIGQUERY_CLIENT = bigquery.Client(self.JSON_KEY)

        #Define variables
        self.BUCKET_NAME = "first_dataset_btctousd"
        self.DATASET_NAME = "dataset"
        self.PARTITION_DATASET = "partitioned_views"
        
        # Define dataproc variables
        self.CLUSTER_NAME = "airflow-cluster"
        self.REGION = "us-central1"
        self.PYSPARK_URI = "gs://airflow-spark-process/sparkprocess.py"

        # Define cluster detail
        self.CLUSTER_CONFIG = {
            
            # Set compute engine
            "master_config": {
                
                # Number of VM instance
                "num_instances": 1,
                
                # Machine type
                "machine_type_uri": "n1-standard-2",
                
                # Define disk config
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
            },
            
            # Set compute engine for worker instance
            "worker_config": {
                
                # Number of VM instance
                "num_instances": 2,
                
                # Machine type
                "machine_type_uri": "n1-standard-2",
                
                # Disk config
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
            },
            
            # Define cluster life cycle
            "lifecycle_config" : {
                
                # Delete cluster if idle more than 15 mins 
                "idle_delete_ttl": duration_pb2.Duration().FromSeconds(900)
            }
        }

        # Define spark job detail
        self.PYSPARK_JOB = {
            "reference": {"project_id": self.PROJECT_ID},
            "placement": {"cluster_name": self.CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": self.PYSPARK_URI, 
                            
                            # Define jar file
                            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
                            
                            # Pass variables to spark job
                            "args": [f"--crypto={self.CRYPTOCURRENCY}", 
                                     f"--bucket={self.BUCKET_NAME}", 
                                     f"--dataset={self.DATASET_NAME}"]
                            }
        }
                
    # Create partition views from GBQ data
    def create_partition_view(self, PROJECT_ID:str, DATASET_NAME:str, BIGQUERY_CLIENT, PARTITION_DATASET:str, FREQUENCY: str, CRYPTOCURRENCY:str, YEAR): 
        
        # Capitalize variables      
        CRYPTOCURRENCY = CRYPTOCURRENCY.upper()
        FREQUENCY = FREQUENCY.upper()     
        
        # Cast string to list by eval (built-in function)
        YEAR = eval(YEAR)  
        
        # Define source id
        source_id = f"{PROJECT_ID}.{DATASET_NAME}.{CRYPTOCURRENCY}"
        
        """Extract years from external input"""
        # If year is empty list means select all year in the table
        if YEAR == []:
            
            # SQL statement : query all year from Date column 
            all_year_query = f"""SELECT DISTINCT EXTRACT(YEAR FROM Date) as Y from {DATASET_NAME}.{CRYPTOCURRENCY}"""
            
            # Query data from GBQ using above SQL statement and cast to list
            all_year = BIGQUERY_CLIENT.query(all_year_query).to_dataframe()['Y'].tolist()       
            print(f"select all {all_year}, {type(all_year)}")
        
        # If select year ex. ][2021, [2021, 2022] etc.
        else:
            
            # Define variables
            all_year = YEAR
            print(f"selected years {all_year}, {type(all_year)}")
        
        """Create views following conditions from external inputs"""    
        # Iterate over each selected years
        for year in all_year:
            print(year)
            
            # Define start, end variables for using if frequency is QUARTER
            start, end = 1,3
            
            # If select Year
            if FREQUENCY == "YEAR":
                
                    # Define view id
                    view_id = f"{PROJECT_ID}.{PARTITION_DATASET}.{CRYPTOCURRENCY}_{year}"
                    view = bigquery.Table(view_id) 
                    
                    # Create view from query          
                    view.view_query = f"""SELECT * FROM `{source_id}` WHERE EXTRACT({FREQUENCY} FROM Date) = {year} """
                    view = BIGQUERY_CLIENT.create_table(view)
            
            # If select Month and Quarter
            elif FREQUENCY in ("MONTH", "QUARTER"):
                
                # SQL statement : query unique months
                query_freq = f"""SELECT DISTINCT EXTRACT(MONTH FROM Date) as M from {DATASET_NAME}.{CRYPTOCURRENCY}"""  
                
                # Query data from GBQ using above SQL statement and cast to list
                period = BIGQUERY_CLIENT.query(query_freq).to_dataframe()['M'].tolist()
                
                max_month = max(period)
                
                # Iterate over period list
                for freq in period:
                    
                    # If select Quarter
                    if FREQUENCY == "QUARTER":
                        
                        # Break this loop when end greater than 12
                        if end > max_month:
                            break
                        
                        # Define view id
                        view_id = f"{PROJECT_ID}.{PARTITION_DATASET}.{CRYPTOCURRENCY}_{year}_{start}_{end}"
                        view = bigquery.Table(view_id)           
                        
                        # Query view 
                        view.view_query = f"""SELECT * FROM `{source_id}` WHERE EXTRACT(MONTH FROM Date) >= {start} AND EXTRACT(MONTH FROM Date) <= {end}
                                            AND EXTRACT(YEAR FROM Date) = {year}"""  
                                            
                        # Increase variables for counting month in each quarter          
                        start, end = start + 3, end + 3
                        
                    # If select Month    
                    elif FREQUENCY == "MONTH":
                        print(all_year, year)
                        
                        # Define view id
                        view_id = f"{PROJECT_ID}.{PARTITION_DATASET}.{CRYPTOCURRENCY}_{year}_{freq}"
                        view = bigquery.Table(view_id)           
                        
                        # Query view
                        view.view_query = f"""SELECT * FROM `{source_id}` WHERE EXTRACT({FREQUENCY} FROM Date) = {freq} 
                                            AND EXTRACT(YEAR FROM Date) = {year}"""
                    
                    # Create view                                                                            
                    view = BIGQUERY_CLIENT.create_table(view)
                    
    
    # Clear all datasets in GBQ  
    def clear_tables_views(self, BIGQUERY_CLIENT, DATASET_NAME, PARTITION_DATASET, PROJECT_ID):
        
        # List datasets
        datasets = list(BIGQUERY_CLIENT.list_datasets())  

        # If datasets in the project exist
        if datasets:           
            
            # Iterate over datasets in the bucket
            for dataset in datasets:
                
                # Define dataset name
                dataset_name = f"{PROJECT_ID}.{dataset.dataset_id}"
                
                # Delete dataset by dataset name
                BIGQUERY_CLIENT.delete_dataset(
                dataset_name, delete_contents=True, not_found_ok=True
                )
                print(f"delete {dataset.dataset_id}")
                
        # If datasets doesn't exist
        else:
            
            # Print comment
            print("does not contain any datasets.")



