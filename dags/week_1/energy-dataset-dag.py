from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API

@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule_interval="@daily",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2021, 1, 1),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args={
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['example']) # If set, this tag is shown in the DAG view of the Airflow UI
def energy_dataset_dag():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using two simple tasks to extract data from a zipped folder
    and load it to GCS.

    """

    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned.

        """
        from zipfile import ZipFile
        # TODO Unzip files into pandas dataframes

        # energy_zip_files = ZipFile('dags/data/energy-consumption-generation-prices-and-weather.zip')
        # energy_dfs = {text_file.filename: pd.read_csv(energy_zip_files.open(text_file.filename))
        #     for text_file in energy_zip_files.infolist()
        #     if text_file.filename.endswith('.csv')}

        with ZipFile('dags/data/energy-consumption-generation-prices-and-weather.zip', 'r') as z:
            energy_files = [f for f in z.namelist() if f.endswith('.csv')]
            energy_dfs = []
            for file in energy_files:
                df = pd.read_csv(z.open(file))
                energy_dfs.append(df)
        return energy_dfs


    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "transform" task, prints out the 
        schema, and then writes the data into GCS as parquet files.
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq

        data_types = ['generation', 'weather']

        # GCSHook uses google_cloud_default connection by default, so we can easily create a GCS client using it
        # https://github.com/apache/airflow/blob/207f65b542a8aa212f04a9d252762643cfd67a74/airflow/providers/google/cloud/hooks/gcs.py#L133

        # The google cloud storage github repo has a helpful example for writing from pandas to GCS:
        # https://github.com/googleapis/python-storage/blob/main/samples/snippets/storage_fileio_pandas.py
        
        client = GCSHook()     
        # TODO Add GCS upload code
        from google.cloud import storage

        storage_client = storage.Client()
        bucket = storage_client.bucket('corise_airflow')
        # blob = bucket.blob(blob_name)

        for i, df in enumerate(unzip_result):
            print(f'Schema of dataframe {i}:')
            print(df.dtypes)

            parquet_buffer = io.BytesIO()
            # df_para_table = pa.Table.from_pandas(df)
            pq.write_table(pa.Table.from_pandas(df), parquet_buffer)

            blob = bucket.blob(data_types[i] + f'_df_{i}.parquet')
            blob.upload_from_string(parquet_buffer.getvalue(), content_type='application/parquet')

        # print(f"Wrote csv with pandas with name {blob_name} from bucket {bucket.name}.")
        return 'Wrote Parquet files to GCP'



    # TODO Add task linking logic here
    # unzip_data = extract()
    # extract()
    unzip_data = extract()
    # order_summary = transform(unzip_data)
    load(unzip_data)


energy_dataset_dag = energy_dataset_dag()