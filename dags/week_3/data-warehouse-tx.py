from datetime import datetime

import pandas as pd
import re
from typing import List

from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task, task_group

PROJECT_ID = 'airflow-week-377305'
DESTINATION_BUCKET = 'corise_airflow'
BQ_DATASET_NAME = 'generation_weather'

def safe_name(s: str) -> str:
    """
    Remove invalid characters for filename
    """
    return re.sub("[^0-9a-zA-Z_]+", "_", s)

DATA_TYPES = ["generation", "weather"] 


normalized_columns = {
    "generation": {
        "time": "time",
        "columns": 
            [   
                "total_load_actual",
                "price_day_ahead",
                "price_actual",
                "generation_fossil_hard_coal",
                "generation_fossil_gas",
                "generation_fossil_brown_coal_lignite",
                "generation_fossil_oil",
                "generation_other_renewable",
                "generation_waste",
                "generation_biomass",
                "generation_other",
                "generation_solar",
                "generation_hydro_water_reservoir",
                "generation_nuclear",
                "generation_hydro_run_of_river_and_poundage",
                "generation_wind_onshore",
                "generation_hydro_pumped_storage_consumption"

            ]
        },
    "weather": {
        "time": "dt_iso",
        "columns": 
            [
                "city_name",
                "temp",
                "pressure",
                "humidity",
                "wind_speed",
                "wind_deg",
                "rain_1h",
                "rain_3h",
                "snow_3h",
                "clouds_all",
            ]
        }
    }


@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    ) 
def data_warehouse_transform_dag():
    """
    ### Data Warehouse Transform DAG
    This DAG performs four operations:
        1. Extracts zip file into two dataframes
        2. Loads these dataframes into parquet files on GCS, with valid column names
        3. Builds external tables on top of these parquet files
        4. Builds normalized views on top of the external tables
        5. Builds a joined view on top of the normalized views, joined on time
    """


    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned


        """
        from zipfile import ZipFile
        filename = "/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"
        # filename = "/Users/sjabbireddy/Desktop/Personal/corise-airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"
        dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in ZipFile(filename).namelist()]
        return dfs


    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "extract" task, formats
        columns to be BigQuery-compliant, and writes data to GCS.
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        
        client = GCSHook().get_conn()       
        bucket = client.get_bucket(DESTINATION_BUCKET)

        for index, df in enumerate(unzip_result):
            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.replace("/", "_")
            df.columns = df.columns.str.replace("-", "_")
            bucket.blob(f"week-3/{DATA_TYPES[index]}.parquet").upload_from_string(df.to_parquet(), "text/parquet")
            print(df.dtypes)

    @task_group
    def create_bigquery_dataset():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
        EmptyOperator(task_id='placeholder')
        # TODO Modify here to create a BigQueryDataset if one does not already exist
        # This is where your tables and views will be created
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_generation_weather_dataset',
            dataset_id=BQ_DATASET_NAME,
            project_id=PROJECT_ID,
            location='us-east1'
        )
        

    @task_group
    def get_bigquery_dataset():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
        EmptyOperator(task_id='placeholder')
        # TODO Modify here to create a BigQueryDataset if one does not already exist
        # This is where your tables and views will be created
        get_dataset = BigQueryGetDatasetOperator(task_id="create_energy_dataset", dataset_id=BQ_DATASET_NAME)

    @task_group
    def check_table_exists():
        from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
        check_table_exists_energy = BigQueryTableExistenceSensor(
            task_id="check_for_generation_table",
            project_id=PROJECT_ID,
            dataset_id=BQ_DATASET_NAME,
            table_id='energy_generation_table',
        )
        check_table_exists_weather = BigQueryTableExistenceSensor(
            task_id="check_for_weather_table",
            project_id=PROJECT_ID,
            dataset_id=BQ_DATASET_NAME,
            table_id='weather_table',
        )
    

    @task_group
    def create_external_tables():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryCreateEmptyTableOperator
        from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
        # EmptyOperator(task_id='placeholder')
        SOURCE_MULTIPLE_TYPES = ''

        # TODO Modify here to produce two external tables, one for each data type, referencing the data stored in GCS

        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is 
        # PARQUET.
        create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
            task_id="create_energy_table",
            
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET_NAME,
                    "tableId": "energy_generation_table",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{DESTINATION_BUCKET}/week-3/generation.parquet"],
                },                
                # "autodetect": True,
                # "schema_object": "/usr/local/airflow/dags/data/energy_prices_table_schema.json",
                "schema": {
                    "fields": [
                        {"name": "time", "type": "STRING"},
                        {"name": "generation_biomass", "type": "FLOAT"},
                        {"name": "generation_fossil_brown_coal_lignite", "type": "FLOAT"},
                        {"name": "generation_fossil_coal_derived_gas", "type": "FLOAT"},
                        {"name": "generation_fossil_gas", "type": "FLOAT"},
                        {"name": "generation_fossil_hard_coal", "type": "FLOAT"},
                        {"name": "generation_fossil_oil", "type": "FLOAT"},
                        {"name": "generation_fossil_oil_shale", "type": "FLOAT"},
                        {"name": "generation_fossil_peat", "type": "FLOAT"},
                        {"name": "generation_geothermal", "type": "FLOAT"},
                        {"name": "generation_hydro_pumped_storage_aggregated", "type": "FLOAT"},
                        {"name": "generation_hydro_pumped_storage_consumption", "type": "FLOAT"},
                        {"name": "generation_hydro_run_of_river_and_poundage", "type": "FLOAT"},
                        {"name": "generation_hydro_water_reservoir", "type": "FLOAT"},
                        {"name": "generation_marine", "type": "FLOAT"},
                        {"name": "generation_nuclear", "type": "FLOAT"},
                        {"name": "generation_other", "type": "FLOAT"},
                        {"name": "generation_other_renewable", "type": "FLOAT"},
                        {"name": "generation_solar", "type": "FLOAT"},
                        {"name": "generation_waste", "type": "FLOAT"},
                        {"name": "generation_wind_onshore", "type": "FLOAT"},
                        {"name": "forecast_solar_day_ahead", "type": "FLOAT"},
                        {"name": "forecast_wind_offshore_eday_ahead", "type": "FLOAT"},
                        {"name": "forecast_wind_onshore_day_ahead", "type": "FLOAT"},
                        {"name": "total_load_forecast", "type": "FLOAT"},
                        {"name": "total_load_actual", "type": "FLOAT"},
                        {"name": "price_day_ahead", "type": "FLOAT"},
                        {"name": "price_actual", "type": "FLOAT"},
                    ]
                },
            },    
        )

        create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
            task_id="create_weather_table",
            # bucket=DESTINATION_BUCKET, #The name of the GCS bucket where the CSV file is stored.
            # source_objects='corise_airflow/week-3/weather.parquet',
            # autodetect=True,
            # source_objects='week-3/*', #The path to the CSV file within the GCS bucket.
            #dictionary that contains DDL configuration parameters for the table.
            
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET_NAME,
                    "tableId": "weather_table",
                },                
                # "autodetect": True,
                # "schema_object": "/usr/local/airflow/dags/data/energy_prices_table_schema.json",
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{DESTINATION_BUCKET}/week-3/weather.parquet"],
                },
                "schema": {
                    "fields": [
                        {"name": "dt_iso", "type": "STRING"},
                        {"name": "city_name", "type": "STRING"},
                        {"name": "temp", "type": "FLOAT"},
                        {"name": "pressure", "type": "INTEGER"},
                        {"name": "humidity", "type": "INTEGER"},
                        {"name": "wind_speed", "type": "INTEGER"},
                        {"name": "wind_deg", "type": "INTEGER"},
                        {"name": "rain_1h", "type": "FLOAT"},
                        {"name": "rain_3h", "type": "FLOAT"},
                        {"name": "snow_3h", "type": "FLOAT"},
                        {"name": "clouds_all", "type": "INTEGER"},
                    ]
                },
            },
            
        )
        


    def produce_select_statement(timestamp_column: str, columns: List[str]) -> str:
        # TODO Modify here to produce a select statement by casting 'timestamp_column' to 
        # TIMESTAMP type, and selecting all of the columns in 'columns'
        columns_str = ", ".join(columns)
        query = f'SELECT CAST({timestamp_column} AS TIMESTAMP) as {timestamp_column}, {columns_str} '
        return query

    @task_group
    def produce_normalized_views():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce views for each of the datasources, capturing only the essential
        # columns specified in normalized_columns. A key step at this stage is to convert the relevant 
        # columns in each datasource from string to time. The utility function 'produce_select_statement'
        # accepts the timestamp column, and essential columns for each of the datatypes and build a 
        # select statement programmatically, which can then be passed to the Airflow Operators.
        EmptyOperator(task_id='placeholder')

        # Define the task IDs for the two external tables
        table1_task_id = 'energy_table_task_id'
        table2_task_id = 'weather_table_task_id'

        # Define the SQL queries for the new normalized views
        sql_statements = {}
        for c in normalized_columns:
            sql_statements[c] = produce_select_statement(normalized_columns[c]['time'], normalized_columns[c]['columns']) # + f'from {c}_table'

        # Create the normalized view operators that depend on the external table tasks
        create_view1 = BigQueryCreateEmptyTableOperator(
            task_id='generation_task_id',
            dataset_id=BQ_DATASET_NAME,
            table_id="generation_view",
            view={
                "query": f"{sql_statements['generation']} from `{PROJECT_ID}.{BQ_DATASET_NAME}.energy_generation_table`",
                "useLegacySql": False,
            },
        )

        create_view2 = BigQueryCreateEmptyTableOperator(
            task_id='weather_task_id',
            dataset_id=BQ_DATASET_NAME,
            table_id="weather_view",
            view={
                "query": f"{sql_statements['weather']} from `{PROJECT_ID}.{BQ_DATASET_NAME}.weather_table`",
                "useLegacySql": False,
            }
        )



    @task_group
    def produce_joined_view():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce a view that joins the two normalized views on time
        EmptyOperator(task_id='placeholder2')
        generation_table_id="generation_view"
        weather_table_id="weather_view"
        joined_view_id = 'joined_view'

        # Create an empty table operator as a placeholder
        # placeholder = BigQueryCreateEmptyTableOperator(
        #     task_id='placeholder',
        #     dataset_id='your_dataset_id',
        #     table_id=joined_view_id,
        #     table_resource={}
        # )

        # Create the joined view operator that depends on the two normalized view tasks
        joined_view = BigQueryCreateEmptyTableOperator(
            task_id='joined_view',
            dataset_id=BQ_DATASET_NAME,
            table_id="joined_energy_weather_view",
            view={
                "query": f"""
                            SELECT
                                e.total_load_actual
                                ,e.price_day_ahead
                                ,e.price_actual
                                ,e.generation_fossil_hard_coal
                                ,e.generation_fossil_gas
                                ,e.generation_fossil_brown_coal_lignite
                                ,e.generation_fossil_oil
                                ,e.generation_other_renewable
                                ,e.generation_waste
                                ,e.generation_biomass
                                ,e.generation_other
                                ,e.generation_solar
                                ,e.generation_hydro_water_reservoir
                                ,e.generation_nuclear
                                ,e.generation_hydro_run_of_river_and_poundage
                                ,e.generation_wind_onshore
                                ,e.generation_hydro_pumped_storage_consumption,
                                w.*
                                FROM
                                `{PROJECT_ID}.{BQ_DATASET_NAME}.generation_view` AS e
                                INNER JOIN 
                                `{PROJECT_ID}.{BQ_DATASET_NAME}.weather_view` AS w
                                ON
                                w.dt_iso = e.time
                        """,
                "useLegacySql": False,
            }
        )


    unzip_task = extract()
    load_task = load(unzip_task)
    create_bigquery_dataset_task = create_bigquery_dataset()
    load_task >> create_bigquery_dataset_task
    
    get_bigquery_dataset_task = get_bigquery_dataset()
    create_bigquery_dataset_task >> get_bigquery_dataset_task

    # check_table_exists_task = check_table_exists()
    # get_bigquery_dataset_task >> check_table_exists_task

    external_table_task = create_external_tables()
    get_bigquery_dataset_task >> external_table_task

    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task

    joined_view_task = produce_joined_view()
    normal_view_task >> joined_view_task


data_warehouse_transform_dag = data_warehouse_transform_dag()