
from datetime import datetime
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import xgboost as xgb
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.models.dag import DAG
from airflow.decorators import task, task_group

from common.week_2.model import multivariate_data, train_xgboost, VAL_END_INDEX

from common.week_2.feature_engineering import join_data_and_add_features

TRAINING_DATA_PATH = 'week-2/price_prediction_training_data.csv'
DATASET_NORM_WRITE_BUCKET = 'corise_airflow' # Modify here corise_airflow/week-2

VAL_END_INDEX = 31056


def df_convert_dtypes(df, convert_from, convert_to):
    cols = df.select_dtypes(include=[convert_from]).columns
    for col in cols:
        df[col] = df[col].values.astype(convert_to)
    return df


@task()
def extract() -> Dict[str, pd.DataFrame]:
    """
    #### Extract task
    A simple task that loads each file in the zipped file into a dataframe,
    building a list of dataframes that is returned


    """
    from zipfile import ZipFile
    filename = "/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"
    dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in ZipFile(filename).namelist()]
    return {
        'df_energy': dfs[0],
        'df_weather': dfs[1]
    }


@task
def post_process_energy_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare energy dataframe for merge with weather data
    """


    # Drop columns that are all 0s\
    import pandas as pd
    df = df.drop(['generation fossil coal-derived gas','generation fossil oil shale', 
                  'generation fossil peat', 'generation geothermal', 
                  'generation hydro pumped storage aggregated', 'generation marine', 
                  'generation wind offshore', 'forecast wind offshore eday ahead',
                  'total load forecast', 'forecast solar day ahead',
                  'forecast wind onshore day ahead'], 
                  axis=1)

    # Extract timestamp
    df['time'] = pd.to_datetime(df['time'], utc=True, infer_datetime_format=True)
    df = df.set_index('time')

    # Interpolate the null price values
    df.interpolate(method='linear', limit_direction='forward', inplace=True, axis=0)
    return df


@task
def post_process_weather_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare weather dataframe for merge with energy data
    """


    # Convert all ints to floats
    df = df_convert_dtypes(df, np.int64, np.float64)

    # Extract timestamp 
    df['time'] = pd.to_datetime(df['dt_iso'], utc=True, infer_datetime_format=True)

    # Drop original time column
    df = df.drop(['dt_iso'], axis=1)
    df = df.set_index('time')

    # Reset index and drop records for the same city and time
    df = df.reset_index().drop_duplicates(subset=['time', 'city_name'],
                                                          keep='first').set_index('time')

    # Remove unnecessary qualitiative columns
    df = df.drop(['weather_main', 'weather_id', 
                                  'weather_description', 'weather_icon'], axis=1)

    # Filter out pressure and wind speed outliers
    df.loc[df.pressure > 1051, 'pressure'] = np.nan
    df.loc[df.pressure < 931, 'pressure'] = np.nan
    df.loc[df.wind_speed > 50, 'wind_speed'] = np.nan

    # Interpolate for filtered values
    df.interpolate(method='linear', limit_direction='forward', inplace=True, axis=0)
    return df


@task
def join_dataframes_and_post_process(df_energy: pd.DataFrame, df_weather: pd.DataFrame) -> pd.DataFrame:
    """
    Join dataframes and drop city-specific features
    """


    df_final = df_energy
    df_1, df_2, df_3, df_4, df_5 = [x for _, x in df_weather.groupby('city_name')]
    dfs = [df_1, df_2, df_3, df_4, df_5]

    for df in dfs:
        city = df['city_name'].unique()
        city_str = str(city).replace("'", "").replace('[', '').replace(']', '').replace(' ', '')
        df = df.add_suffix('_{}'.format(city_str))
        df_final = df_final.merge(df, on=['time'], how='outer')
        df_final = df_final.drop('city_name_{}'.format(city_str), axis=1)


    cities = ['Barcelona', 'Bilbao', 'Madrid', 'Seville', 'Valencia']
    for city in cities:
        df_final = df_final.drop(['rain_3h_{}'.format(city)], axis=1)

    return df_final


@task
def add_features(df: pd.DataFrame) -> pd.DataFrame:

    """
    Extract helpful temporal, geographic, and highly correlated energy features
    """
    # Calculate the weight of every city
    total_pop = 6155116 + 5179243 + 1645342 + 1305342 + 987000
    weight_Madrid = 6155116 / total_pop
    weight_Barcelona = 5179243 / total_pop
    weight_Valencia = 1645342 / total_pop
    weight_Seville = 1305342 / total_pop
    weight_Bilbao = 987000 / total_pop
    cities_weights = {'Madrid': weight_Madrid, 
                      'Barcelona': weight_Barcelona,
                      'Valencia': weight_Valencia,
                      'Seville': weight_Seville,
                      'Bilbao': weight_Bilbao}

    for i in range(len(df)):
        # Generate 'hour', 'weekday' and 'month' features
        position = df.index[i]
        hour = position.hour
        weekday = position.weekday()
        month = position.month
        df.loc[position, 'hour'] = hour
        df.loc[position, 'weekday'] = weekday
        df.loc[position, 'month'] = month

        # Generate 'business hour' feature
        if (hour > 8 and hour < 14) or (hour > 16 and hour < 21):
            df.loc[position, 'business hour'] = 2
        elif (hour >= 14 and hour <= 16):
            df.loc[position, 'business hour'] = 1
        else:
            df.loc[position, 'business hour'] = 0
        print("business hours generated")

        # Generate 'weekend' feature

        if (weekday == 6):
            df.loc[position, 'weekday'] = 2
        elif (weekday == 5):
            df.loc[position, 'weekday'] = 1
        else:
            df.loc[position, 'weekday'] = 0
        print("weekdays generated")

        # Generate 'temp_range' for each city
        temp_weighted = 0
        for city in cities_weights.keys():
            temp_max = df.loc[position, 'temp_max_{}'.format(city)]
            temp_min = df.loc[position, 'temp_min_{}'.format(city)]
            df.loc[position, 'temp_range_{}'.format(city)] = abs(temp_max - temp_min)

            # Generated city-weighted temperature 
            temp = df.loc[position, 'temp_{}'.format(city)]
            temp_weighted += temp * cities_weights.get('{}'.format(city))
        df.loc[position, 'temp_weighted'] = temp_weighted

        print("city temp features generated")


    df['generation coal all'] = df['generation fossil hard coal'] + df['generation fossil brown coal/lignite']
    return df


@task
def prepare_model_inputs(df_final: pd.DataFrame):
    """
    Transform each feature to fall within a range from 0 to 1, pull out the target price from the features, 
    and use PCA to reduce the features to those with an explained variance >= 0.80. Concatenate the scaled and 
    dimensionality-reduced feature matrix with the scaled target vector, and return this result.
    matrix with the 
    """
    from sklearn.preprocessing import LabelEncoder, StandardScaler, MinMaxScaler
    from sklearn.decomposition import PCA
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    X = df_final[df_final.columns.drop('price actual')].values
    y = df_final['price actual'].values
    y = y.reshape(-1, 1)
    scaler_X = MinMaxScaler(feature_range=(0, 1))
    scaler_y = MinMaxScaler(feature_range=(0, 1))
    scaler_X.fit(X[:VAL_END_INDEX])
    scaler_y.fit(y[:VAL_END_INDEX])
    X_norm = scaler_X.transform(X)
    y_norm = scaler_y.transform(y)

    pca = PCA(n_components=0.80)
    pca.fit(X_norm[:VAL_END_INDEX])
    X_pca = pca.transform(X_norm)
    dataset_norm = np.concatenate((X_pca, y_norm), axis=1)
    df_norm = pd.DataFrame(dataset_norm)
    client = GCSHook().get_conn()
    # 
    write_bucket = client.bucket(DATASET_NORM_WRITE_BUCKET)
    # print('writing to bucket:', write_bucket)
    # print('Training_data_path:', TRAINING_DATA_PATH)
    write_bucket.blob(TRAINING_DATA_PATH).upload_from_string(pd.DataFrame(dataset_norm).to_csv())
    return 'uploaded to gcloud'


@task
def read_dataset_norm():
    """
    Read dataset norm from storage

    """

    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    import io
    client = GCSHook().get_conn()
    read_bucket = client.bucket(DATASET_NORM_WRITE_BUCKET)
    dataset_norm = pd.read_csv(io.BytesIO(read_bucket.blob(TRAINING_DATA_PATH).download_as_bytes())).to_numpy()

    return dataset_norm


def multivariate_data(dataset,
                      data_indices,
                      history_size,
                      target_size,
                      step, 
                      single_step=False) -> Tuple[np.ndarray, np.ndarray]:
    """
    Produce subset of dataset indexed by data_indices, with a window size of history_size hours
    """

    target = dataset[:, -1]
    data = []
    labels = []
    for i in data_indices:
        indices = range(i, i + history_size, step)
        # If within the last 23 hours in the dataset, skip 
        if i + history_size > len(dataset) - 1:
            continue
        data.append(dataset[indices])
        if single_step:
            labels.append(target[i + target_size])
        else:
            labels.append(target[i : i + target_size])
    return np.array(data), np.array(labels)


def train_xgboost(X_train, y_train, X_val, y_val) -> xgb.Booster:
    """
    Train xgboost model using training set and evaluated against evaluation set, using 
        a set of model parameters
    """

    X_train_xgb = X_train.reshape(-1, X_train.shape[1] * X_train.shape[2])
    X_val_xgb = X_val.reshape(-1, X_val.shape[1] * X_val.shape[2])
    param = {'eta': 0.03, 'max_depth': 180, 
             'subsample': 1.0, 'colsample_bytree': 0.95, 
             'alpha': 0.1, 'lambda': 0.15, 'gamma': 0.1,
             'objective': 'reg:linear', 'eval_metric': 'rmse', 
             'silent': 1, 'min_child_weight': 0.1, 'n_jobs': -1}
    dtrain = xgb.DMatrix(X_train_xgb, y_train)
    dval = xgb.DMatrix(X_val_xgb, y_val)
    eval_list = [(dtrain, 'train'), (dval, 'eval')]
    xgb_model = xgb.train(param, dtrain, 10, eval_list, early_stopping_rounds=3)
    return xgb_model


@task
def produce_indices(max_indices: int, num_indices: int) -> List[Tuple[np.ndarray, np.ndarray]]:
    import random
    """
    Produce zipped list of training and validation indices

    Each pair of training and validation indices should not overlap, and the
    training indices should never exceed the max of VAL_END_INDEX. This is 
    because the data from VAL_END_INDEX to the end will be used in the test
    dataset downstream from this pipeline.

    The number of pairs produced here will be equivalent to the number of 
    mapped 'format_data_and_train_model' tasks you have. For example, a list of
    size that is simply 
    [np.array(range(start_training_idx, end_training_idx) , range(end_training_idx, end_val_idx))]
    will be produced one mapped task.
    """
    
    # TODO Modify here
    indices = []
    
    for i in range(0, num_indices):
        all_indices = set(range(1, max_indices))
        random.seed(i)
        num_unique_elements = round(len(all_indices) * 0.8)
        train_indices = set(random.sample(all_indices, num_unique_elements))
        val_indices = all_indices - train_indices
        train_and_val_pair = np.array(list(train_indices)), np.array(list(val_indices))
        indices.append(train_and_val_pair)
        i = i+1

    return indices

    # return [zip([(np.array(sorted(list(train_indices))), np.array(sorted(list(val_indices))))])]
    # return list(zip([np.array(sorted(list(train_indices))), np.array(sorted(list(val_indices)))]))

@task
def format_data_and_train_model(dataset_norm: np.ndarray,
                                indices: Tuple[np.ndarray, np.ndarray]) -> xgb.Booster:
    # from airflow.models.xcom import XCom
    import pickle
    """
    Extract training and validation sets and labels, and train a model with a given
    set of training and validation indices
    """
    past_history = 24
    future_target = 0
    train_indices, val_indices = indices
    print(f"train_indices is {train_indices}, val_indices is {val_indices}")
    X_train, y_train = multivariate_data(dataset_norm, train_indices, past_history, future_target, step=1, single_step=True)
    X_val, y_val = multivariate_data(dataset_norm, val_indices, past_history, 
                                     future_target, step=1, single_step=True)
    model = train_xgboost(X_train, y_train, X_val, y_val)
    print(f"Model eval score is {model.best_score}")

    # Store the trained model in an XCom with the specified key
    # XCom.set(key='xcom_key', value=pickle.dumps(model))

    return model


@task
def select_best_model(models: List[xgb.Booster]):
    from io import BytesIO
    from google.cloud import storage
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    """
    Select model that generalizes the best against the validation set, and 
    write this to GCS. The best_score is an attribute of the model, and corresponds to
    the highest eval score yielded during training.
    """

    # TODO Modify here
    # Find the model with highest evaluation score
    print('coolected models',models)
    best_model = max(models, key= lambda model: model.best_score)
    # best_model = max(models)
    
    #Wrtie the best model to GCS
    MODEL_PATH = "week-2/best_model.bst"
    client = GCSHook()
    client.upload(
        bucket_name=DATASET_NORM_WRITE_BUCKET,
        object_name=MODEL_PATH,
        data=BytesIO(best_model.save_raw(raw_format="ubj")).getvalue(),
        timeout=600,
    )
    # with NameTemporaryFile as tmp_file:
    #     #save the best model to a temp file
    #     pickle.dump(best_model, tmp_file)
    #     tmp_file.flush()

    #     #Write the temporary file to GCS
    #     gcs = storage.Client()
    #     bucket = gcs.get_bucket('corise_airflow')
    #     blob = bucket.blob('/')
    #     blob.upload_from_filename(tmp_file.name)

    return best_model    



@task_group()
def join_data_and_add_features():
    """
    Task group responsible for feature engineering, including:
      1. Extracting dataframes from local zipped file
      2. Processing energy and weather dataframes
      3. Joining dataframes
      4. Adding features to the joined dataframe
      5. Producing a dimension-reduced numpy array containing the most
         significant features, and save it to GCS
    """
    output = extract()
    df_energy, df_weather =  output["df_energy"], output["df_weather"]
    df_energy = post_process_energy_df(df_energy)
    df_weather = post_process_weather_df(df_weather)
    df_final = join_dataframes_and_post_process(df_energy, df_weather)
    df_final = add_features(df_final)
    prepare_task = prepare_model_inputs(df_final)


@task_group
def train_and_select_best_model():
    from google.cloud import storage
    """
    Task group responsible for training XGBoost models to predict energy prices, including:
       1. Reading the dataset norm from GCS
       2. Producing a list of training and validation indices numpy array tuples,  
       3. Mapping each element of that list onto the indices argument of format_data_and_train_model
       4. Calling select_best_model on the output of all of the mapped tasks to select the best model and 
          write it to GCS 

    Using different train/val splits, train multiple models and select the one with the best evaluation score.
    """
    
    #Reading the dataset norm from GCS
    past_history = 24
    future_target = 0
    # dataset_norm = read_dataset_norm()
    dataset = read_dataset_norm()

    # TODO: Modify here to select best model and save it to GCS, using above methods including
    # format_data_and_train_model, produce_indices, and select_best_model



with DAG(dag_id = "energy_price_prediction_featuring",
    # task_id="energy_price_prediction_featuring",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['model_training'],
    render_template_as_native_obj=True,
    concurrency=5
    ) as energy_price_prediction_featuring:

        group_1 = join_data_and_add_features() 
        # group_2 = train_and_select_best_model()
        # group_1 >> group_2
 
with DAG(
    dag_id="energy_price_predictin_modelling",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example2"],
) as energy_price_predictin_modelling:
    # [START howto_operator_external_task_sensor]
    feature_sensor = ExternalTaskSensor(
        task_id="feature_sensor",
        external_dag_id=energy_price_prediction_featuring.dag_id,
        # external_task_id=energy_price_prediction_featuring.task_id,
        poke_interval = 60,
        timeout=600,
        # allowed_states=["success"],
        # failed_states=["failed", "skipped"],
        mode="reschedule",
    )
    group_2 = train_and_select_best_model()
    feature_sensor >> group_2