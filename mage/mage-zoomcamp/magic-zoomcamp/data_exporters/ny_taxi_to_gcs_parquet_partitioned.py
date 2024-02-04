from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os

import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/mage-pr-5a1e3f2919ec.json'
bucket_name = 'mage-data-engineering-bucket-hossam'
project_id = 'mage-pr'
table_name = 'nyc_taxi'
root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data_to_google_cloud_storage(data: DataFrame, **kwargs) -> None:
    
    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'default'

    # bucket_name = 'mage-data-engineering-bucket-hossam'
    # object_key = 'nyc_taxi_data.parquet'

    # GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
    #     df,
    #     bucket_name,
    #     object_key,  
    # )

    # data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )