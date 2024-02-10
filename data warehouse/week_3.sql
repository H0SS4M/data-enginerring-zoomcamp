CREATE OR REPLACE EXTERNAL TABLE `root-wharf-412723.nytaxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://root-wharf-412723-terra-bucket/green/green_tripdata_2022-*.parquet']
);

select * from `root-wharf-412723.nytaxi.external_green_tripdata` limit 10;

CREATE OR REPLACE TABLE nytaxi.green_tripdata_non_partitoned AS
SELECT * FROM `root-wharf-412723.nytaxi.external_green_tripdata`;

select count(*) from nytaxi.green_tripdata_non_partitoned;

SELECT count(distinct(PULocationID)) FROM `root-wharf-412723.nytaxi.green_tripdata_non_partitoned`;

SELECT count(distinct(PULocationID)) FROM `root-wharf-412723.nytaxi.external_green_tripdata`;

select count(*) from `root-wharf-412723.nytaxi.green_tripdata_non_partitoned` where fare_amount = 0;

CREATE OR REPLACE TABLE `root-wharf-412723.nytaxi.green_tripdata_partitoned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `root-wharf-412723.nytaxi.external_green_tripdata`
);

select distinct(PUlocationID) from `root-wharf-412723.nytaxi.green_tripdata_partitoned`
  where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';

select distinct(PUlocationID) from `root-wharf-412723.nytaxi.green_tripdata_non_partitoned`
  where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';
