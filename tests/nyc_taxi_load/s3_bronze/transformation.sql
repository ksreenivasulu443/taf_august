SELECT *
FROM bronze.yellow_tripdata_bronze
WHERE etl_insert_ts = (
    SELECT MAX(etl_insert_ts)
    FROM bronze.yellow_tripdata_bronze
)

