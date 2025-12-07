from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = (
    SparkSession.builder
        .appName("read_s3_local_jars")
        .config(
            "spark.jars",
            "/Users/admin/PycharmProjects/taf_august/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/Users/admin/PycharmProjects/taf_august/jars/hadoop-aws-3.3.4.jar,"
            "/Users/admin/PycharmProjects/taf_august/jars/redshift-jdbc42-2.1.0.9.jar"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.access.key", "AKIA43SC3GDGUCJXHDWW")
        .config("spark.hadoop.fs.s3a.secret.key", "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")
        .getOrCreate()
)

jdbc_url = "jdbc:redshift://default-workgroup.883829715149.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
existing_silver_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "silver.yellow_tripdata_silver_bkp") \
    .option("user", "admin") \
    .option("password", "Dharmavaram1") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .load()

bronze_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", 'admin') \
    .option("password", 'Dharmavaram1') \
    .option("dbtable", 'bronze.yellow_tripdata_bronze') \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .load()

bronze_df = bronze_df.select(
    col("vendorid").cast("integer"),
    to_timestamp("tpep_pickup_datetime").alias("tpep_pickup_datetime"),
    to_timestamp("tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
    col("passenger_count").cast("integer"),
    col("trip_distance").cast("float"),
    col("ratecodeid").cast("integer"),
    col("store_and_fwd_flag"),
    col("pulocationid").cast("integer"),
    col("dolocationid").cast("integer"),
    col("payment_type").cast("integer"),
    col("fare_amount").cast("float"),
    col("extra").cast("float"),
    col("mta_tax").cast("float"),
    col("tip_amount").cast("float"),
    col("tolls_amount").cast("float"),
    col("improvement_surcharge").cast("float"),
    col("total_amount").cast("float"),
    col("congestion_surcharge").cast("float"),
    col("airport_fee").cast("float"),
    col("cbd_congestion_fee").cast("float"),
    current_timestamp().alias("etl_insert_ts"),
    col("hash_key")
).dropDuplicates(["hash_key"]).filter("fare_amount>0")


new_rows_df = bronze_df.alias("s").join(
        existing_silver_df.alias("t"),
        col("s.hash_key") == col("t.hash_key"),
        "left_anti"
    )

    # Rows in silver but not in staging ( unchanged )
unchanged_df = existing_silver_df.alias("t").join(
        bronze_df.alias("s"),
        col("t.hash_key") == col("s.hash_key"),
        "left_anti"
    )

    # Updated rows = existing rows where any value changed
cols_to_compare = [
        "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "ratecodeid",
        "store_and_fwd_flag", "pulocationid", "dolocationid",
        "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee", "cbd_congestion_fee"
    ]

cond = " OR ".join([f"s.{c} <> t.{c}" for c in cols_to_compare])

updated_df = bronze_df.alias("s").join(
        existing_silver_df.alias("t"),
        col("s.hash_key") == col("t.hash_key"),
        "inner"
    ).filter(cond).select("s.*")

    # Union all SCD1 rows
final_df = unchanged_df.unionByName(updated_df).unionByName(new_rows_df)

# ----------------------------------------------------
# Write Final SCD1 Output
# ----------------------------------------------------
final_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", 'admin') \
    .option("password", 'Dharmavaram1') \
    .option("dbtable", "silver.yellow_tripdata_silver_expected") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .mode("overwrite") \
    .save()