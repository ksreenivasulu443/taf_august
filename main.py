import sys
import json
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, current_timestamp, upper, initcap
)
from pyspark.sql import DataFrame

# ------------------------------------------------------------
# JOB ARGS (BATCH_TS OPTIONAL)
# ------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "REDSHIFT_SECRET_NAME",
        "REDSHIFT_TMP_DIR",
        "REDSHIFT_SILVER_TABLE",
        "REDSHIFT_GOLD_TABLE",
        "REDSHIFT_GOLD_BACKUP_TABLE"
    ]
)

# Optional argument
batch_ts = None
if "--BATCH_TS" in sys.argv:
    batch_ts = sys.argv[sys.argv.index("--BATCH_TS") + 1]

# ------------------------------------------------------------
# SPARK + AWS SETUP
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ------------------------------------------------------------
# LOAD REDSHIFT CREDENTIALS
# ------------------------------------------------------------
sm = boto3.client("secretsmanager")
secret_value = sm.get_secret_value(SecretId=args["REDSHIFT_SECRET_NAME"])
creds = json.loads(secret_value["SecretString"])
jdbc_url = f"jdbc:redshift://{creds['host']}:{creds['port']}/{creds['dbname']}"

# ------------------------------------------------------------
# FINAL GOLD SCHEMA (NO SURROGATE KEY)
# ------------------------------------------------------------
final_cols = [
    "customer_id","name","email","phone","batch_id",
    "start_date","end_date","active_flag","record_insert_ts"
]

def align(df: DataFrame, cols: list) -> DataFrame:
    """Ensures df has all columns in cols (adding nulls) in correct order."""
    for c in cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
    return df.select(cols)

# ------------------------------------------------------------
# READ SILVER DELTA
# ------------------------------------------------------------
silver = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", args["REDSHIFT_SILVER_TABLE"])
    .option("user", creds["username"])
    .option("password", creds["password"])
    .option("driver", "com.amazon.redshift.jdbc42.Driver")
    .load()
)

print("Silver delta rows:", silver.count())

# Remove unwanted audit column
if "record_updated_ts" in silver.columns:
    silver = silver.drop("record_updated_ts")

# Normalize key attributes
silver = (
    silver.withColumn("name", initcap(col("name")))
          .withColumn("email", upper(col("email")))
)

# ------------------------------------------------------------
# READ GOLD HISTORY
# ------------------------------------------------------------
gold_exists = True
try:
    gold = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", args["REDSHIFT_GOLD_TABLE"])
        .option("user", creds["username"])
        .option("password", creds["password"])
        .option("driver", "com.amazon.redshift.jdbc42.Driver")
        .load()
    )
except Exception as ex:
    gold_exists = False
    gold = None
    print("Gold table not found — performing initial load")

# ------------------------------------------------------------
# INITIAL LOAD CASE
# ------------------------------------------------------------
if not gold_exists or gold.count() == 0:
    print("Initial load — Silver delta → Gold")

    init_df = silver.withColumn(
        "start_date", current_timestamp()
    ).withColumn(
        "end_date", lit(None).cast("timestamp")
    ).withColumn(
        "active_flag", lit("Y")
    ).withColumn(
        "record_insert_ts", current_timestamp()
    )

    init_df = align(init_df, final_cols)

    init_df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", args["REDSHIFT_GOLD_TABLE"]) \
        .option("user", creds["username"]) \
        .option("password", creds["password"]) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("tempdir", args["REDSHIFT_TMP_DIR"]) \
        .mode("overwrite") \
        .save()

    print("Initial gold load done.")
    sys.exit(0)

# ------------------------------------------------------------
# BACKUP GOLD
# ------------------------------------------------------------
print("Backing up gold →", args["REDSHIFT_GOLD_BACKUP_TABLE"])
gold.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", args["REDSHIFT_GOLD_BACKUP_TABLE"]) \
    .option("user", creds["username"]) \
    .option("password", creds["password"]) \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("tempdir", args["REDSHIFT_TMP_DIR"]) \
    .mode("overwrite") \
    .save()

print("Backup completed.")

# ------------------------------------------------------------
# SPLIT GOLD INTO ACTIVE + HISTORY
# ------------------------------------------------------------
gold_active = gold.filter(col("active_flag") == "Y")
gold_history = gold.filter(col("active_flag") != "Y")

print("Active gold rows:", gold_active.count())
print("Historical gold rows:", gold_history.count())

columns = [
    "customer_id",
    "name",
    "email",
    "phone",
    "batch_id",
    "start_date",
    "end_date",
    "active_flag",
    "record_insert_ts"
]


new_records = (silver.join(gold, on="customer_id", how="left_anti").
            withColumn('start_date', current_timestamp()).withColumn('end_date',lit('2099-12-31T23:59:59')).
            withColumn('active_flag','Y'))

change_records_create_new = (silver.join(gold, on="customer_id", how="left_semi").
            withColumn('start_date', current_timestamp()).withColumn('end_date',lit('2099-12-31T23:59:59')).
            withColumn('active_flag','Y'))

change_records_mark_history = (gold.join(silver.select("customer_id", "start_date","batch_id"), on="customer_id", how="left_semi").
            withColumn('end_date',current_timestamp()).
            withColumn('active_flag','N'))

untouched_records = (gold.join(silver, on="customer_id", how="left anti"))



final_gold = (new_records.select(*columns).union(change_records_create_new.select(*columns)).union(change_records_mark_history.select(*columns)).
              union(untouched_records.select(*columns)).union(gold_history.select(*columns)))




# ------------------------------------------------------------
# WRITE FINAL GOLD → OVERWRITE
# ------------------------------------------------------------
final_gold.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", args["REDSHIFT_GOLD_TABLE"]) \
    .option("user", creds["username"]) \
    .option("password", creds["password"]) \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("tempdir", args["REDSHIFT_TMP_DIR"]) \
    .mode("overwrite") \
    .save()

print("SCD Type-2 Processing Completed Successfully 🚀")
