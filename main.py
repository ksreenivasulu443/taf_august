
# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()


df = spark.read.parquet("/Users/admin/PycharmProjects/taf_august/input_files/userdata1.parquet")

df.show()

