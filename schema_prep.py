
# Import SparkSession
from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()


# df = spark.read.csv("/Users/admin/PycharmProjects/taf_august/input_files/customer_data/customer_data_01.csv", header=True, inferSchema=True)
#
# print(df.schema.json())

with open("/Users/admin/PycharmProjects/taf_august/tests/table1/schema.json", 'r') as f:
    schema = StructType.fromJson(json.load(f))

print("schema is", schema)



df_schema= spark.read.schema(schema).csv("/Users/admin/PycharmProjects/taf_august/input_files/customer_data/customer_data_01.csv", header=True)

df_schema.show()

df_schema.printSchema()