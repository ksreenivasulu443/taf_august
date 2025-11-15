
# Import SparkSession
from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()


df = spark.read.csv("/Users/admin/PycharmProjects/taf_august/input_files/customer_data/customer_data_01.csv", header=True, inferSchema=True)

df.show()
source_schema = df.schema

list1 = []
for field in source_schema:
    # print(field)
    #
    # print("field name", field.name)
    # print("field datatype", field.dataType.simpleString())

    list1.append((field.name.lower(), field.dataType.simpleString()))
print(list1)
source_schema_df = spark.createDataFrame(list1,["col_name", "source_data_type"])

source_schema_df.show()



#
# with open("/Users/admin/PycharmProjects/taf_august/tests/table1/schema2.json", 'r') as f:
#     schema = StructType.fromJson(json.load(f))
#
# print("schema is", schema)
#
#
#
# df_schema = spark.read.schema(schema).csv("/Users/admin/PycharmProjects/taf_august/input_files/customer_data/customer_data_01.csv", header=True)
#
# df_schema.show()
#
# df_schema.printSchema()