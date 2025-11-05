import json
from pyspark.sql.types import StructType
import os

def read_schema(dir_path):
    schema_path = os.path.join(dir_path, 'schema.json')
    with open(schema_path, 'r') as f:
        schema = StructType.fromJson(json.load(f))
    return schema