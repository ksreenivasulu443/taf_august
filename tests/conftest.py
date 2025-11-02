import pytest
from pyspark.sql import SparkSession
import os
import yaml

@pytest.fixture(scope='session')
def spark_session():
    print("this is spark session fixture")
    spark = SparkSession.builder.master('local[1]').appName("ETL Automation FW").getOrCreate()
    return spark

@pytest.fixture(scope='module')
def read_config(request):
    dir_path = request.node.fspath.dirname
    config_path = os.path.join(dir_path,'config.yml')
    with open(config_path,'r') as f:
        config_data = yaml.safe_load(f)

    return config_data



@pytest.fixture(scope='module')
def read_data(spark_session,read_config):
    spark = spark_session
    config_data = read_config

    print("spark is", spark)
    print("config is", config_data, type(config_data))
    print("=="*50)
    source_config = config_data['source']
    print("==" * 50)
    print("source config", source_config)
    target_config = config_data['target']
    print("==" * 50)
    print("targte config", target_config)





