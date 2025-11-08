import pytest
from pyspark.sql import SparkSession
import os
import yaml
from src.utility.read_file_lib import read_file
from src.utility.read_db_lib import read_db


@pytest.fixture(scope='session')
def spark_session():
    print("this is spark session fixture")
    taf_august= os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_server = os.path.join(taf_august,'jars','mssql-jdbc-12.2.0.jre8.jar')
    postgres_jar = os.path.join(taf_august, 'jars', 'postgresql-42.6.2.jar')
    jar_path =  sql_server + ',' + postgres_jar
    print("jar_path", jar_path)
    spark = (SparkSession.builder.master('local[1]')
             .config("spark.jars", jar_path)
             .config("spark.driver.extraClassPath", jar_path)
             .config("spark.executor.extraClassPath", jar_path)
             .appName("ETL Automation FW").getOrCreate())
    return spark


@pytest.fixture(scope='module')
def read_config(request):
    dir_path = request.node.fspath.dirname
    config_path = os.path.join(dir_path,'config.yml')
    with open(config_path,'r') as f:
        config_data = yaml.safe_load(f)

    return config_data

@pytest.fixture(scope='module')
def read_data(spark_session,read_config, request):
    spark = spark_session
    config_data = read_config
    dir_path = request.node.fspath.dirname

    print("spark is", spark)
    print("config is", config_data, type(config_data))
    print("=="*50)
    source_config = config_data['source']
    print("==" * 50)
    print("source config", source_config)
    target_config = config_data['target']
    print("==" * 50)
    print("targte config", target_config)


    if source_config['type'] == 'database':
        source_df = read_db(config=source_config, spark = spark, dir_path=dir_path)
    else:
        source_df = read_file(config = source_config, spark = spark, dir_path=dir_path)

    if target_config['type'] == 'database':
        target_df = read_db(config=target_config, spark = spark, dir_path=dir_path)
    else:
        target_df = read_file(config = target_config, spark = spark,dir_path=dir_path)

    return source_df, target_df







