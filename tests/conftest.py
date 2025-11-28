import pytest
from pyspark.sql import SparkSession
import os
import yaml
from src.utility.read_file_lib import read_file
from src.utility.read_db_lib import read_db

import logging


@pytest.fixture(scope='session')
def spark_session():
    print("this is spark session fixture")
    taf_august= os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_server_jar = os.path.join(taf_august,'jars','mssql-jdbc-12.2.0.jre8.jar')
    postgres_jar = os.path.join(taf_august, 'jars', 'postgresql-42.6.2.jar')
    aws_bundle_jar = os.path.join(taf_august, 'jars', 'aws-java-sdk-bundle-1.12.262.jar')
    hadoop_aws_jar = os.path.join(taf_august, 'jars', 'hadoop-aws-3.3.4.jar')
    red_shift_jar = os.path.join(taf_august, 'jars', 'redshift-jdbc42-2.1.0.9.jar')
    jar_path =  sql_server_jar + ',' + postgres_jar + ',' + aws_bundle_jar + ',' + hadoop_aws_jar + ',' + red_shift_jar


    cred_file_path = "/Users/admin/PycharmProjects/taf_august/tests/cred_files/cred_config.yml"
    print(cred_file_path)
    with open(cred_file_path, "r") as file:
        creds = yaml.safe_load(file)['s3_qa']

    print("jar_path", jar_path)
    spark = (SparkSession.builder.master('local[1]')
             .config("spark.jars", jar_path)
             .config("spark.driver.extraClassPath", jar_path)
             .config("spark.executor.extraClassPath", jar_path)
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
             .config("spark.hadoop.fs.s3a.access.key", "AKIA43SC3GDGYHFQUAUQ")
             .config("spark.hadoop.fs.s3a.secret.key", "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")
             .appName("ETL Automation FW").getOrCreate())

    # spark.sparkContext.setLogLevel("INFO")
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
    source_config = config_data['source']
    target_config = config_data['target']
    validation_config = config_data['validations']
    if source_config['type'] == 'database':
        source_df = read_db(config=source_config, spark = spark, dir_path=dir_path)
    else:
        source_df = read_file(config = source_config, spark = spark, dir_path=dir_path)

    if target_config['type'] == 'database':
        target_df = read_db(config=target_config, spark = spark, dir_path=dir_path)
    else:
        target_df = read_file(config = target_config, spark = spark,dir_path=dir_path)

    return source_df.drop(*source_config['exclude_cols']), target_df.drop(*target_config['exclude_cols']), validation_config







