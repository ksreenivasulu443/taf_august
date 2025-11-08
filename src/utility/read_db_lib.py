def read_db(config, spark):
    df = (spark.read.format("jdbc")
          .option("url", 'jdbc:snowflake://pgkktza-qw01651.snowflakecomputing.com')
          .option("user", 'AUGUSTAUTOMATION')
          .option("password", 'Dharmavaram@2025')
          .option("dbtable", 'test_db.test_schema.SAMPLE_TABLE')
          .option("driver", 'net.snowflake.client.jdbc.SnowflakeDriver')
          .load())

    return df