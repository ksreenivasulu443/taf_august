# Databricks notebook source
df = (spark.read.format("jdbc") 
    .option("url", 'jdbc:snowflake://pgkktza-qw01651.snowflakecomputing.com') 
    .option("user", 'AUGUSTAUTOMATION') 
    .option("password", 'Dharmavaram@2025') 
    .option("dbtable", 'test_db.test_schema.SAMPLE_TABLE') 
    .option("driver", 'net.snowflake.client.jdbc.SnowflakeDriver') 
    .load())

df.display()

# COMMAND ----------



# COMMAND ----------

df2 = (spark.read.format("jdbc") 
    .option("url", 'jdbc:snowflake://pgkktza-qw01651.snowflakecomputing.com') 
    .option("user", 'AUGUSTAUTOMATION') 
    .option("password", 'Dharmavaram@2025') 
    .option("query", """select upper(surname), lower(GIVEN_NAME) from test_db.test_schema.SAMPLE_TABLE where identifier>5""") 
    .option("driver", 'net.snowflake.client.jdbc.SnowflakeDriver') 
    .load())

df2.display()

# COMMAND ----------

df_sql_server = spark.read.format("jdbc") \
    .option("url", 'jdbc:sqlserver://august2025.database.windows.net:1433;database=augustautodb') \
    .option("user", 'autoadmin') \
    .option("password", 'Dharmavaram1@') \
    .option("query", 'select customerid, email from [dbo].[customer]') \
    .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
    .load()

df_sql_server.display()

# COMMAND ----------

adls_account_name = "augustadls"
key = "S7Sn0Dy5w+uB6f0wUH9pNxYqLctiFbAc5XSqxh12hSDU7a2HCHRJUFzJeR+ABDfTK64t4B+IPZFN+AStdHemfw=="
spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

df = spark.read.parquet(f"abfss://test@decautoadls.dfs.core.windows.net/raw/customer/")
display(df)

# COMMAND ----------

account_name = "augustadls"
account_key  = "S7Sn0Dy5w+uB6f0wUH9pNxYqLctiFbAc5XSqxh12hSDU7a2HCHRJUFzJeR+ABDfTK64t4B+IPZFN+AStdHemfw=="

# configure key for blob endpoint
spark.conf.set(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key)

# path using wasbs://
file_path = "wasbs://test@augustadls.blob.core.windows.net/contact/Contact_info_t.csv"

# read CSV
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")   # optional
      .csv(file_path))

display(df)


# COMMAND ----------

