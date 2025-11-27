# from pyspark.sql import SparkSession
#
# spark = (SparkSession.builder
#     .appName("read_s3_bronze")
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     .config("spark.hadoop.fs.s3a.access.key", "AKIA43SC3GDGYHFQUAUQ")
#     .config("spark.hadoop.fs.s3a.secret.key", "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")
#     .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
#     .getOrCreate())
#
# df = spark.read.option("header", "true").csv("s3a://august-batch-etl/Contact_info.csv")
#
# df.show()
# df.printSchema()
#
#
#
#
# from pyspark.sql import SparkSession
#
# spark = (
#     SparkSession.builder
#         .appName("read_s3_bronze")
#         .config(
#             "spark.jars",
#             "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar,"
#             "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
#         )
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
#         .config("spark.hadoop.fs.s3a.access.key", "AKIA43SC3GDGYHFQUAUQ")
#         .config("spark.hadoop.fs.s3a.secret.key", "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")
#         .config("spark.hadoop.fs.s3a.fast.upload", "true")
#         .config("spark.hadoop.fs.s3a.connection.maximum", "200")
#         .getOrCreate()
# )
#
# df = spark.read.option("header", "true").csv("s3a://august-batch-etl/Contact_info.csv")
# df.show()
# df.printSchema()


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("read_s3_local_jars")
        .config(
            "spark.jars",
            "/Users/admin/PycharmProjects/taf_august/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/Users/admin/PycharmProjects/taf_august/jars/hadoop-aws-3.3.4.jar"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
        .config("spark.hadoop.fs.s3a.access.key", "AKIA43SC3GDGYHFQUAUQ")
        .config("spark.hadoop.fs.s3a.secret.key", "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")
        .getOrCreate()
)

df = spark.read.option("header", "true").csv("s3a://august-batch-etl/Contact_info.csv")

df.show()
# df.printSchema()


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("Connect_Redshift_Serverless")
        .config(
            "spark.jars",
            "/Users/admin/PycharmProjects/taf_august/jars/redshift-jdbc42-2.1.0.9.jar"
        )
        .getOrCreate()
)
#jdbc_url = "jdbc:redshift://august-etl-group.883829715149.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
jdbc_url = "jdbc:redshift://default-workgroup.883829715149.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "public.contact_info") \
    .option("user", "admin") \
    .option("password", "Dharmavaram1") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .load()

df.show()

#


glue(Ec2 - computer)
