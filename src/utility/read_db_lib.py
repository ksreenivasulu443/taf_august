import yaml
import os
def read_db(config, spark,dir_path):
    tests_path = os.path.dirname(dir_path)
    cred_file_path = os.path.join(tests_path, "cred_files", "cred_config.yml")
    print(cred_file_path)
    with open(cred_file_path, "r") as file:
        creds = yaml.safe_load(file)[config['cred_lookup']]

    df = (spark.read.format("jdbc")
          .option("url", creds['url'])
          .option("user", creds['user'])
          .option("password", creds['password'])
          .option("dbtable", config['table'])
          .option("driver", creds['driver'])
          .load())

    return df

