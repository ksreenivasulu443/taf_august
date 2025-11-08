import yaml
import os
from src.utility.general_lib import read_sql
def read_db(config, spark,dir_path):
    tests_path = os.path.dirname(dir_path)
    cred_file_path = os.path.join(tests_path, "cred_files", "cred_config.yml")
    print(cred_file_path)
    with open(cred_file_path, "r") as file:
        creds = yaml.safe_load(file)[config['cred_lookup']]

    if config['transformation'][0].lower() == 'y' :
        query = read_sql(dir_path=dir_path)
        df = (spark.read.format("jdbc")
              .option("url", creds['url'])
              .option("user", creds['user'])
              .option("password", creds['password'])
              .option("query", query)
              .option("driver", creds['driver'])
              .load())
    else:
        df = (spark.read.format("jdbc")
              .option("url", creds['url'])
              .option("user", creds['user'])
              .option("password", creds['password'])
              .option("dbtable", config['table'])
              .option("driver", creds['driver'])
              .load())

    return df

