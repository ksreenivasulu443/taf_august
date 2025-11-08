from src.utility.general_lib import read_schema, flatten

def read_file(config, spark, dir_path):
    """
    :param config: this will contain configs of source or target
    :param spark: spark session
    :return: spark dataframe
    """
    path = config['path']
    type = config['type'].lower()
    schema = config['schema']
    options = config['options']
    exclude_cols = config['exclude_cols']

    if type == 'csv':
        if schema == 'Y':
            schema_json = read_schema(dir_path)
            df = spark.read.schema(schema_json).csv(path=path,header=options['header'],sep=options['delimiter'])
        else:
            df = spark.read.csv(path=path,header=options['header'], sep=options['delimiter'], inferSchema=options['inferSchema'])
    elif type == 'json':
        df = spark.read.json(path=path,multiline=options['multiline'])
        if options['flatten'] == 'Y':
            df = flatten(df)
    elif type == 'parquet':
        df = spark.read.parquet(path)
    elif type == 'avro':
        df = spark.read.format('avro').load(path=path)
    elif type == 'txt':
        df = spark.read.format('csv').load(path=path,header=options['header'],sep=options['delimiter'])
    return df