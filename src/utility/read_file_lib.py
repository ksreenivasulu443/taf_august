def read_file(config, spark):
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
        df = spark.read.csv(path=path,header=options['header'])
    elif type == 'json':
        df = spark.read.json(path=path,multiline=options['multiline'])
    elif type == 'parquet':
        df = spark.read.parquet(path)
    elif type == 'avro':
        df = spark.read.format('avro').load(path=path)
    return df