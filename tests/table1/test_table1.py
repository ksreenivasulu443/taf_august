from src.data_validations.count_check import count_check
from src.data_validations.duplicate_check import duplicate_check
from src.data_validations.uniqueness_check import uniqueness_check
from src.data_validations.null_value_check import null_value_check
from src.data_validations.data_compare_check import data_compare
from src.data_validations.schema_check import schema_check


# def test_count(read_data):
#     source_df, target_df, validation_config = read_data
#     key_columns = validation_config['count_check']['key_columns']
#     status = count_check(source_df=source_df, target_df=target_df, key_columns=key_columns)
#     assert status.upper() == 'PASS'
#
# def test_duplicate(read_data):
#     source_df, target_df, validation_config = read_data
#     target_df.show()
#     key_columns = validation_config['duplicate_check']['key_columns']
#     status = duplicate_check(df=target_df, key_col=key_columns)
#     assert status.upper() == 'PASS'
#
# def test_uniqueness_check(read_data):
#     source_df, target_df, validation_config = read_data
#     unique_cols = validation_config['uniqueness_check']['unique_columns']
#     status = uniqueness_check( df=target_df,unique_cols=unique_cols)
#     assert status == 'PASS'

# def test_null_check(read_data):
#     source_df, target_df, validation_config = read_data
#     target_df.show()
#     null_columns = validation_config['null_check']['null_columns']
#     num_records = validation_config['null_check']['num_records']
#     status = null_value_check(df=target_df, null_cols=null_columns,num_records=num_records)
#     assert status == 'PASS'

def test_data_compare_check(read_data):
    source_df, target_df, validation_config = read_data
    key_columns = validation_config['data_compare_check']['key_column']
    num_records = validation_config['data_compare_check']['num_records']
    status = data_compare(source=source_df, target=target_df, key_column=key_columns , num_records=num_records)
    assert status == 'PASS'


def test_schema(read_data, spark_session):
    source_df, target_df, validation_config = read_data
    spark  = spark_session
    status = schema_check(source=source_df, target=target_df,spark=spark)
    assert status == 'PASS'

