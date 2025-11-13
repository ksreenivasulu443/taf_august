from src.data_validations.count_check import count_check
from src.data_validations.duplicate_check import duplicate_check
from src.data_validations.uniqueness_check import uniqueness_check


def test_count(read_data):
    source_df, target_df, validation_config = read_data
    key_columns = validation_config['count_check']['key_columns']
    status = count_check(source_df=source_df, target_df=target_df, key_columns=key_columns)
    assert status.upper() == 'PASS'

def test_duplicate(read_data):
    source_df, target_df, validation_config = read_data
    target_df.show()
    key_columns = validation_config['duplicate_check']['key_columns']
    status = duplicate_check(df=target_df, key_col=key_columns)
    assert status.upper() == 'PASS'

def test_uniqueness_check(read_data):
    source_df, target_df, validation_config = read_data
    unique_cols = validation_config['uniqueness_check']['unique_columns']
    status = uniqueness_check( df=target_df,unique_cols=unique_cols)
    assert status == 'PASS'
