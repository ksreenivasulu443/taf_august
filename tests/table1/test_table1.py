from src.data_validations.count_check import count_check


def test_count(read_data):
    source_df, target_df, validation_config = read_data
    key_columns = validation_config['count_check']['key_columns']
    status = count_check(source_df=source_df, target_df=target_df, key_columns=key_columns)
    assert status.upper() == 'PASS'
