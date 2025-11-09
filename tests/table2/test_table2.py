from src.data_validations.count_check import count_check
def test_count(read_data):
    source_df, target_df = read_data
    status = count_check(source_df=source_df, target_df=target_df)
    assert status.upper() == 'PASS'

