from src.data_validations.count_check import count_check
import logging
def test_count(read_data):
    source_df, target_df = read_data
    status = count_check(source_df=source_df, target_df=target_df)
    logging.INFO("This is table2 count validation")
    assert status.upper() == 'PASS'

