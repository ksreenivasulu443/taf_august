def test_count(read_data):
    source_df, target_df = read_data
    source_df.show()
    target_df.show()
    source_df.printSchema()
    assert True