import pandas as pd

data = {
    "name": ["John", "Sara", "Mike"],
    "age": [25, 30, 29],
    "email": ["john@gmail.com", "sara@", "mike@gmail.com"]  # One bad email
}

df = pd.DataFrame(data)
print(df)

from great_expectations.dataset import PandasDataset

ge_df = PandasDataset(df)

# Expect column to exist
ge_df.expect_column_to_exist("email")

# Expect age to be greater than 18
ge_df.expect_column_values_to_be_between("age", min_value=18, max_value=60)

# Expect email format to be valid
ge_df.expect_column_values_to_match_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")

# Show results
print(ge_df.validate())

