from pyspark.sql.functions import col, regexp_extract, udf
import datetime
from pyspark.sql.types import BooleanType


def name_check(target, column):
    pattern = "^[a-zA-Z ]+$"

    df = target.withColumn(
        "is_valid",
        regexp_extract(col(column), pattern, 0) != ""
    )

    df.show()

    failed = df.filter("is_valid = False")
    failed.show()

    status = 'FAIL' if failed.count() > 0 else 'PASS'
    return status


def check_range(target, column, min_val, max_val):
    invalid_df = target.filter(f"{column} not between {min_val} and {max_val}")
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        status = 'FAIL'
    else:
        status = 'PASS'
    return status


def date_check_YYYYMMDD(target, column):
    def is_valid_date_format(date_str: str) -> bool:
        try:
            # Try to parse the string in the format 'dd-mm-yyyy'
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    date_format_udf = udf(is_valid_date_format, BooleanType())

    df_with_validation = target.withColumn("is_valid_format", date_format_udf(col("column"))).filter(
        'is_valid_format = False')

    df_with_validation.show()

    failed = df_with_validation.count()
    if failed > 0:
        status ='FAIL'
    else:
        status ='PASS'
    return status

def date_check_DDMMYYYY(target, column):
    def is_valid_date_format(date_str: str) -> bool:
        try:
            # Try to parse the string in the format 'dd-mm-yyyy'
            datetime.strptime(date_str, "%d-%m-%Y")
            return True
        except ValueError:
            return False

    date_format_udf = udf(is_valid_date_format, BooleanType())

    df_with_validation = target.withColumn("is_valid_format", date_format_udf(col("column"))).filter(
        'is_valid_format = False')

    df_with_validation.show()

    failed = df_with_validation.count()
    if failed > 0:
        status ='FAIL'
    else:
        status ='PASS'
    return status