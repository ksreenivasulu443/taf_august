from pyspark.sql.functions import lit, col, when
from src.utility.report_lib import write_output

def data_compare(source, target, key_column,num_records=5):

    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)


    failed_count = failed.count()
    if failed_count > 0:
        failed_records = failed.limit(num_records).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]
        write_output(
                "data compare Check",
                "FAIL",
                f"Data mismatch data: {failed_preview}"
            )
    else:
        write_output(
            "data compare Check",
            "PASS",
            f"No mismatches found"
        )


    if failed_count > 0:
        columnList = source.columns# ['id','first_name','last_name','salary']
        print("columnList", columnList)
        print("keycolumns", key_column) #['id']
        for column in columnList: # column = salary
            print(column.lower())
            if column not in key_column: # salary not in ['id']
                key_column.append(column) # ['id','salary']
                temp_source = source.select(key_column).withColumnRenamed(column, "source_" + column)

                temp_target = target.select(key_column).withColumnRenamed(column, "target_" + column)
                key_column.remove(column) # ['id']
                temp_join = temp_source.join(temp_target, key_column, how='full_outer')
                (temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
                                                        "True").otherwise("False")).
                 filter("comparison == False ").show())

        status ='FAIL'

        return status
    else:
        status = 'PASS'
        return status



