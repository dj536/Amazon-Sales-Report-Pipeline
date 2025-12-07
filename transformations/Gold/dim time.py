import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, DateType, LongType 
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, weekofyear, md5, concat
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

@dlt.view(
    name="dim_time_source_view"
)
def dim_time_source_view():
    return dlt.readStream("silver_view") \
        .select(col("Date")) \
        .distinct() \
        .withColumn("Day", dayofmonth(col("Date"))) \
        .withColumn("Month", month(col("Date"))) \
        .withColumn("Quarter", quarter(col("Date"))) \
        .withColumn("Year", year(col("Date"))) \
        .withColumn("Week", weekofyear(col("Date"))) \
        .withColumn(
            "TimeKey", 
            date_format(col("Date"), "yyyyMMdd")
        )


@dlt.table(
    name="dwh_dim_time",
    )
def dwh_dim_time():
    return dlt.readStream("dim_time_source_view")