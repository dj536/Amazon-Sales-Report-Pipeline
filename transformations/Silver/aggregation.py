import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, DateType, LongType 

@dlt.create_view(
    name="sales_aggregation_view",
    comment="Vue agrégée des ventes utilisant les calculs SUM(Qty), SUM(Amount) et COUNT(OrderID)."
)
def sales_aggregation_view():
    return dlt.readStream("silver_view") \
        .groupBy(
            col("Category"),
            col("Date")     
        ) \
        .agg(
            F.sum(col("Qty")).alias("TotalQty"),
            
            F.sum(col("Amount")).alias("TotalAmount"),
            
            F.count(col("Order_ID")).alias("OrderCount")
        )

@dlt.table(
    name="ods_sales_summary",
    comment="Table récapitulant les ventes agrégées par Category et Date."
)
def ods_sales_summary():
    return dlt.readStream("sales_aggregation_view")
