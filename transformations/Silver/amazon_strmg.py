import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, DateType, LongType 


@dlt.create_view(
    name = "silver_view"
)
def silver_view():
    df_silver = spark.readStream.table("streaming_amazon")
    df_silver = df_silver.withColumn(
        "Order_ID", 
        when(col("Order_ID").isNull() | (F.trim(col("Order_ID")) == ''), 'UNKNOWN_ORDER_ID').otherwise(col("Order_ID"))
    ).withColumn(
        "SKU", 
        when(col("SKU").isNull() | (F.trim(col("SKU")) == ''), 'UNKNOWN_SKU').otherwise(col("SKU"))
    ).withColumn(
        "Date", 
        when(col("Date").isNull(), F.lit('1900-01-01').cast('date')).otherwise(col("Date"))
    ).withColumn(
        "Qty", 
        when(col("Qty").isNull(), 0).otherwise(col("Qty"))
    ).withColumn(
        "Amount", 
        when(col("Amount").isNull(), 0.0).otherwise(col("Amount"))
    ).withColumn(
        "Fulfilment", 
        when(col("Fulfilment").isNull() | (F.trim(col("Fulfilment")) == ''), 'MISSING_FULFILMENT').otherwise(col("Fulfilment"))
    )
    
    df_silver = df_silver.withColumn(
        "Year", 
        year(col("Date"))
    ).withColumn(
        "Month", 
        month(col("Date"))
    ).withColumn(
        "Day", 
        dayofmonth(col("Date"))
    )
    
    df_silver = df_silver.withColumn(
        "Order_ID", 
        trim(col("Order_ID"))
    ).withColumn(
        "SKU", 
        trim(col("SKU"))
    ).withColumn(
        "Fulfilment", 
        trim(col("Fulfilment"))
    )
    
    return df_silver 

dlt.create_streaming_table(name = 'ods_sales')

dlt.create_auto_cdc_flow(
    target = 'ods_sales',
    source = 'silver_view',
    keys = ['Order_ID'],
    sequence_by = col('ingestion_timestamp'),
    stored_as_scd_type = 1
)  
    
