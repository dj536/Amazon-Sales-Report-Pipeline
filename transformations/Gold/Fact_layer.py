import dlt
from pyspark.sql.functions import col, concat, lit, date_format

@dlt.view(
    name="dwh_fact_sales_view",
)
def dwh_fact_sales_view():
    df = dlt.readStream("silver_view").select(
        col("Order_ID").alias("SalesKeySource"),
        "Qty",
        "Amount",
        col("Date").alias("TransactionDate"),

        concat(col("SKU"), col("Style")).alias("product_key"),

        col("Status").alias("status_key"),

        concat(col("Sales_Channel_"), lit("_"), col("Fulfilment"), lit("_"), col("ship-service-level")).alias("channelKey"),

        concat(col("ship-state"), lit("_"), col("ship-country"), lit("_"), col("ship-postal-code")).alias("location_key"),
        
        date_format(col("Date"), "yyyyMMdd").alias("TimeKey")
    )
    return df

@dlt.table(
    name="dwh_fact_sales",
)
def dwh_fact_sales():
    fact_df = dlt.readStream("dwh_fact_sales_view")
    
    dim_product = dlt.read("dwh_dim_product")
    dim_order_status = dlt.read("dwh_dim_orderStatus")
    dim_sales_channel = dlt.read("dwh_dim_sales_channel")
    dim_location = dlt.read("dwh_dim_location")
    dim_time = dlt.read("dwh_dim_time").withColumnRenamed("TimeKey", "DimTimeKey")
    
    fact_df = fact_df.join(
        dim_time.select(col("DimTimeKey")),
        fact_df.TimeKey == dim_time.DimTimeKey,
        "inner"
    ).drop("TimeKey")
    
    fact_df = fact_df.join(
        dim_product
            .filter(col("__END_AT").isNull())
            .select(col("product_key").alias("ProductKey"), col("product_key")),
        fact_df.product_key == dim_product.product_key,
        "inner"
    ).drop("product_key")
    
    fact_df = fact_df.join(
        dim_order_status
            .filter(col("__END_AT").isNull())
            .select(col("status_key").alias("OrderStatusKey"), col("status_key")),
        fact_df.status_key == dim_order_status.status_key,
        "inner"
    ).drop("status_key")
    
    fact_df = fact_df.join(
        dim_sales_channel
            .filter(col("__END_AT").isNull())
            .select(col("channelKey").alias("SalesChannelKey"), col("channelKey")),
        fact_df.channelKey == dim_sales_channel.channelKey,
        "inner"
    ).drop("channelKey")
    
    fact_df = fact_df.join(
        dim_location
            .filter(col("__END_AT").isNull())
            .select(col("location_key").alias("LocationKey"), col("location_key")),
        fact_df.location_key == dim_location.location_key,
        "inner"
    ).drop("location_key")
    
    return fact_df.select(
        col("SalesKeySource").alias("SalesKey"),
        col("ProductKey"),
        col("DimTimeKey").alias("TimeKey"),
        col("LocationKey"),
        col("SalesChannelKey"),
        col("OrderStatusKey"),
        col("Qty"),
        col("Amount")
    )