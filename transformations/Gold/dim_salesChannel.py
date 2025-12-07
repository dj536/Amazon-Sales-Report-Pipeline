import dlt
from pyspark.sql.functions import col, concat, lit

@dlt.view(
    name="dwh_sales_channel_view"
)
def dwh_sales_channel_source_view():
    df = dlt.readStream("silver_view") \
        .select(
            col("Sales_Channel_").alias("salesChannel"),
            col("Fulfilment").alias("fulfilmentType"),
            col("ship-service-level").alias("servicelevel"),
            "ingestion_timestamp"
        )\
            .distinct()
    return df.withColumn(
        "channelKey",
        concat(
            col("salesChannel"), lit("_"),
            col("fulfilmentType"), lit("_"), 
            col("serviceLevel")
        )
    )


dlt.create_streaming_table(name="dwh_dim_sales_channel")

dlt.create_auto_cdc_flow(
    target = 'dwh_dim_sales_channel',
    source = 'dwh_sales_channel_view',
    keys = ['channelKey'],
    sequence_by = col('ingestion_timestamp'), 
    stored_as_scd_type = 2,
    except_column_list = ['ingestion_timestamp']
)




