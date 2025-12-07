import dlt
from pyspark.sql.functions import col, when, lit

@dlt.view(
    name="dwh_order_status_source_view"
)
def dwh_order_status_source_view():
    df = dlt.readStream("silver_view") \
        .select(col("Status").alias("orderStatus"), "ingestion_timestamp") \
            .distinct()
    df= df.withColumn(
        "StatusCategory",
        when(col("orderStatus").isin("Shipped", "Delivered", "Expedited"), "Completed")
        .when(col("orderStatus").isin("Cancelled", "Returned"), "Rejected")
        .otherwise("Pending")
    )
    return df.withColumn(
            "status_key",
            col("orderStatus")
    )

dlt.create_streaming_table(name="dwh_dim_orderStatus")


dlt.create_auto_cdc_flow(
    target = 'dwh_dim_orderStatus',
    source = 'dwh_order_status_source_view',
    keys = ['status_key'],
    sequence_by = col('ingestion_timestamp'), 
    stored_as_scd_type = 2,
    except_column_list = ['ingestion_timestamp']
)

