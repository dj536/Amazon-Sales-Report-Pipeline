import dlt
from pyspark.sql.functions import col, concat, md5, substring, lit
@dlt.view(
    name="dwh_location_source_view"
)
def dwh_location_source_view():
    df = dlt.readStream("silver_view") \
        .select(col("ship-state").alias("ShipState"),
            col("ship-postal-code").alias("ShipPostalCode"),
            col("ship-country").alias("ShipCountry"),
            "ingestion_timestamp") \
            .distinct() 
    return df.withColumn(
        "location_key",
        concat(
             col("ShipState"), lit("_"),
            col("ShipCountry"), lit("_"),
            col("ShipPostalCode")
        )
    )


dlt.create_streaming_table(name="dwh_dim_location")

dlt.create_auto_cdc_flow(
    target = 'dwh_dim_location',
    source = 'dwh_location_source_view',
    keys = ['location_key'],
    sequence_by = col('ingestion_timestamp'), 
    stored_as_scd_type = 2,
    except_column_list = ['ingestion_timestamp']
)


