import dlt
from pyspark.sql.functions import col, concat, md5, substring, lit

@dlt.view(
    name="dwh_product_source_view",
)
def dwh_product_source_view():
    df = dlt.readStream("silver_view") \
        .select("SKU", "Style", "Category", "Size", "ingestion_timestamp") \
        .distinct()        
    df = df.withColumn(
        "ProductCode",
        substring(col("SKU"), 1, 4) 
    ).withColumn(
        "Line",
        substring(col("SKU"), 5, 3) 
    ).withColumn(
        "product_key", 
        concat(col("SKU"), col("Style")))

    return df 


dlt.create_streaming_table(name="dwh_dim_product")

dlt.create_auto_cdc_flow(
    target = 'dwh_dim_product',
    source = 'dwh_product_source_view',
    keys = ['product_key'], 
    sequence_by = col('ingestion_timestamp'), 
    stored_as_scd_type = 2,
    except_column_list = ['ingestion_timestamp']
)