import dlt
import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType, LongType 

expectations_amazon_sales_rules = {
    "rule1": "Order_ID IS NOT NULL",
    "rule2": "SKU IS NOT NULL",
    "rule3": "Date IS NOT NULL",    
    "rule4": "Qty IS NOT NULL AND Qty > 0",
    "rule5": "Amount IS NOT NULL AND Amount > 0"
}

@dlt.table(
    name = 'streaming_amazon'
)
@dlt.expect_all(expectations_amazon_sales_rules)
def streaming_amazon():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/amazon_sales_catalog/amazon_sales_report-schema/bronze_volume/Data/")

    new_names = []
    for colonne in df.columns:
        new_name = re.sub(r'[ ,;{}()\n\t=]', '_', colonne)
        new_names.append(new_name)
    df = df.toDF(*new_names)

    df = df.withColumn("Date", F.trim(F.regexp_replace(F.col("Date"), '"', '')))

    df = df.withColumn(
    "Date", 
    F.to_date(F.col("Date"), 'MM-dd-yy'))

    df = df.withColumn(
    "Qty", 
    F.col("Qty").cast(IntegerType()))

    df = df.withColumn(
    "Amount", 
    F.col("Amount").cast(DoubleType()))

    df = df.withColumn(
    "index", 
    F.col("index").cast(LongType()))

    df = df.withColumn("ingestion_timestamp", F.current_timestamp())

    #print(df.columns)
    df.printSchema()
    
    return df