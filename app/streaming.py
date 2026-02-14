from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

def stream_sales(spark):

    print("Starting streaming ingestion...")

    sales_schema = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("sale_date", StringType(), True)
    ])

    sales_stream = spark.readStream \
        .schema(sales_schema) \
        .option("header", True) \
        .csv("s3a://data/raw")

    query = sales_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://data/checkpoints/sales_stream") \
        .start("s3a://data/warehouse/sales")

    print("Streaming query started")

    query.awaitTermination(20)   # IMPORTANT: finite duration

    query.stop()

    print("Streaming ingestion completed")
