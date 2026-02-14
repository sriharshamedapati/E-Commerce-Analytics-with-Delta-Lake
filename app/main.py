from spark_session import get_spark
from utils import create_bucket_if_not_exists, upload_raw_files
from batch import ingest_products
from merge import merge_customers
from streaming import stream_sales

if __name__ == "__main__":

    spark = get_spark()

    print("Spark started successfully!")

    # Create MinIO bucket
    create_bucket_if_not_exists("data")

    # Upload CSV files into MinIO raw zone
    upload_raw_files("data")

    # Batch ingestion (products)
    ingest_products(spark)

    # Customers MERGE + time travel + vacuum
    merge_customers(spark)

    # Streaming ingestion
    stream_sales(spark)

    spark.stop()

    print("Pipeline finished successfully")
