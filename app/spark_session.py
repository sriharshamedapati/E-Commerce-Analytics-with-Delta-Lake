from pyspark.sql import SparkSession


def get_spark():

    spark = SparkSession.builder \
        .appName("LakehouseProject") \
        .master("spark://spark-master:7077") \
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ) \
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        ) \
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        ) \
        .getOrCreate()

    return spark