from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

def ingest_products(spark):

    products_path = "s3a://data/warehouse/products"

    schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True)
    ])

    df = spark.read \
        .schema(schema) \
        .csv("s3a://data/raw/products.csv", header=True)

    df.write \
        .format("delta") \
        .partitionBy("category") \
        .mode("overwrite") \
        .save(products_path)

    print("Products Delta table created")

    # Data quality constraint
    try:
        spark.sql(f"""
            ALTER TABLE delta.`{products_path}`
            ADD CONSTRAINT price_positive CHECK (price > 0)
        """)
        print("Price constraint added")
    except:
        print("Constraint already exists")

    # Optimize + ZORDER
    try:
        spark.sql(f"""
            OPTIMIZE delta.`{products_path}`
            ZORDER BY (product_id)
        """)
        print("OPTIMIZE completed")
    except:
        print("OPTIMIZE skipped")
