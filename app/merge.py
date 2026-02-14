def merge_customers(spark):

    customers_path = "s3a://data/warehouse/customers"

    # -------------------------
    # Initial load
    # -------------------------
    customers_df = spark.read.csv(
        "s3a://data/raw/customers.csv",
        header=True,
        inferSchema=True
    )

    customers_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(customers_path)

    print("Initial customers Delta table created")

    # -------------------------
    # Load updates
    # -------------------------
    updates_df = spark.read.csv(
        "s3a://data/raw/updates.csv",
        header=True,
        inferSchema=True
    )

    updates_df.createOrReplaceTempView("updates")

    # -------------------------
    # MERGE UPSERT
    # -------------------------
    spark.sql(f"""
        MERGE INTO delta.`{customers_path}` AS target
        USING updates AS source
        ON target.customer_id = source.customer_id

        WHEN MATCHED THEN
            UPDATE SET
                target.email = source.email,
                target.last_updated_date = source.last_updated_date

        WHEN NOT MATCHED THEN
            INSERT *
    """)

    print("Customers MERGE completed")

    # VERY IMPORTANT
    spark.catalog.clearCache()

    # -------------------------
    # VERIFY latest version
    # -------------------------
    print("Latest version:")

    spark.read.format("delta") \
        .load(customers_path) \
        .where("customer_id = 2") \
        .show()

    # -------------------------
    # TIME TRAVEL (MUST BE BEFORE VACUUM)
    # -------------------------
    print("Time travel version 0:")

    spark.read.format("delta") \
        .option("versionAsOf", 0) \
        .load(customers_path) \
        .where("customer_id = 2") \
        .show()

    # -------------------------
    # VACUUM (Requirement #10)
    # -------------------------
    spark.conf.set(
        "spark.databricks.delta.retentionDurationCheck.enabled",
        "false"
    )

    spark.sql(f"""
        VACUUM delta.`{customers_path}` RETAIN 0 HOURS
    """)

    print("VACUUM completed")
