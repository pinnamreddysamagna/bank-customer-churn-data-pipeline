import logging
from pyspark.sql.functions import *
from pyspark.sql.functions import monotonically_increasing_id

# -------------------------------------------------
# Logger Configuration
# -------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_layer")

try:

    logger.info("Starting Gold Layer Pipeline")

    # -------------------------------------------------
    # Ensure Gold Schema Exists
    # -------------------------------------------------

    spark.sql("CREATE SCHEMA IF NOT EXISTS churn_catalog.gold")

    # -------------------------------------------------
    # Step 1 : Read Silver Table
    # -------------------------------------------------

    silver_table = "churn_catalog.silver.customer_cleaned1"

    df = spark.table(silver_table)

    logger.info(f"Silver data loaded. Rows: {df.count()}")

    # -------------------------------------------------
    # Step 2 : Feature Engineering
    # -------------------------------------------------

    df = df.withColumn(
        "credit_score_category",
        when(col("creditscore") >= 750, "Excellent")
        .when(col("creditscore") >= 650, "Good")
        .when(col("creditscore") >= 550, "Average")
        .otherwise("Poor")
    )

    df = df.withColumn(
        "balance_segment",
        when(col("balance") == 0, "Zero Balance")
        .when(col("balance") < 50000, "Low Balance")
        .when(col("balance") < 100000, "Medium Balance")
        .otherwise("High Balance")
    )

    df = df.withColumn(
        "product_usage_level",
        when(col("numofproducts") == 1, "Low Usage")
        .when(col("numofproducts") == 2, "Moderate Usage")
        .otherwise("High Usage")
    )

    logger.info("Feature engineering completed")

    # -------------------------------------------------
    # Dimension Table - Customer
    # -------------------------------------------------

    dim_customer = df.select(
        col("customerid").alias("customer_id"),
        "surname",
        "gender",
        "age",
        "credit_score_category"
    ).dropDuplicates()

    dim_customer.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("churn_catalog.gold.dim_customer")

    logger.info("dim_customer created")

    # -------------------------------------------------
    # Dimension Table - Geography
    # -------------------------------------------------

    dim_geography = df.select(
        col("geography").alias("country")
    ).dropDuplicates()

    dim_geography = dim_geography.withColumn(
        "geo_id",
        monotonically_increasing_id()
    )

    dim_geography.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("churn_catalog.gold.dim_geography")

    logger.info("dim_geography created")

    # -------------------------------------------------
    # Dimension Table - Product
    # -------------------------------------------------

    dim_product = df.select(
        col("numofproducts").alias("num_of_products"),
        "product_usage_level"
    ).dropDuplicates()

    dim_product = dim_product.withColumn(
        "product_id",
        monotonically_increasing_id()
    )

    dim_product.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("churn_catalog.gold.dim_product")

    logger.info("dim_product created")

    # -------------------------------------------------
    # Dimension Table - Activity
    # -------------------------------------------------

    dim_activity = df.select(
        col("isactivemember").alias("is_active_member")
    ).dropDuplicates()

    dim_activity = dim_activity.withColumn(
        "activity_id",
        monotonically_increasing_id()
    )

    dim_activity.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("churn_catalog.gold.dim_activity")

    logger.info("dim_activity created")

    # -------------------------------------------------
    # Step 3 : Create Fact Table
    # -------------------------------------------------

    fact_df = df \
        .join(dim_geography, df.geography == dim_geography.country, "left") \
        .join(dim_product, df.numofproducts == dim_product.num_of_products, "left") \
        .join(dim_activity, df.isactivemember == dim_activity.is_active_member, "left")

    fact_customer_churn = fact_df.select(
        col("customerid").alias("customer_id"),
        "geo_id",
        "product_id",
        "activity_id",
        col("creditscore").alias("credit_score"),
        "balance"
    )

    fact_customer_churn.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("churn_catalog.gold.fact_customer_churn")

    logger.info("fact_customer_churn created")

    logger.info("Gold Layer Pipeline Completed Successfully")

except Exception as e:

    logger.error("Gold pipeline failed")
    logger.error(str(e))

finally:

    logger.info("Gold pipeline execution completed")