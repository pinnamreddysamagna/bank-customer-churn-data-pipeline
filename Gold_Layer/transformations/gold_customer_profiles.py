import logging
from pyspark.sql.functions import *

# -----------------------------------
# Logger Configuration
# -----------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_layer")

try:

    logger.info("Starting Gold layer pipeline")

    # -----------------------------------
    # Step 1 : Read Silver Table
    # -----------------------------------

    silver_table = "churn_catalog.silver.customer_cleaned1"

    df = spark.table(silver_table)

    logger.info("Silver data loaded successfully")

    # -----------------------------------
    # Step 2 : Feature Engineering
    # -----------------------------------

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

    # -----------------------------------
    # Step 3 : Create Dimension Tables
    # -----------------------------------

    dim_customer = df.select(
        "customerid",
        "surname",
        "gender",
        "age",
        "tenure"
    ).dropDuplicates()

    dim_geography = df.select(
        "geography"
    ).dropDuplicates()

    dim_product = df.select(
        "numofproducts",
        "product_usage_level",
        "hascrcard"
    ).dropDuplicates()

    dim_activity = df.select(
        "isactivemember",
        "estimatedsalary"
    ).dropDuplicates()

    logger.info("Dimension tables created")

    # -----------------------------------
    # Step 4 : Create Fact Table
    # -----------------------------------

    fact_customer_churn = df.select(
        "customerid",
        "creditscore",
        "balance",
        "credit_score_category",
        "balance_segment",
        "numofproducts",
        "product_usage_level",
        "isactivemember"
    )

    logger.info("Fact table created")

    # -----------------------------------
    # Step 5 : Ensure Gold Schema Exists
    # -----------------------------------

    spark.sql("CREATE SCHEMA IF NOT EXISTS churn_catalog.gold")

    # -----------------------------------
    # Step 6 : Write Gold Tables
    # -----------------------------------

    dim_customer.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("churn_catalog.gold.dim_customer")

    dim_geography.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("churn_catalog.gold.dim_geography")

    dim_product.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("churn_catalog.gold.dim_product")

    dim_activity.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("churn_catalog.gold.dim_activity")

    fact_customer_churn.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("churn_catalog.gold.fact_customer_churn")

    logger.info("Gold tables successfully created")

except Exception as e:

    logger.error("Gold pipeline failed")
    logger.error(str(e))

finally:

    logger.info("Gold pipeline execution completed")