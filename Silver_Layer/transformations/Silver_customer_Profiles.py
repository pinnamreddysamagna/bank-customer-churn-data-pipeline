import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------
# Logger Configuration
# -----------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_layer")

try:

    logger.info("Starting Silver layer pipeline")

    # -----------------------------------
    # Step 1 : Read Bronze Table
    # -----------------------------------

    bronze_table = "churn_catalog.raw.customer_data1"

    df = spark.table(bronze_table)

    logger.info(f"Bronze data loaded. Rows: {df.count()}")

    # -----------------------------------
    # Step 2 : Select Required Columns
    # -----------------------------------

    df = df.select(
        "customerid",
        "surname",
        "creditscore",
        "geography",
        "gender",
        "age",
        "tenure",
        "balance",
        "numofproducts",
        "hascrcard",
        "isactivemember",
        "estimatedsalary"
    )

    # -----------------------------------
    # Step 3 : Remove Null Customer IDs
    # -----------------------------------

    df = df.filter(col("customerid").isNotNull())

    logger.info("Null customer IDs removed")

    # -----------------------------------
    # Step 4 : Remove Duplicate Records
    # -----------------------------------

    df = df.dropDuplicates(["customerid"])

    logger.info("Duplicate records removed")

    # -----------------------------------
    # Step 5 : Data Type Standardization
    # -----------------------------------

    df = df.withColumn("creditscore", col("creditscore").cast(IntegerType()))
    df = df.withColumn("age", col("age").cast(IntegerType()))
    df = df.withColumn("balance", col("balance").cast(DoubleType()))

    logger.info("Data types standardized")

    # -----------------------------------
    # Step 6 : Handle NULL Values
    # -----------------------------------

    # Replace NULL surname
    df = df.withColumn(
        "surname",
        when(col("surname").isNull() | (trim(col("surname")) == ""), "NONE")
        .otherwise(col("surname"))
    )

    # Replace NULL gender or blank
    df = df.withColumn(
        "gender",
        when(col("gender").isNull() | (trim(col("gender")) == ""), "NONE")
        .otherwise(col("gender"))
    )

    # Replace NULL creditscore with average
    avg_credit = int(df.select(avg("creditscore")).collect()[0][0])

    df = df.withColumn(
        "creditscore",
        when(col("creditscore").isNull(), avg_credit)
        .otherwise(col("creditscore"))
    )

    logger.info("Null values handled")

    # -----------------------------------
    # Step 7 : Clean Special Characters in Surname
    # -----------------------------------

    # Keep only alphabets
    df = df.withColumn(
        "surname",
        regexp_replace(col("surname"), "[^a-zA-Z]", "")
    )

    logger.info("Special characters removed from surname")

    # -----------------------------------
    # Step 8 : Business Rules Validation
    # -----------------------------------

    df = df.filter((col("age") >= 18) & (col("age") <= 100))
    df = df.filter((col("creditscore") >= 300) & (col("creditscore") <= 900))

    logger.info("Business rules applied")

    # -----------------------------------
    # Step 9 : Standardize Text Fields
    # -----------------------------------

    df = df.withColumn("geography", upper(col("geography")))
    df = df.withColumn("gender", upper(col("gender")))

    logger.info("Categorical columns standardized")
    # -----------------------------------
    # Step 11 : Write Silver Table
    # -----------------------------------

    silver_table = "churn_catalog.silver.customer_cleaned1"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_table)

    logger.info("Silver table successfully created")

except Exception as e:

    logger.error("Silver pipeline failed")
    logger.error(str(e))

finally:

    logger.info("Silver pipeline execution completed")