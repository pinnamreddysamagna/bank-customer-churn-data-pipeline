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

    total_rows = df.count()
    logger.info(f"Bronze data loaded. Rows: {total_rows}")

    # Validation 1 : Check if Bronze table is empty
    if total_rows == 0:
        raise Exception("Bronze table is empty")

    # -----------------------------------
    # Step 2 : Select Required Columns
    # -----------------------------------

    required_columns = [
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
    ]

    # Validation 2 : Check missing columns
    missing_cols = [c for c in required_columns if c not in df.columns]

    if len(missing_cols) > 0:
        raise Exception(f"Missing columns in Bronze table: {missing_cols}")

    df = df.select(*required_columns)

    logger.info("Required columns selected")

    # -----------------------------------
    # Step 3 : Remove Null Customer IDs
    # -----------------------------------

    null_ids = df.filter(col("customerid").isNull()).count()
    logger.info(f"Null customer IDs found: {null_ids}")

    df = df.filter(col("customerid").isNotNull())

    logger.info("Null customer IDs removed")

    # -----------------------------------
    # Step 4 : Remove Duplicate Records
    # -----------------------------------

    duplicate_count = df.groupBy("customerid").count().filter(col("count") > 1).count()
    logger.info(f"Duplicate customer IDs detected: {duplicate_count}")

    df = df.dropDuplicates(["customerid"])

    logger.info("Duplicate records removed")

    # -----------------------------------
    # Step 5 : Data Type Standardization
    # -----------------------------------

    df = df.withColumn("creditscore", col("creditscore").cast(IntegerType()))
    df = df.withColumn("age", col("age").cast(IntegerType()))
    df = df.withColumn("tenure", col("tenure").cast(IntegerType()))
    df = df.withColumn("numofproducts", col("numofproducts").cast(IntegerType()))
    df = df.withColumn("hascrcard", col("hascrcard").cast(IntegerType()))
    df = df.withColumn("isactivemember", col("isactivemember").cast(IntegerType()))
    df = df.withColumn("balance", col("balance").cast(DoubleType()))
    df = df.withColumn("estimatedsalary", col("estimatedsalary").cast(DoubleType()))

    logger.info("Data types standardized")

    # -----------------------------------
    # Step 6 : Handle NULL Values
    # -----------------------------------

    null_surname = df.filter(col("surname").isNull() | (trim(col("surname")) == "")).count()
    logger.info(f"Null surname records: {null_surname}")

    df = df.withColumn(
        "surname",
        when(col("surname").isNull() | (trim(col("surname")) == ""), "NONE")
        .otherwise(col("surname"))
    )

    null_gender = df.filter(col("gender").isNull() | (trim(col("gender")) == "")).count()
    logger.info(f"Null gender records: {null_gender}")

    df = df.withColumn(
        "gender",
        when(col("gender").isNull() | (trim(col("gender")) == ""), "NONE")
        .otherwise(col("gender"))
    )

    # Replace NULL creditscore with average
    avg_credit = int(df.select(avg("creditscore")).collect()[0][0])

    null_credit = df.filter(col("creditscore").isNull()).count()
    logger.info(f"Null credit scores found: {null_credit}")

    df = df.withColumn(
        "creditscore",
        when(col("creditscore").isNull(), avg_credit)
        .otherwise(col("creditscore"))
    )

    logger.info("Null values handled")

    # -----------------------------------
    # Step 7 : Clean Special Characters in Surname
    # -----------------------------------

    df = df.withColumn(
        "surname",
        regexp_replace(col("surname"), "[^a-zA-Z]", "")
    )

    logger.info("Special characters removed from surname")

    # -----------------------------------
    # Step 8 : Business Rules Validation
    # -----------------------------------

    invalid_age = df.filter((col("age") < 18) | (col("age") > 100)).count()
    logger.info(f"Invalid age records: {invalid_age}")

    invalid_credit = df.filter((col("creditscore") < 300) | (col("creditscore") > 900)).count()
    logger.info(f"Invalid credit score records: {invalid_credit}")

    df = df.filter((col("age") >= 18) & (col("age") <= 100))
    df = df.filter((col("creditscore") >= 300) & (col("creditscore") <= 900))

    logger.info("Business rules applied")

    # -----------------------------------
    # Step 9 : Standardize Text Fields
    # -----------------------------------

    df = df.withColumn("geography", upper(col("geography")))
    df = df.withColumn("gender", upper(col("gender")))

    logger.info("Categorical columns standardized")

    # Validation : Check geography distribution
    logger.info("Geography distribution:")
    df.groupBy("geography").count().show()

    # -----------------------------------
    # Step 10 : Write Silver Table
    # -----------------------------------

    final_rows = df.count()
    logger.info(f"Final records to write in Silver: {final_rows}")

    silver_table = "churn_catalog.silver.customer_cleaned1"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(silver_table)

    logger.info("Silver table successfully created")

except Exception as e:

    logger.error("Silver pipeline failed")
    logger.error(str(e))

finally:

    logger.info("Silver pipeline execution completed")
