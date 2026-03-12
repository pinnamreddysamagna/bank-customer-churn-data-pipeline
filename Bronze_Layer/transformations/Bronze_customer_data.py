from pyspark.sql.functions import current_timestamp, col,when,rand
import logging

# --------------------------------------------------
# Logger Configuration
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_ingestion")

# --------------------------------------------------
# Configuration
# --------------------------------------------------

SOURCE_PATH = "s3://bank-customer-churn-data/processed/"
BRONZE_TABLE = "churn_catalog.raw.customer_data1"

try:

    logger.info("Starting Bronze ingestion pipeline")

    # --------------------------------------------------
    # Check if source path exists and contains files
    # --------------------------------------------------

    try:
        files = dbutils.fs.ls(SOURCE_PATH)
    except Exception:
        logger.warning(f"Source path does not exist: {SOURCE_PATH}")
        files = []

    if len(files) == 0:
        logger.warning("No files found in source path. Bronze ingestion skipped.")

    else:

        logger.info(f"{len(files)} file(s) detected")

        # --------------------------------------------------
        # Read raw parquet data from S3
        # --------------------------------------------------

        df = spark.read.format("parquet").load(SOURCE_PATH)

        logger.info("Raw data loaded successfully")

        # --------------------------------------------------
        # Normalize column names (avoid case issues)
        # --------------------------------------------------

        df = df.toDF(*[c.lower() for c in df.columns])

        logger.info("Column normalization completed")

        
        # --------------------------------------------------
        # Replace surnames with NULL
        # --------------------------------------------------
        df = df.withColumn(
            "surname",
            when(rand() < 0.1, None).otherwise(col("surname"))
        )
        
        logger.info("surnames replaced with NULL values")
         # --------------------------------------------------
        # Replace creditscore with NULL
        # --------------------------------------------------
        df = df.withColumn(
           "creditscore",
           when(rand() < 0.1, None).otherwise(col("creditscore"))
        )
        logger.info("credit score replaced with NULL values")
        # --------------------------------------------------
        # Replace gender with NULL
        # --------------------------------------------------
        df = df.withColumn(
            "gender",
            when(rand() < 0.1, " ").otherwise(col("gender"))
        )
        logger.info("gender replaced with NULL values")
        # --------------------------------------------------
        # Ensure Bronze schema exists
        # --------------------------------------------------

        spark.sql("CREATE SCHEMA IF NOT EXISTS churn_catalog.raw")

        logger.info("Schema verified")

        # --------------------------------------------------
        # Write data to Bronze Delta table
        # --------------------------------------------------

        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(BRONZE_TABLE)

        logger.info(f"Bronze table updated successfully: {BRONZE_TABLE}")

except Exception as e:

    logger.error("Bronze ingestion failed")
    logger.error(str(e))

finally:

    logger.info("Bronze pipeline execution finished")