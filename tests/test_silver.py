import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,upper

SILVER_TABLE = "churn_catalog.silver.customer_cleaned1"

# --------------------------------------
# Spark Session Fixture
# --------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("silver-layer-test") \
        .getOrCreate()

# --------------------------------------
# Test 1 : Silver table exists
# --------------------------------------

def test_silver_table_exists(spark):

    tables = spark.sql("SHOW TABLES IN churn_catalog.silver")

    assert tables.filter(tables.tableName == "customer_cleaned1").count() == 1


# --------------------------------------
# Test 2 : Silver table contains data
# --------------------------------------

def test_silver_row_count(spark):

    df = spark.table(SILVER_TABLE)

    assert df.count() > 0


# --------------------------------------
# Test 3 : CustomerID should not be NULL
# --------------------------------------

def test_customerid_not_null(spark):

    df = spark.table(SILVER_TABLE)

    null_ids = df.filter(col("customerid").isNull()).count()

    assert null_ids == 0


# --------------------------------------
# Test 4 : No duplicate customer IDs
# --------------------------------------

def test_no_duplicate_customerid(spark):

    df = spark.table(SILVER_TABLE)

    duplicates = df.groupBy("customerid") \
                   .count() \
                   .filter(col("count") > 1) \
                   .count()

    assert duplicates == 0


# --------------------------------------
# Test 5 : Age must be between 18 and 100
# --------------------------------------

def test_age_valid_range(spark):

    df = spark.table(SILVER_TABLE)

    invalid_age = df.filter(
        (col("age") < 18) | (col("age") > 100)
    ).count()

    assert invalid_age == 0


# --------------------------------------
# Test 6 : Credit score range validation
# --------------------------------------

def test_credit_score_range(spark):

    df = spark.table(SILVER_TABLE)

    invalid_credit = df.filter(
        (col("creditscore") < 300) | (col("creditscore") > 900)
    ).count()

    assert invalid_credit == 0


# --------------------------------------
# Test 7 : Gender column should not be NULL
# --------------------------------------

def test_gender_not_null(spark):

    df = spark.table(SILVER_TABLE)

    null_gender = df.filter(col("gender").isNull()).count()

    assert null_gender == 0


# --------------------------------------
# Test 8 : Geography column should be uppercase
# --------------------------------------

def test_geography_uppercase(spark):

    df = spark.table(SILVER_TABLE)

    invalid_geo = df.filter(col("geography") != upper(col("geography"))).count()

    assert invalid_geo == 0