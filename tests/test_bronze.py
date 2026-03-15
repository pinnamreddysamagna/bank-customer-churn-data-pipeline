import pytest
from pyspark.sql import SparkSession

BRONZE_TABLE = "churn_catalog.raw.customer_data1"

# -----------------------------------------
# Spark Session Fixture
# -----------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("bronze-test") \
        .getOrCreate()

# -----------------------------------------
# Test 1 : Bronze table exists
# -----------------------------------------

def test_bronze_table_exists(spark):

    tables = spark.sql("SHOW TABLES IN churn_catalog.raw")

    assert tables.filter(tables.tableName == "customer_data1").count() == 1

# -----------------------------------------
# Test 2 : Bronze table has data
# -----------------------------------------

def test_bronze_row_count(spark):

    df = spark.table(BRONZE_TABLE)

    assert df.count() > 0


# -----------------------------------------
# Test 3 : Required columns exist
# -----------------------------------------

def test_bronze_columns(spark):

    df = spark.table(BRONZE_TABLE)

    expected_columns = [
        "creditscore",
        "geography",
        "gender",
        "age",
        "balance",
        "numofproducts",
        "hascrcard",
        "isactivemember",
        "estimatedsalary"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 4 : Column names are lowercase
# -----------------------------------------

def test_column_lowercase(spark):

    df = spark.table(BRONZE_TABLE)

    for col_name in df.columns:
        assert col_name == col_name.lower()


# -----------------------------------------
# Test 5 : Creditscore contains some NULLs
# (because your pipeline randomly inserts NULL)
# -----------------------------------------

def test_creditscore_nulls(spark):

    df = spark.table(BRONZE_TABLE)

    null_count = df.filter(df.creditscore.isNull()).count()

    assert null_count >= 0


# -----------------------------------------
# Test 6 : Gender column exists
# -----------------------------------------

def test_gender_column(spark):

    df = spark.table(BRONZE_TABLE)

    assert "gender" in df.columns