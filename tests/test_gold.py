import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Gold tables
DIM_CUSTOMER = "churn_catalog.gold.dim_customer"
DIM_GEOGRAPHY = "churn_catalog.gold.dim_geography"
DIM_PRODUCT = "churn_catalog.gold.dim_product"
DIM_ACTIVITY = "churn_catalog.gold.dim_activity"
FACT_TABLE = "churn_catalog.gold.fact_customer_churn"


# --------------------------------------
# Spark Session Fixture
# --------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("gold-layer-test") \
        .getOrCreate()


# --------------------------------------
# Test 1 : dim_customer table exists
# --------------------------------------

def test_dim_customer_exists(spark):

    tables = spark.sql("SHOW TABLES IN churn_catalog.gold")

    assert tables.filter(tables.tableName == "dim_customer").count() == 1


# --------------------------------------
# Test 2 : dim_customer contains data
# --------------------------------------

def test_dim_customer_not_empty(spark):

    df = spark.table(DIM_CUSTOMER)

    assert df.count() > 0


# --------------------------------------
# Test 3 : credit_score_category values valid
# --------------------------------------

def test_credit_score_category_values(spark):

    df = spark.table(DIM_CUSTOMER)

    invalid = df.filter(
        ~col("credit_score_category").isin(
            "Excellent", "Good", "Average", "Poor"
        )
    ).count()

    assert invalid == 0


# --------------------------------------
# Test 4 : Geography dimension created
# --------------------------------------

def test_dim_geography_exists(spark):

    tables = spark.sql("SHOW TABLES IN churn_catalog.gold")

    assert tables.filter(tables.tableName == "dim_geography").count() == 1


# --------------------------------------
# Test 5 : geo_id generated
# --------------------------------------

def test_geo_id_not_null(spark):

    df = spark.table(DIM_GEOGRAPHY)

    null_geo = df.filter(col("geo_id").isNull()).count()

    assert null_geo == 0


# --------------------------------------
# Test 6 : Product dimension created
# --------------------------------------

def test_dim_product_exists(spark):

    tables = spark.sql("SHOW TABLES IN churn_catalog.gold")

    assert tables.filter(tables.tableName == "dim_product").count() == 1


# --------------------------------------
# Test 7 : product_id generated
# --------------------------------------

def test_product_id_not_null(spark):

    df = spark.table(DIM_PRODUCT)

    null_products = df.filter(col("product_id").isNull()).count()

    assert null_products == 0


# --------------------------------------
# Test 8 : Activity dimension created
# --------------------------------------

def test_dim_activity_exists(spark):

    tables = spark.sql("SHOW TABLES IN churn_catalog.gold")

    assert tables.filter(tables.tableName == "dim_activity").count() == 1


# --------------------------------------
# Test 9 : Fact table exists
# --------------------------------------

def test_fact_table_exists(spark):

    tables = spark.sql("SHOW TABLES IN churn_catalog.gold")

    assert tables.filter(tables.tableName == "fact_customer_churn").count() == 1


# --------------------------------------
# Test 10 : Fact table has data
# --------------------------------------

def test_fact_table_not_empty(spark):

    df = spark.table(FACT_TABLE)

    assert df.count() > 0


# --------------------------------------
# Test 11 : Foreign keys should not be NULL
# --------------------------------------

def test_fact_foreign_keys_not_null(spark):

    df = spark.table(FACT_TABLE)

    null_keys = df.filter(
        col("geo_id").isNull() |
        col("product_id").isNull() |
        col("activity_id").isNull()
    ).count()

    assert null_keys == 0