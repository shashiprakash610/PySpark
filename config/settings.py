import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(BASE_DIR, "data/raw/sales.csv")
PROCESSED_PATH = os.path.join(BASE_DIR, "data/processed/sales_parquet")

# Schema Definition
SALES_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("country", StringType(), True)
])

# Spark Config
SPARK_CONFIG = {
    "spark.sql.shuffle.partitions": "4",          # Optimize for local
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g"
}