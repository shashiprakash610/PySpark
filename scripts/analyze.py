from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, avg, col

# Create Spark session
spark = SparkSession.builder.appName("BA Statistik Analysis").getOrCreate()

# Load processed Parquet files
df = spark.read.parquet("Data/processed/*.parquet")

df.select("date").show(10, truncate=False)
df.printSchema()
df.filter(col("date").isNull()).show(5)
# Extract year and month from 'date'
df = df.withColumn("year", year("date")).withColumn("month", month("date"))
print("Columns in dataframe:", df.columns)

# Get name of data column (unemployment rate, count, etc.)
value_col = [col for col in df.columns if col not in ["date", "region", "category", "source_file", "year", "month"]][0]

# Compute average value by year, region, and category
yearly_avg = df.groupBy("category", "region", "year") \
    .agg(avg(col(value_col)).alias("average_value")) \
    .orderBy("category", "region", "year")


# Show top results
yearly_avg.show(50, truncate=False)

# Save to CSV if needed
yearly_avg.write.mode("overwrite").option("header", True).csv("Data/analysis/yearly_averages")

# Stop Spark session
spark.stop()
