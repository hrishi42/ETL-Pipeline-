from pyspark.sql import SparkSession
import os

# Step 1: Create a SparkSession with S3 support
spark = SparkSession.builder \
    .appName("ETL_Pipeline_S3") \
    .config("spark.hadoop.fs.s3a.access.key", "<YOUR_AWS_ACCESS_KEY>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<YOUR_AWS_SECRET_KEY>") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Step 2: Define S3 source and target paths
input_path = "s3a://your-bucket/input-data/sales.csv"
output_path = "s3a://your-bucket/processed-data/sales_transformed/"

# Step 3: Read CSV from S3
df = spark.read.option("header", True).csv(input_path)

# Step 4: Transform the Data
# Example: Filter sales over $500 and add a new column
from pyspark.sql.functions import col, lit

transformed_df = df.filter(col("amount") > 500).withColumn("processed", lit(True))

# Step 5: Write the transformed data back to S3 (as partitioned Parquet, for example)
transformed_df.write.mode("overwrite").parquet(output_path)

# Done!
print(f"✅ Data transformed and saved to: {output_path}")

# Clean up
spark.stop()
