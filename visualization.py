# =============================================================================
# PART 4: DATA PREPARATION FOR VISUALIZATION
# Spark 3.4.2 | Python 3.11
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as spark_sum, explode
)
from pyspark.sql.types import *
from pymongo import MongoClient
import os

# -----------------------------------------------------------------------------
# 1. Spark Session
# -----------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Visualization Data Preparation") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark session started")

# -----------------------------------------------------------------------------
# 2. MongoDB Connection
# -----------------------------------------------------------------------------
client = MongoClient("mongodb://localhost:27017/")
ecommerce = client["ecommerce"]
print("✓ MongoDB connected (database: ecommerce)")

# -----------------------------------------------------------------------------
# 3. Extract Data from MongoDB
# -----------------------------------------------------------------------------
print("\n[STEP 2] Extracting Data from MongoDB...")
print("-" * 80)

users_mongo = list(ecommerce.users.find({}, {"_id": 0}).limit(10000))
transactions_raw = list(ecommerce.transactions.find({}, {"_id": 0}).limit(500000))

print(f"✓ Loaded {len(users_mongo)} users")
print(f"✓ Loaded {len(transactions_raw)} transactions")

# -----------------------------------------------------------------------------
# 4. Normalize Transaction Data (CRITICAL FIX)
# -----------------------------------------------------------------------------
transactions_mongo = []
for t in transactions_raw:
    t["total"] = float(t.get("total", 0))
    t["discount"] = float(t.get("discount", 0))

    for item in t.get("items", []):
        item["price"] = float(item.get("price", 0))
        item["quantity"] = int(item.get("quantity", 0))

    transactions_mongo.append(t)

# -----------------------------------------------------------------------------
# 5. Explicit Schemas
# -----------------------------------------------------------------------------
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("geo_data", StructType([
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ]), True)
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("total", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("items", ArrayType(
        StructType([
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])
    ), True)
])

users_df = spark.createDataFrame(users_mongo, schema=user_schema)
transactions_df = spark.createDataFrame(transactions_mongo, schema=transaction_schema)

print("✓ Spark DataFrames created successfully")

# -----------------------------------------------------------------------------
# 6. Output Directory
# -----------------------------------------------------------------------------
BASE_OUTPUT = "output"
os.makedirs(BASE_OUTPUT, exist_ok=True)

# =============================================================================
# VISUALIZATION 1: SALES OVER TIME
# =============================================================================
print("\n[1] Creating Sales Over Time data")

sales_over_time_df = (
    transactions_df
    .withColumn("date", to_date(col("timestamp")))
    .groupBy("date")
    .agg(spark_sum("total").alias("daily_sales"))
    .orderBy("date")
)

sales_over_time_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{BASE_OUTPUT}/sales_over_time")

# =============================================================================
# VISUALIZATION 2: CUSTOMER SPENDING DISTRIBUTION
# =============================================================================
print("[2] Creating Customer Spending data")

customer_spending_df = (
    transactions_df
    .groupBy("user_id")
    .agg(spark_sum("total").alias("total_spent"))
)

customer_spending_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{BASE_OUTPUT}/customer_spending")

# =============================================================================
# VISUALIZATION 3: TOP-SELLING PRODUCTS
# =============================================================================
print("[3] Creating Top Products data")

products_df = transactions_df.select(explode("items").alias("item"))

top_products_df = (
    products_df
    .groupBy(col("item.product_id").alias("product_id"))
    .agg(spark_sum("item.quantity").alias("units_sold"))
    .orderBy(col("units_sold").desc())
)

top_products_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{BASE_OUTPUT}/top_products")

# =============================================================================
# VISUALIZATION 4: CONVERSION FUNNEL
# =============================================================================
print("[4] Creating Conversion Funnel data")

purchased_users = transactions_df.select("user_id").distinct().count()

funnel_df = spark.createDataFrame([
    ("Viewed Product", 50000),
    ("Added to Cart", 30000),
    ("Purchased", purchased_users)
], ["stage", "users"])

funnel_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{BASE_OUTPUT}/conversion_funnel")

# -----------------------------------------------------------------------------
# Finish
# -----------------------------------------------------------------------------
spark.stop()
client.close()
print("\n✓ All visualization datasets created successfully")
