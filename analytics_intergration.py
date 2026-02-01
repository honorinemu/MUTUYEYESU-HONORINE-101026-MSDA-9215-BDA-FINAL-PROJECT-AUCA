"""
PART 3: ANALYTICS INTEGRATION
==============================
Cross-Platform Analytics using MongoDB, HBase (simulated), and Spark

Business Question:
Customer Lifetime Value (CLV) combining:
- MongoDB: User profiles + Transaction history
- HBase: Session engagement (simulated via JSON)
- Spark: Integration + Complex analytics

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
import happybase

print("=" * 80)
print("PART 3: ANALYTICS INTEGRATION - CROSS-PLATFORM CLV ANALYSIS")
print("=" * 80)

# ============================================================================
# STEP 1: INITIALIZE CONNECTIONS
# ============================================================================
print("\n[STEP 1] Initializing Connections...")
print("-" * 80)

spark = SparkSession.builder \
    .appName("Integrated CLV Analysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark connected")

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["ecommerce"]
print("✓ MongoDB connected")

try:
    hbase_conn = happybase.Connection("localhost", port=9090)
    hbase_conn.open()
    hbase_available = True
    print("✓ HBase connected")
except Exception:
    hbase_available = False
    print("⚠ HBase not available – using JSON simulation")

# ============================================================================
# STEP 2: EXTRACT DATA FROM MONGODB
# ============================================================================
print("\n[STEP 2] Extracting Data from MongoDB...")
print("-" * 80)

users_mongo = list(mongo_db.users.find({}, {"_id": 0}).limit(10000))
transactions_mongo = list(mongo_db.transactions.find({}, {"_id": 0}).limit(50000))

print(f"✓ Extracted {len(users_mongo)} users")
print(f"✓ Extracted {len(transactions_mongo)} transactions")

# ---------------------------------------------------------------------------
# 🔧 NORMALIZE MONGODB DATA (CRITICAL FIX)
# ---------------------------------------------------------------------------

# Normalize users
for u in users_mongo:
    u["user_id"] = str(u.get("user_id", ""))
    u["registration_date"] = str(u.get("registration_date", ""))

    geo = u.get("geo_data", {})
    u["geo_data"] = {
        "state": str(geo.get("state", "")),
        "city": str(geo.get("city", "")),
        "country": str(geo.get("country", ""))
    }

# Normalize transactions
for tx in transactions_mongo:
    tx["transaction_id"] = str(tx.get("transaction_id", ""))
    tx["user_id"] = str(tx.get("user_id", ""))
    tx["timestamp"] = str(tx.get("timestamp", ""))
    tx["payment_method"] = str(tx.get("payment_method", ""))

    # 🔑 These are the fields that caused your crash
    tx["total"] = float(tx.get("total", 0.0))
    tx["discount"] = float(tx.get("discount", 0.0))

    # Normalize nested items
    items = tx.get("items", [])
    normalized_items = []

    for item in items:
        normalized_items.append({
            "product_id": str(item.get("product_id", "")),
            "quantity": int(item.get("quantity", 1)),
            "price": float(item.get("price", 0.0))
        })

    tx["items"] = normalized_items

# ---------------------------------------------------------------------------
# Explicit schemas (KEEP THESE – THEY ARE CORRECT)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Create Spark DataFrames (NOW SAFE)
# ---------------------------------------------------------------------------

users_df = spark.createDataFrame(users_mongo, schema=user_schema)
transactions_df = spark.createDataFrame(transactions_mongo, schema=transaction_schema)

print("✓ MongoDB data successfully converted to Spark DataFrames")


# ============================================================================
# STEP 3: LOAD SESSION DATA (HBASE SIMULATION)
# ============================================================================
print("\n[STEP 3] Loading Session Data...")
print("-" * 80)

DATA_PATH = "/Users/honorinemnono/Desktop/BDA FINAL PROJECT/generateddata/"
sessions_df = spark.read.json(DATA_PATH + "sessions_0.json")

print(f"✓ Loaded {sessions_df.count():,} sessions")

# ============================================================================
# STEP 4: SESSION ENGAGEMENT METRICS
# ============================================================================
print("\n[STEP 4] Computing Session Engagement Metrics...")
print("-" * 80)

session_metrics = sessions_df.groupBy("user_id").agg(
    count("session_id").alias("total_sessions"),
    sum(when(col("conversion_status") == "converted", 1).otherwise(0)).alias("converted_sessions"),
    avg("duration_seconds").alias("avg_session_duration_sec"),
    sum(size("viewed_products")).alias("total_products_viewed")
).withColumn(
    "conversion_rate",
    round((col("converted_sessions") / col("total_sessions")) * 100, 2)
)

# ============================================================================
# STEP 5: TRANSACTION METRICS
# ============================================================================
print("\n[STEP 5] Computing Transaction Metrics...")
print("-" * 80)

transaction_metrics = transactions_df.groupBy("user_id").agg(
    count("transaction_id").alias("total_transactions"),
    round(sum("total"), 2).alias("total_spent"),
    round(avg("total"), 2).alias("avg_order_value")
)

# ============================================================================
# STEP 6: DATA INTEGRATION
# ============================================================================
print("\n[STEP 6] Integrating All Data Sources...")
print("-" * 80)

users_flat = users_df.select(
    "user_id",
    col("geo_data.state").alias("state"),
    col("geo_data.city").alias("city"),
    col("geo_data.country").alias("country")
)

integrated_df = users_flat \
    .join(transaction_metrics, "user_id", "left") \
    .join(session_metrics, "user_id", "left")

# ============================================================================
# STEP 7: CUSTOMER LIFETIME VALUE (CLV)
# ============================================================================
print("\n[STEP 7] Calculating CLV...")
print("-" * 80)

clv_df = integrated_df.withColumn(
    "engagement_score",
    round(
        coalesce(col("total_sessions"), lit(0)) * 0.3 +
        coalesce(col("converted_sessions"), lit(0)) * 0.5 +
        coalesce(col("total_products_viewed"), lit(0)) / 100 * 0.2,
        2
    )
).withColumn(
    "predicted_future_value",
    round(coalesce(col("avg_order_value"), lit(0)) * col("engagement_score") * 0.5, 2)
).withColumn(
    "total_clv",
    round(coalesce(col("total_spent"), lit(0)) + col("predicted_future_value"), 2)
).withColumn(
    "customer_segment",
    when((col("total_clv") > 1000) & (col("engagement_score") > 10), "VIP")
    .when((col("total_clv") > 500) & (col("engagement_score") > 5), "High Value")
    .when(col("total_clv") > 200, "Medium Value")
    .otherwise("Low Value")
)

print("\nTop 20 Customers by CLV:")
clv_df.orderBy(desc("total_clv")).show(20, truncate=False)

# ============================================================================
# STEP 8: SEGMENT SUMMARY
# ============================================================================
print("\n[STEP 8] Customer Segmentation Summary...")
print("-" * 80)

segment_summary = clv_df.groupBy("customer_segment").agg(
    count("user_id").alias("customers"),
    round(avg("total_clv"), 2).alias("avg_clv"),
    round(sum("total_spent"), 2).alias("historical_revenue"),
    round(sum("predicted_future_value"), 2).alias("predicted_revenue")
)

segment_summary.show(truncate=False)

# ============================================================================
# STEP 9: SAVE RESULTS
# ============================================================================
print("\n[STEP 9] Saving Results...")
print("-" * 80)

OUTPUT_PATH = "/Users/honorinemnono/Desktop/BDA FINAL PROJECT/SPARK/"

clv_df.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(OUTPUT_PATH + "clv_analysis")

segment_summary.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(OUTPUT_PATH + "segment_summary")

print("✓ Results saved successfully")

# ============================================================================
# CLEANUP
# ============================================================================
mongo_client.close()
spark.stop()

print("\n" + "=" * 80)
print("PART 3: ANALYTICS INTEGRATION COMPLETE!")
print("=" * 80)
