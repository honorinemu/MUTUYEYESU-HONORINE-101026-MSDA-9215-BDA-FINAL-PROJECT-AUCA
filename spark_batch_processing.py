# spark_batch_processing.py
"""
PART 2: APACHE SPARK BATCH PROCESSING & SQL ANALYTICS
======================================================
Big Data Analytics Final Project

This script demonstrates:
1. Data cleaning and normalization using Spark
2. Product recommendation calculation (bought together + viewed together)
3. Spark SQL analytics on e-commerce data
4. Cross-source analytics (MongoDB/HBase simulation)


"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("="*80)
print("PART 2: APACHE SPARK - BATCH PROCESSING & SQL ANALYTICS")
print("="*80)

# ============================================================================
# STEP 1: INITIALIZE SPARK SESSION
# ============================================================================
print("\n[STEP 1] Initializing Spark Session...")
print("-" * 80)

spark = SparkSession.builder \
    .appName("E-Commerce Product Recommendations") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✓ Spark Session created")
print(f"  Spark Version: {spark.version}")

# Paths
DATA_PATH = "/Users/honorinemnono/Desktop/BDA FINAL PROJECT/generateddata/"
OUTPUT_PATH = "/Users/honorinemnono/Desktop/BDA FINAL PROJECT/SPARK/"

# ============================================================================
# STEP 2: LOAD RAW DATA FROM JSON FILES
# ============================================================================
print("\n[STEP 2] Loading Raw Data...")
print("-" * 80)

print("Loading products...")
products_df = spark.read.json(DATA_PATH + "products.json")
print(f"✓ Products: {products_df.count():,}")

print("Loading users...")
users_df = spark.read.json(DATA_PATH + "users.json")
print(f"✓ Users: {users_df.count():,}")

print("Loading transactions...")
transactions_df = spark.read.json(DATA_PATH + "transactions.json")
print(f"✓ Transactions: {transactions_df.count():,}")

print("Loading categories...")
categories_df = spark.read.json(DATA_PATH + "categories.json")
print(f"✓ Categories: {categories_df.count():,}")

print("Loading sessions...")
sessions_df = spark.read.json(DATA_PATH + "sessions_0.json")
print(f"✓ Sessions: {sessions_df.count():,}")

# ============================================================================
# STEP 3: DATA CLEANING & NORMALIZATION
# ============================================================================
print("\n[STEP 3] Data Cleaning & Normalization...")
print("-" * 80)

# Clean Transactions
print("\n3.1 Cleaning Transactions...")
transactions_clean = transactions_df \
    .filter(col("user_id").isNotNull()) \
    .filter(col("items").isNotNull()) \
    .filter(size(col("items")) > 0) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("transaction_date", to_date(col("timestamp")))

print(f"  Before: {transactions_df.count():,}")
print(f"  After: {transactions_clean.count():,}")
print(f"  Removed: {transactions_df.count() - transactions_clean.count():,}")

# Clean Sessions
print("\n3.2 Cleaning Sessions...")
sessions_clean = sessions_df \
    .filter(col("user_id").isNotNull()) \
    .filter(col("session_id").isNotNull()) \
    .filter(col("viewed_products").isNotNull()) \
    .filter(size(col("viewed_products")) > 0) \
    .withColumn("start_time", to_timestamp(col("start_time"))) \
    .withColumn("end_time", to_timestamp(col("end_time")))

print(f"  Before: {sessions_df.count():,}")
print(f"  After: {sessions_clean.count():,}")

# Clean Products
print("\n3.3 Cleaning Products...")
products_clean = products_df \
    .filter(col("product_id").isNotNull()) \
    .filter(col("category_id").isNotNull()) \
    .filter(col("base_price").isNotNull()) \
    .withColumn("base_price", col("base_price").cast("double")) \
    .filter(col("base_price") > 0)

print(f"  Products cleaned: {products_clean.count():,}")

print("\n✓ Data cleaning complete!")

# ============================================================================
# STEP 4: PRODUCT RECOMMENDATION ANALYSIS
# ============================================================================
print("\n[STEP 4] Product Recommendation Analysis...")
print("-" * 80)

# 4.1: Products Bought Together
print("\n4.1 Products Frequently Bought Together...")

transaction_items = transactions_clean \
    .select("transaction_id", "user_id", explode("items").alias("item")) \
    .select("transaction_id", "user_id", col("item.product_id").alias("product_id"))

product_pairs = transaction_items.alias("a").join(
    transaction_items.alias("b"),
    (col("a.transaction_id") == col("b.transaction_id")) &
    (col("a.product_id") < col("b.product_id"))
).select(
    col("a.product_id").alias("product_a"),
    col("b.product_id").alias("product_b")
)

bought_together = product_pairs.groupBy("product_a", "product_b") \
    .agg(count("*").alias("bought_together_count")) \
    .orderBy(desc("bought_together_count"))

# Enrich with product names
bought_together_enriched = bought_together \
    .join(products_clean.select(col("product_id").alias("product_a"), 
                                 col("name").alias("product_a_name")), "product_a") \
    .join(products_clean.select(col("product_id").alias("product_b"),
                                 col("name").alias("product_b_name")), "product_b") \
    .select("product_a", "product_a_name", "product_b", "product_b_name", 
            "bought_together_count") \
    .orderBy(desc("bought_together_count"))

print("\nTop 10 Product Pairs (Bought Together):")
bought_together_enriched.show(10, truncate=50)

bought_together_enriched.cache()

# 4.2: Products Viewed Together
print("\n4.2 Products Frequently Viewed Together...")

viewed_products = sessions_clean \
    .select("session_id", "user_id", explode("viewed_products").alias("product_id"))

view_pairs = viewed_products.alias("a").join(
    viewed_products.alias("b"),
    (col("a.session_id") == col("b.session_id")) &
    (col("a.product_id") < col("b.product_id"))
).select(
    col("a.product_id").alias("product_a"),
    col("b.product_id").alias("product_b")
)

viewed_together = view_pairs.groupBy("product_a", "product_b") \
    .agg(count("*").alias("viewed_together_count")) \
    .orderBy(desc("viewed_together_count"))

viewed_together_enriched = viewed_together \
    .join(products_clean.select(col("product_id").alias("product_a"),
                                 col("name").alias("product_a_name")), "product_a") \
    .join(products_clean.select(col("product_id").alias("product_b"),
                                 col("name").alias("product_b_name")), "product_b") \
    .select("product_a", "product_a_name", "product_b", "product_b_name",
            "viewed_together_count") \
    .orderBy(desc("viewed_together_count"))

print("\nTop 10 Product Pairs (Viewed Together):")
viewed_together_enriched.show(10, truncate=50)

viewed_together_enriched.cache()

print("\n✓ Recommendation analysis complete!")

# ============================================================================
# STEP 5: SPARK SQL ANALYTICS
# ============================================================================
print("\n[STEP 5] Spark SQL Analytics...")
print("-" * 80)

# Register as SQL views
transactions_clean.createOrReplaceTempView("transactions")
products_clean.createOrReplaceTempView("products")
sessions_clean.createOrReplaceTempView("sessions")
users_df.createOrReplaceTempView("users")
categories_df.createOrReplaceTempView("categories")

# SQL Query 1: Product Performance (Sales + Views)
print("\n5.1 SQL Query: Product Performance Analysis")

sql_query_1 = """
WITH product_sales AS (
    SELECT 
        item.product_id,
        COUNT(DISTINCT transaction_id) as num_transactions,
        SUM(item.quantity) as total_quantity_sold,
        SUM(item.subtotal) as total_revenue
    FROM (
        SELECT transaction_id, explode(items) as item
        FROM transactions
    )
    GROUP BY item.product_id
),
product_views AS (
    SELECT
        product_id,
        COUNT(DISTINCT session_id) as num_sessions_viewed,
        COUNT(*) as total_views
    FROM (
        SELECT session_id, explode(viewed_products) as product_id
        FROM sessions
    )
    GROUP BY product_id
)
SELECT 
    p.product_id,
    p.name as product_name,
    p.base_price,
    c.name as category_name,
    COALESCE(ps.num_transactions, 0) as transactions,
    COALESCE(ps.total_quantity_sold, 0) as quantity_sold,
    COALESCE(ps.total_revenue, 0) as revenue,
    COALESCE(pv.total_views, 0) as total_views,
    CASE 
        WHEN pv.total_views > 0 THEN (ps.num_transactions / pv.total_views) * 100
        ELSE 0 
    END as conversion_rate
FROM products p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id
LEFT JOIN product_views pv ON p.product_id = pv.product_id
LEFT JOIN categories c ON p.category_id = c.category_id
WHERE ps.total_revenue IS NOT NULL OR pv.total_views IS NOT NULL
ORDER BY revenue DESC
LIMIT 20
"""

product_performance = spark.sql(sql_query_1)
print("\nTop 20 Products by Performance:")
product_performance.show(20, truncate=False)

# SQL Query 2: User Behavior Analysis
print("\n5.2 SQL Query: User Purchase Behavior")

sql_query_2 = """
SELECT 
    u.user_id,
    u.geo_data.state as state,
    COUNT(DISTINCT t.transaction_id) as total_purchases,
    SUM(t.total) as total_spent,
    AVG(t.total) as avg_order_value,
    COUNT(DISTINCT s.session_id) as total_sessions,
    CASE 
        WHEN COUNT(DISTINCT t.transaction_id) >= 5 THEN 'High Value'
        WHEN COUNT(DISTINCT t.transaction_id) >= 2 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment
FROM users u
LEFT JOIN transactions t ON u.user_id = t.user_id
LEFT JOIN sessions s ON u.user_id = s.user_id
GROUP BY u.user_id, u.geo_data.state
HAVING total_purchases > 0
ORDER BY total_spent DESC
LIMIT 20
"""

user_behavior = spark.sql(sql_query_2)
print("\nTop 20 Users by Spending:")
user_behavior.show(20, truncate=False)

# SQL Query 3: Conversion Funnel
print("\n5.3 SQL Query: Session Funnel Analysis")

sql_query_3 = """
SELECT 
    conversion_status,
    COUNT(*) as session_count,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(duration_seconds / 60.0) as avg_duration_minutes,
    AVG(size(viewed_products)) as avg_products_viewed,
    device_profile.type as device_type
FROM sessions
GROUP BY conversion_status, device_profile.type
ORDER BY conversion_status, session_count DESC
"""

funnel_analysis = spark.sql(sql_query_3)
print("\nConversion Funnel by Device:")
funnel_analysis.show(20, truncate=False)

print("\n✓ Spark SQL analytics complete!")

# ============================================================================
# STEP 6: SAVE RESULTS
# ============================================================================
print("\n[STEP 6] Saving Results...")
print("-" * 80)

# Save recommendations
bought_together_enriched.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(OUTPUT_PATH + "product_recommendations_bought")
print("✓ Saved: product_recommendations_bought/")

viewed_together_enriched.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(OUTPUT_PATH + "product_recommendations_viewed")
print("✓ Saved: product_recommendations_viewed/")

# Save SQL results
product_performance.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(OUTPUT_PATH + "product_performance_analysis")
print("✓ Saved: product_performance_analysis/")

user_behavior.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(OUTPUT_PATH + "user_behavior_analysis")
print("✓ Saved: user_behavior_analysis/")

funnel_analysis.coalesce(1).write.mode("overwrite") \
    .option("header", "true").csv(OUTPUT_PATH + "session_funnel_analysis")
print("✓ Saved: session_funnel_analysis/")

print("\n✓ All results saved!")

# ============================================================================
# STEP 7: SUMMARY
# ============================================================================
print("\n" + "="*80)
print("PART 2 COMPLETE!")
print("="*80)

print("""
DELIVERABLES GENERATED:
✓ Data cleaning & normalization
✓ Product recommendations (bought + viewed together)
✓ 3 Spark SQL queries (performance, behavior, funnel)
✓ Results saved to CSV files

FILES CREATED:
- product_recommendations_bought/
- product_recommendations_viewed/
- product_performance_analysis/
- user_behavior_analysis/
- session_funnel_analysis/
""")

spark.stop()
print("✓ Spark session stopped")