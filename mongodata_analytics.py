# mongodata_analytics.py
import json
from pymongo import MongoClient
from collections import defaultdict

# --- MongoDB Connection ---
client = MongoClient("mongodb://localhost:27017")
db = client["ecommerce"]

# --- Aggregation 1: Top-Selling Products ---
def top_selling_products(limit=10):
    pipeline = [
        {"$unwind": "$items"},  # Break out line items
        {"$group": {"_id": "$items.product_id", "total_quantity_sold": {"$sum": "$items.quantity"}}},
        {"$sort": {"total_quantity_sold": -1}},
        {"$limit": limit}
    ]
    return list(db.transactions.aggregate(pipeline))

# --- Aggregation 2: Most Viewed Products ---
def most_viewed_products(limit=10):
    pipeline = [
        {"$unwind": "$page_views"},  # Flatten page_views array
        {"$match": {"page_views.product_id": {"$ne": None}}},
        {"$group": {"_id": "$page_views.product_id", "views": {"$sum": 1}}},
        {"$sort": {"views": -1}},
        {"$limit": limit}
    ]
    return list(db.sessions.aggregate(pipeline))

# --- Aggregation 3: Revenue by Category ---
def revenue_by_category(limit=10):
    pipeline = [
        {"$unwind": "$items"},  # Flatten items
        {"$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "product_info"
        }},
        {"$unwind": "$product_info"},
        {"$group": {
            "_id": "$product_info.category_id",
            "total_revenue": {"$sum": "$items.subtotal"}
        }},
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit}
    ]
    return list(db.transactions.aggregate(pipeline))

# --- Aggregation 4: User Segmentation by Purchase Frequency ---
def user_segmentation(limit=10):
    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "transactions_count": {"$sum": 1},
            "total_spent": {"$sum": "$total"}
        }},
        {"$sort": {"transactions_count": -1}},
        {"$limit": limit}
    ]
    return list(db.transactions.aggregate(pipeline))

# --- Main Execution ---
def main():
    print("\n🔥 Top Selling Products:")
    for p in top_selling_products():
        print(p)

    print("\n👀 Most Viewed Products:")
    for p in most_viewed_products():
        print(p)

    print("\n💰 Revenue by Category:")
    for r in revenue_by_category():
        print(r)

    print("\n📊 Top Users by Purchase Frequency:")
    for u in user_segmentation():
        print(u)

if __name__ == "__main__":
    main()
