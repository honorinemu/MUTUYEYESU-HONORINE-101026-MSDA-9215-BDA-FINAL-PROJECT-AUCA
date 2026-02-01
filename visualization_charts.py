# =============================================================================
# VISUALIZATION CHARTS & SIMPLE ANALYTICS
# =============================================================================

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os

sns.set(style="whitegrid")
BASE_OUTPUT = "output"

# -----------------------------
# Helper function to load Spark CSV output
# -----------------------------
def load_spark_csv(folder_path):
    csv_file = glob.glob(os.path.join(folder_path, "*.csv"))[0]
    return pd.read_csv(csv_file)

# =============================================================================
# 1️⃣ Sales Over Time
# =============================================================================
sales_over_time = load_spark_csv(os.path.join(BASE_OUTPUT, "sales_over_time"))

# Plot
plt.figure(figsize=(12,6))
sns.lineplot(data=sales_over_time, x="date", y="daily_sales", marker="o")
plt.title("Daily Sales Over Time")
plt.xlabel("Date")
plt.ylabel("Daily Sales ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/sales_over_time_chart.png")
plt.show()

# Simple analytics
total_sales = sales_over_time['daily_sales'].sum()
avg_daily_sales = sales_over_time['daily_sales'].mean()
print(f"Total Sales: ${total_sales:,.2f}")
print(f"Average Daily Sales: ${avg_daily_sales:,.2f}")

# =============================================================================
# 2️⃣ Customer Spending Distribution
# =============================================================================
customer_spending = load_spark_csv(os.path.join(BASE_OUTPUT, "customer_spending"))

plt.figure(figsize=(12,6))
sns.histplot(customer_spending['total_spent'], bins=50, kde=True)
plt.title("Customer Spending Distribution")
plt.xlabel("Total Spent ($)")
plt.ylabel("Number of Customers")
plt.tight_layout()
plt.savefig("output/customer_spending_chart.png")
plt.show()

# Simple analytics
top_spender = customer_spending.loc[customer_spending['total_spent'].idxmax()]
print(f"Top Customer: {top_spender['user_id']} spent ${top_spender['total_spent']:,.2f}")
median_spent = customer_spending['total_spent'].median()
print(f"Median Customer Spending: ${median_spent:,.2f}")

# =============================================================================
# 3️⃣ Top-Selling Products
# =============================================================================
top_products = load_spark_csv(os.path.join(BASE_OUTPUT, "top_products"))

plt.figure(figsize=(12,6))
sns.barplot(
    data=top_products.head(15),
    x="product_id",
    y="units_sold",
    palette="viridis"
)
plt.title("Top 15 Products by Units Sold")
plt.xlabel("Product ID")
plt.ylabel("Units Sold")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/top_products_chart.png")
plt.show()

# Simple analytics
best_product = top_products.iloc[0]
print(f"Best-selling Product: {best_product['product_id']} ({best_product['units_sold']} units sold)")

# =============================================================================
# 4️⃣ Conversion Funnel
# =============================================================================
conversion_funnel = load_spark_csv(os.path.join(BASE_OUTPUT, "conversion_funnel"))

plt.figure(figsize=(8,5))
sns.barplot(data=conversion_funnel, x="stage", y="users", palette="coolwarm")
plt.title("Conversion Funnel")
plt.xlabel("Funnel Stage")
plt.ylabel("Number of Users")
plt.tight_layout()
plt.savefig("output/conversion_funnel_chart.png")
plt.show()

# Simple analytics
viewed = conversion_funnel.loc[conversion_funnel['stage']=="Viewed Product", 'users'].values[0]
purchased = conversion_funnel.loc[conversion_funnel['stage']=="Purchased", 'users'].values[0]
conversion_rate = (purchased / viewed) * 100
print(f"Conversion Rate: {conversion_rate:.2f}% from Viewed Product → Purchased")

print("\n✓ All charts created successfully in /output folder")
