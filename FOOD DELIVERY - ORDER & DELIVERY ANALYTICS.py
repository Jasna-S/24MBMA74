# Databricks notebook source
from pyspark.sql import functions as F

# Read your dataset (already given path)
df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv("/Volumes/workspace/default/usecaseinternal/FOOD DELIVERY SAMPLE DATASET - USE THIS FOR ABOVE EXCEL.csv"))

# Cast order_value to numeric if needed
df = df.withColumn("order_value", F.col("order_value").cast("double"))

# --- PySpark Core: total order value per customer ---
total_order_value_per_customer = (
    df.groupBy("customer_id")
      .agg(F.sum("order_value").alias("total_order_value"),
           F.count("*").alias("num_orders"))
      .orderBy(F.desc("total_order_value"))
)

# Show results
display(total_order_value_per_customer)


# COMMAND ----------

from pyspark.sql import functions as F

# Read dataset
df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv("/Volumes/workspace/default/usecaseinternal/FOOD DELIVERY SAMPLE DATASET - USE THIS FOR ABOVE EXCEL.csv"))

# Cast delivery_time to numeric
df = df.withColumn("delivery_time", F.col("delivery_time").cast("int"))

# --- PySpark Core: average delivery time per restaurant ---
avg_delivery_time_per_restaurant = (
    df.filter(F.lower(F.trim(F.col("status"))) == "delivered")   # consider only delivered orders
      .groupBy("restaurant_id")
      .agg(F.avg("delivery_time").alias("avg_delivery_time_mins"),
           F.count("*").alias("delivered_orders"))
      .orderBy("avg_delivery_time_mins")
)

# Show results
display(avg_delivery_time_per_restaurant)


# COMMAND ----------

# Create Temp View
df.createOrReplaceTempView("orders")

# Adjusted SQL: Identify customers with frequent cancellations
frequent_cancellations = spark.sql("""
SELECT
  customer_id,
  COUNT(*) AS total_orders,
  SUM(CASE WHEN lower(trim(status)) LIKE '%cancel%' THEN 1 ELSE 0 END) AS cancellations,
  ROUND(SUM(CASE WHEN lower(trim(status)) LIKE '%cancel%' THEN 1 ELSE 0 END) / COUNT(*), 2) AS cancellation_rate
FROM orders
GROUP BY customer_id
HAVING cancellations >= 1   -- change to >=2 if your data has more cancellations
ORDER BY cancellations DESC, cancellation_rate DESC
""")

display(frequent_cancellations)


# COMMAND ----------

# Create Temp View (if not already created)
df.createOrReplaceTempView("orders")

# PySpark SQL: Peak order hours
peak_order_hours = spark.sql("""
SELECT
  hour(timestamp) AS hour_of_day,
  COUNT(*) AS total_orders,
  SUM(CASE WHEN lower(trim(status)) LIKE '%cancel%' THEN 1 ELSE 0 END) AS cancelled_orders
FROM orders
GROUP BY hour(timestamp)
ORDER BY total_orders DESC
""")

# Show result
display(peak_order_hours)


# COMMAND ----------

display(total_order_value_per_customer)
display(avg_delivery_time_per_restaurant)
display(frequent_cancellations)
display(peak_order_hours)


# COMMAND ----------

top_customers = total_order_value_per_customer.limit(10).toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(10,5))
plt.bar(top_customers["customer_id"], top_customers["total_order_value"], color="skyblue")
plt.title("Top 10 Customers by Total Order Value")
plt.xlabel("Customer ID")
plt.ylabel("Total Order Value")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

fastest_restaurants = avg_delivery_time_per_restaurant.orderBy("avg_delivery_time_mins").limit(10).toPandas()

plt.figure(figsize=(10,5))
plt.bar(fastest_restaurants["restaurant_id"], fastest_restaurants["avg_delivery_time_mins"], color="lightgreen")
plt.title("Top 10 Fastest Restaurants (Avg Delivery Time)")
plt.xlabel("Restaurant ID")
plt.ylabel("Avg Delivery Time (mins)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

cancel_customers = frequent_cancellations.toPandas()

plt.figure(figsize=(10,5))
plt.bar(cancel_customers["customer_id"], cancel_customers["cancellations"], color="salmon")
plt.title("Customers with Frequent Cancellations")
plt.xlabel("Customer ID")
plt.ylabel("Number of Cancellations")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

orders_by_hour = peak_order_hours.orderBy("hour_of_day").toPandas()

plt.figure(figsize=(9,5))
plt.plot(orders_by_hour["hour_of_day"], orders_by_hour["total_orders"], marker="o", color="purple")
plt.title("Orders by Hour of Day")
plt.xlabel("Hour of Day (0-23)")
plt.ylabel("Total Orders")
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()
