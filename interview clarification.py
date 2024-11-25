# Databricks notebook source

# Initialize Spark Session
# spark = SparkSession.builder.appName("DatabricksOptimization").getOrCreate()

# Load JSON file
customers_df = spark.read.json("dbfs:/FileStore/customer.json")

# Load CSV file
transactions_df = spark.read.csv("dbfs:/FileStore/transactions.csv", header=True, inferSchema=True)

# Display data
customers_df.show()
transactions_df.show()


# COMMAND ----------

joined_df = customers_df.join(transactions_df,on = 'customer_id' , how = 'inner')
joined_df.show()

# COMMAND ----------

# Calculate total transaction amount for each customer

amount_df = joined_df.groupBy("customer_id", "name").sum("amount")
amount_df_order = amount_df.orderBy("name")
amount_df_order.show()

# COMMAND ----------

# Simulate the same above sum operation using RDDs.
# We will be using the RDD \
transactions_rdd = transactions_df.rdd.map(lambda row: (row.customer_id, row.amount))
print(transactions_rdd.collect())
reduced_rdd = transactions_rdd.reduceByKey(lambda x, y: x + y)
print(reduced_rdd.collect())



# COMMAND ----------

# Repartition and Coalesce
# 1.	Repartition: Use when increasing partitions for parallelism.

repartitioned_df = transactions_df.repartition(4)  # Increase partitions
repartitioned_df.display()



# COMMAND ----------

# 2.	Coalesce: Use to reduce partitions to optimize resource usage.

coalesced_df = transactions_df.coalesce(1)  # Reduce to 1 partition
coalesced_df.display()

# COMMAND ----------

# Caching : Cache the joined DataFrame to speed up iterative actions.

joined_df.cache()
joined_df.count()  # Trigger cache


# COMMAND ----------

# Serialization : Use Kryo serialization for faster data transfer.

spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


# COMMAND ----------

# Broadcast Join : Broadcast smaller datasets for faster joins.

from pyspark.sql.functions import broadcast

broadcasted_df = transactions_df.join(broadcast(customers_df), "customer_id")
broadcasted_df.show()


