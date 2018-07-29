from pyspark.sql import SparkSession

# Create *local* Spark Session: Gateway to all distributed functionality
spark = SparkSession.builder\
	.appName(name="PySpark Intro")\
	.master("local[*]")\
	.getOrCreate()

# Eagerly read a CSV with header to determine full schema (with data types)
green_trips = spark.read\
    .option("header", "true")\
    .option("inferSchema", "false")\
    .csv("green_tripdata_2017-06.csv")

# Create a temporary "view" to allow arbitrary SQL commands 
green_trips.createOrReplaceTempView("green_trips")

# Run arbitrary SQL command to get hour of pickup time with sum of total revenue, ordered by pickup time hour
revenue_by_hour = spark.sql("""
SELECT hour(lpep_pickup_datetime), SUM(total_amount) AS total
FROM green_trips
GROUP BY hour(lpep_pickup_datetime)
ORDER BY hour(lpep_pickup_datetime) ASC""")

# Write revenue by hour results (24 rows) out to "green_revenue_by_hour" directory (multiple files)
revenue_by_hour.write.mode("overwrite").csv("green_revenue_by_hour")