from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count , lower, collect_list, concat_ws, desc, min, coalesce

spark = SparkSession \
    .builder \
    .config("spark.driver.maxResultSize",  "0") \
    .appName("01_staff-graded-assignment") \
    .master("yarn") \
    .getOrCreate()

clickstream = spark.read.csv("hdfs:///data/lsml/sga/clickstream.csv"
                                   , inferSchema = True
                                   , header=True
                                   , sep="\t")

partitionCols = ["user_id", "session_id"]

min_ts = clickstream \
        .where(lower(col("event_type")).like("%error%")) \
        .groupby(partitionCols).agg(min(col("timestamp")).alias("ts"))

clickstream.join(min_ts,
   on=[
     clickstream.user_id == min_ts.user_id, 
     clickstream.session_id == min_ts.session_id
      ]
                 , how='left') \
        .select(clickstream.user_id, clickstream.session_id, 'event_type', 'event_page', 'timestamp', coalesce('ts','timestamp').alias("ts") )\
        .filter(col("timestamp") <= col("ts")) \
        .where(col("event_type") == "page") \
        .sort(partitionCols + ["timestamp"]) \
        .groupby(partitionCols).agg(concat_ws("-", collect_list("event_page")).alias("path")) \
        .groupby("path").agg(count("session_id").alias("cnt")) \
        .orderBy(desc("cnt")) \
        .show(30)

