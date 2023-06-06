from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType


spark = SparkSession.builder.master("local[1]") \
    .appName("Create subscriptions") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


subscriptions= [
  {"subscription": "Basic", "numberOfChannels": 50, "extras": {}},
  {"subscription": "Premium", "numberOfChannels": 100, "extras": {"HBO": "4", "Cinemax": "3"}},
  {"subscription": "Ultimate", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3", "Sports Package": "2"}},
  {"subscription": "Sports", "numberOfChannels": 75, "extras": {"Sports Package": "5"}},
  {"subscription": "Entertainment", "numberOfChannels": 75, "extras": {"Showtime": "4", "Kids Package": "3"}},
  {"subscription": "News", "numberOfChannels": 50, "extras": {"CNN": "5", "Fox News": "3"}},
  {"subscription": "Movies", "numberOfChannels": 100, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3"}},
  {"subscription": "Family", "numberOfChannels": 75, "extras": {"Kids Package": "5", "DVR": "3"}},
  {"subscription": "Premium Plus", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "4", "Sports Package": "3", "DVR": "2"}},
  {"subscription": "Custom", "numberOfChannels": 150, "extras": {}}
]

schemaSubscriptions = StructType([
  StructField("subscription", StringType(), False),
  StructField("numberOfChannels", LongType(), False),
  StructField("extras", MapType(StringType(), StringType(), valueContainsNull = True), nullable = True)
])

df = spark.createDataFrame(subscriptions, schemaSubscriptions)
df.show()
df.write.mode("overwrite").parquet(f'/opt/airflow/data/files/subscriptions')