from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.functions import max, col, lit, row_number
import os
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType, BooleanType, StringType, DateType, LongType, MapType
from pyspark.sql.functions import col,lit

SCHEMA_REGISTERS_READ = StructType([
        StructField("id", LongType(), True),
        StructField("active", BooleanType(), True),
        StructField("subscription", StringType(), True),
        StructField("customer_first_name", StringType(), True),
        StructField("customer_last_name", StringType(), True),
        StructField("cost", LongType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("date", DateType(), True)
    ])

SCHEMA_REGISTERS_ENRICHED = StructType([
    StructField("id", LongType(), True),
    StructField("active", BooleanType(), True),
    StructField("subscription", StringType(), True),
    StructField("customer_first_name", StringType(), True),
    StructField("customer_last_name", StringType(), True),
    StructField("cost", LongType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("numberOfChannels", LongType(), True),
    StructField("extras", MapType(StringType(), StringType(), valueContainsNull = True), nullable = True),
    StructField("date", DateType(), True)
])

def getMaxDateSnapshots(path,spark):
  dfsnapshots = spark.read.schema(SCHEMA_REGISTERS_ENRICHED).parquet(path)
  dfsnapshots.show(n=1000, truncate=False)

  datessnapshots = dfsnapshots.select("date").distinct()
  datessnapshots.show()

  maxDatesnapshots = datessnapshots.agg(max('date')).collect()[0][0]
  print('Max date snapshots: ',maxDatesnapshots)

  return maxDatesnapshots

def readMaxDateSnapshoots(path,maxDatesnapshots,spark):
  snapshoot = spark.read.schema(SCHEMA_REGISTERS_ENRICHED).parquet(f'{path}/date={maxDatesnapshots.strftime("%Y-%m-%d")}')
  snapshoot.show()
  return snapshoot

def getSubscriptions(path,spark):
  subscriptions = spark.read.parquet(path)
  subscriptions.show(n=1000, truncate=False)

  return subscriptions


def getMaxDateRegisters(path,spark):
  registers = spark.read.schema(SCHEMA_REGISTERS_READ).json(path)
  registers.show(n=1000, truncate=False)

  distinct_dates = registers.select("date").distinct()
  distinct_dates.show()

  dateData = distinct_dates.agg(max('date')).collect()[0][0]
  print('Date data json: ', dateData)

  return (dateData, registers)

def enrichNewRegisters(subscriptions,maxDatesnapshots,spark,path):
  registers = spark.read.schema(SCHEMA_REGISTERS_READ).json(path)
  registers = registers.filter(col('date') > maxDatesnapshots)

  joined = registers.join(subscriptions, ['subscription'])
  joined = joined.select('id',"active","subscription","customer_first_name","customer_last_name","cost","start_date","end_date","numberOfChannels","extras","date").orderBy("id")
  joined.show()

  print(registers.schema)
  print(joined.schema)

  return joined

def joinData(snapshots,joined):
  snapshots.show()
  result = snapshots.union(joined)
  result.show()
  return result
  
def createNewSnapshot(data,dateData,path):
  result = (data
                .withColumn("rowNumber", row_number().over(Window.partitionBy(col("id")).orderBy(col("date").desc())))
                .where(col("rowNumber") == lit(1))
                .drop("rowNumber"))
  
  result.show(n=1000, truncate=False)

  result = result.drop('date')
  result.write.mode("overwrite").format('parquet').save(f'{path}/date={dateData.strftime("%Y-%m-%d")}')
  return result

def enrichData(registers,subscription):

  joined = registers.join(subscription, ['subscription'])
  joined.show(n=1000, truncate=False)

  return joined

def main():
    spark = SparkSession.builder.master("local[1]") \
        .appName("Create snapshot") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    snapshotsPath = "/opt/airflow/data/files/snapshots"
    subscriptionsPath = "/opt/airflow/data/files/subscriptions"
    registersPath = "/opt/airflow/data/files/registers"

    if os.listdir("/opt/airflow/data/files/snapshots"):

        ###get max date snapshots
        maxDatesnapshots = getMaxDateSnapshots(snapshotsPath,spark)

        ###read max snapshots
        snapshoot = readMaxDateSnapshoots(snapshotsPath,maxDatesnapshots,spark)

        if os.listdir("/opt/airflow/data/files/registers"):

            #####get subscriptions
            subscriptions = getSubscriptions(subscriptionsPath,spark)

            ###get max date registers
            dateData,_ = getMaxDateRegisters(registersPath,spark)

            if (dateData > maxDatesnapshots):
                ###enrich new registers
                joined = enrichNewRegisters(subscriptions,maxDatesnapshots,spark,registersPath)

                ###Make union past snapshoot with new enriched registers
                latestData = joinData(snapshoot,joined)


                ###create new snapshoot
                createNewSnapshot(latestData,dateData,snapshotsPath)
            else:
                print('snapshots Up to date')

        else:
            print('ERROR: No data .json')

    else:
        if os.listdir("/opt/airflow/data/files/registers"):

            #####get subscriptions
            subscriptions = getSubscriptions(subscriptionsPath,spark)

            ###get max date registers
            dateData,registers = getMaxDateRegisters(registersPath,spark)

            ##enrich data
            joined = enrichData(registers,subscriptions)

            ###create new snapshoot
            createNewSnapshot(joined,dateData,snapshotsPath)

if __name__ == "__main__":
    main()