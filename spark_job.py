from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext

spark = SparkSession.builder \
  .appName('Jupyter BigQuery Storage')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()


df_co = spark.read.option("header",True).csv("gs://rawfiles-287823/rawfiles/co.csv")
df_co = df_co.withColumn("County_Id", F.concat(F.col("State Code"),F.lit('-'), F.col("County Code")))
df_co = df_co.select('County_Id','State Name','County Name', 'Latitude', 'Longitude', 'Date GMT', 'Time GMT', col('Sample Measurement').alias('co'))

df_no2 = spark.read.option("header",True).csv("gs://rawfiles-287823/rawfiles/no2.csv")
df_no2 = df_no2.withColumn("County_Id", F.concat(F.col("State Code"),F.lit('-'), F.col("County Code")))
df_no2 = df_no2.select('County_Id','State Name','County Name', 'Latitude', 'Longitude', 'Date GMT', 'Time GMT', col('Sample Measurement').alias('no2'))

df_o3 = spark.read.option("header",True).csv("gs://rawfiles-287823/rawfiles/ozone.csv")
df_o3 = df_o3.withColumn("County_Id", F.concat(F.col("State Code"),F.lit('-'), F.col("County Code")))
df_o3 = df_o3.select('County_Id','State Name','County Name', 'Latitude', 'Longitude', 'Date GMT', 'Time GMT', col('Sample Measurement').alias('o3'))

df_so2 = spark.read.option("header",True).csv("gs://rawfiles-287823/rawfiles/so2.csv")
df_so2 = df_so2.withColumn("County_Id", F.concat(F.col("State Code"),F.lit('-'), F.col("County Code")))
df_so2 = df_so2.select('County_Id','State Name','County Name', 'Latitude', 'Longitude', 'Date GMT', 'Time GMT', col('Sample Measurement').alias('so2'))

df_pm25 = spark.read.option("header",True).csv("gs://rawfiles-287823/rawfiles/pm25.csv")
df_pm25 = df_pm25.withColumn("County_Id", F.concat(F.col("State Code"),F.lit('-'), F.col("County Code")))
df_pm25 = df_pm25.select('County_Id','State Name','County Name', 'Latitude', 'Longitude', 'Date GMT', 'Time GMT', col('Sample Measurement').alias('pm25'))

df_pm10 = spark.read.option("header",True).csv("gs://rawfiles-287823/rawfiles/pm10.csv")
df_pm10 = df_pm10.withColumn("County_Id", F.concat(F.col("State Code"),F.lit('-'), F.col("County Code")))
df_pm10 = df_pm10.select('County_Id','State Name','County Name', 'Latitude', 'Longitude', 'Date GMT', 'Time GMT', col('Sample Measurement').alias('pm10'))


month = ['01','02','03','04','05','06','07','08','09','10','11','12']
m_31 = ['01','03','05','07','08','10','12']
m_30 = ['04','06','09','11']
time = ['00:00', '01:00', '02:00', '03:00', '04:00','05:00','06:00','07:00','08:00','09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00','18:00','19:00','20:00','21:00','22:00','23:00']
for m in month:
    if m in m_31:
        days = 31
    elif m in m_30:
        days = 30
    else:
        days = 28
    for d in range(1, 3):
        if (m == '01') & (d == 1):
            continue
        else:
            for t in time:
                if d < 10:
                    day_value = "-0"+str(d)
                else:
                    day_value = "-"+str(d)
                    
                df_co_temp = df_co.filter((df_co['Date GMT'] == "2019-"+m+day_value) & (df_co['Time GMT'] == t))
                df_co_temp = df_co_temp.select('County_Id', 'State Name', 'County Name', 'Latitude', 'Longitude',  'co')
                df_co_lat = df_co_temp.dropDuplicates(['County_Id'])
                df_co_lat = df_co_lat.select('County_Id', 'Latitude', 'Longitude')
                df_co_temp = df_co_temp.groupBy('County_Id', 'State Name', 'County Name').agg(F.mean('co'))


                df_no2_temp = df_no2.filter((df_no2['Date GMT'] == "2019-"+m+day_value) & (df_no2['Time GMT'] == t))
                df_no2_temp = df_no2_temp.select('County_Id', 'State Name', 'County Name', 'Latitude', 'Longitude', 'no2')
                df_no2_lat = df_no2_temp.dropDuplicates(['County_Id'])
                df_no2_lat = df_no2_lat.select('County_Id', 'Latitude', 'Longitude')
                df_no2_temp = df_no2_temp.groupBy('County_Id', 'State Name', 'County Name').agg(F.mean('no2'))
                df_lat_long = df_no2_lat.unionAll(df_co_lat)
                df_lat_long = df_lat_long.dropDuplicates(['County_Id'])


                df_o3_temp = df_o3.filter((df_o3['Date GMT'] == "2019-"+m+day_value) & (df_o3['Time GMT'] == t))
                df_o3_temp = df_o3_temp.select('County_Id', 'State Name', 'County Name', 'Latitude', 'Longitude', 'o3')
                df_o3_lat = df_o3_temp.dropDuplicates(['County_Id'])
                df_o3_lat = df_o3_lat.select('County_Id', 'Latitude', 'Longitude')
                df_o3_temp = df_o3_temp.groupBy('County_Id', 'State Name', 'County Name').agg(F.mean('o3'))
                df_lat_long = df_lat_long.unionAll(df_o3_lat)
                df_lat_long = df_lat_long.dropDuplicates(['County_Id'])


                df_so2_temp = df_so2.filter((df_so2['Date GMT'] == "2019-"+m+day_value) & (df_so2['Time GMT'] == t))
                df_so2_temp = df_so2_temp.select('County_Id', 'State Name', 'County Name', 'Latitude', 'Longitude', 'so2')
                df_so2_lat = df_so2_temp.dropDuplicates(['County_Id'])
                df_so2_lat = df_so2_lat.select('County_Id', 'Latitude', 'Longitude')
                df_so2_temp = df_so2_temp.groupBy('County_Id', 'State Name', 'County Name').agg(F.mean('so2'))
                df_lat_long = df_lat_long.unionAll(df_so2_lat)
                df_lat_long = df_lat_long.dropDuplicates(['County_Id'])


                df_pm25_temp = df_pm25.filter((df_pm25['Date GMT'] == "2019-"+m+day_value) & (df_pm25['Time GMT'] == t))
                df_pm25_temp = df_pm25_temp.select('County_Id', 'State Name', 'County Name', 'Latitude', 'Longitude', 'pm25')
                df_pm25_lat = df_pm25_temp.dropDuplicates(['County_Id'])
                df_pm25_lat = df_pm25_lat.select('County_Id', 'Latitude', 'Longitude')
                df_pm25_temp = df_pm25_temp.groupBy('County_Id', 'State Name', 'County Name').agg(F.mean('pm25'))
                df_lat_long = df_lat_long.unionAll(df_pm25_lat)
                df_lat_long = df_lat_long.dropDuplicates(['County_Id'])


                df_pm10_temp = df_pm10.filter((df_pm10['Date GMT'] == "2019-"+m+day_value) & (df_pm10['Time GMT'] == t))
                df_pm10_temp = df_pm10_temp.select('County_Id', 'State Name', 'County Name', 'Latitude', 'Longitude', 'pm10')
                df_pm10_lat = df_pm10_temp.dropDuplicates(['County_Id'])
                df_pm10_lat = df_pm10_lat.select('County_Id', 'Latitude', 'Longitude')
                df_pm10_temp = df_pm10_temp.groupBy('County_Id', 'State Name', 'County Name').agg(F.mean('pm10'))
                df_lat_long = df_lat_long.unionAll(df_pm10_lat)
                df_lat_long = df_lat_long.dropDuplicates(['County_Id'])


                df_temp = df_co_temp.join(df_no2_temp, ['County_Id', 'State Name', 'County Name'], how='full')\
                .join(df_o3_temp, ['County_Id', 'State Name', 'County Name'], how='full')\
                .join(df_so2_temp, ['County_Id', 'State Name', 'County Name'], how='full')\
                .join(df_pm10_temp, ['County_Id', 'State Name', 'County Name'], how='full')\
                .join(df_pm25_temp, ['County_Id', 'State Name', 'County Name'], how='full')
                
                df_temp = df_temp.select(col("County_Id").alias("county_id"), col("State Name").alias("State"), col("County Name").alias("County"), col("avg(co)").alias("co"), col("avg(no2)").alias("no2"), col("avg(o3)").alias("o3"), col("avg(so2)").alias("so2"), col("avg(pm10)").alias("pm10"), col("avg(pm25)").alias("pm25"))
                df_lat_long = df_lat_long.select(col("County_Id").alias("county_id"), col("Latitude").alias("latitude"), col("Longitude").alias("longitude"))

                df = df_temp.join(df_lat_long, on='county_id', how='inner')

                filepath = "gs://hourly_files_287823/2019-"+m+day_value+"-"+t
                df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(filepath)

