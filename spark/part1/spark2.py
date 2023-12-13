from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
import os

path = 'files'
spark = SparkSession.builder.getOrCreate()
df = ''
for file in os.listdir(path):
    print(file)
    df1 = spark.read.json(path + "\\" + file, multiLine=True)
    if df:
        df = df.union(df1.select(['lecturer', 'discipline', 'auditorium', 'kindOfWork']))
    else:
        df = df1.select(['lecturer', 'discipline', 'auditorium', 'kindOfWork'])

w = Window.partitionBy('lecturer')

# Вывод дисциплин, которые преподаватель ведет чаще всего
print(df.groupBy(['lecturer', 'discipline'])\
      .count()\
      .withColumn('maxCnt', f.max('count').over(w))\
      .where(f.col('count') == f.col('maxCnt'))\
      .drop('maxCnt')\
      .show(truncate=False)
      )
