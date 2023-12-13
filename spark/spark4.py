from pyspark.sql import SparkSession
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

# Вывод количества занятий по преподавателям, сгруппированные по видам занятий
print(df.groupBy(['lecturer', 'kindOfWork']).count().show(truncate=False))