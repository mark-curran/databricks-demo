"""
Implementation of the quickstart Spark Connect guide:

https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_connect.html
"""

from datetime import date, datetime

from pyspark.sql import Row, SparkSession

SPARK_REMOTE = "sc://localhost:15002"
spark = SparkSession.builder.remote(SPARK_REMOTE).appName("simpleApp").getOrCreate()


df = spark.createDataFrame(
    [Row(a=1, b=2, c="string1", d=date(2025, 1, 1), e=datetime(2025, 1, 1, 12, 0))]
)

df.show()
