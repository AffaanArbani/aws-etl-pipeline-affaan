from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CountJob").getOrCreate()

df = spark.read.option("header", True).csv(
    "s3://student-pipeline-affaan/processed/"
)

count = df.count()

result = spark.createDataFrame([(count,)], ["total_records"])

result.write.mode("overwrite").csv(
    "s3://student-pipeline-affaan/emr-output/"
)