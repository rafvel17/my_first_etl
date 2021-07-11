from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from constants import *

spark = SparkSession \
    .builder \
    .appName("my_first_etl") \
    .config("spark.jars", "../jars/postgresql-42.2.23.jar") \
    .getOrCreate()

# EXTRACT
students_df = spark.read.jdbc(url=DB_URL, table=STUDENTS_TABLE, properties=DB_PROPS)
subjects_df = spark.read.jdbc(url=DB_URL, table=SUBJECTS_TABLE, properties=DB_PROPS)
marks_df = spark.read.jdbc(url=DB_URL, table=MARKS_TABLE, properties=DB_PROPS)

# TRANSFORM
subjects_df = subjects_df \
    .withColumnRenamed("id", "subject_id") \
    .withColumnRenamed("name", "subject")

students_df = students_df \
    .withColumnRenamed("id", "student_id") \
    .withColumnRenamed("name", "student")

filtered_df = marks_df.filter(marks_df.subject_id == "E001")

joined_df = filtered_df \
    .join(subjects_df, "subject_id") \
    .join(students_df, "student_id")

sorted_df = joined_df.sort(f.desc("mark"))

result_df = sorted_df.select("subject_id", "subject", "student_id", "student", "mark")

# LOAD
result_df = result_df.coalesce(1)
result_df.write.csv("../output/result_df/", mode="overwrite")
