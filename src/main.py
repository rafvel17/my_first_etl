from pyspark.sql import SparkSession

DB_URL = "jdbc:postgresql:postgres"
STUDENTS_TABLE = "university.students"

DB_PROPS = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver",
}

spark = SparkSession \
    .builder \
    .appName("my_first_etl") \
    .config("spark.jars", "../jars/postgresql-42.2.23.jar") \
    .getOrCreate()

students_df = spark.read.jdbc(url=DB_URL, table=STUDENTS_TABLE, properties=DB_PROPS)
students_df.show()
