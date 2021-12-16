from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

spark = SparkSession.builder \
    .master("local") \
    .appName("Books DataFrame") \
    .getOrCreate()

books_schema = StructType([
    StructField("bookID", IntegerType()),
    StructField("title", StringType()),
    StructField("authors", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("isbn", StringType()),
    StructField("isbn13", StringType()),
    StructField("language_code", StringType()),
    StructField("num_pages", IntegerType()),
    StructField("ratings_count", LongType()),
    StructField("text_reviews_count", LongType()),
    StructField("publication_date", StringType()),
    StructField("publisher", StringType())
])

books_df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(books_schema) \
    .csv("D:/Documentos/Spark/pyspark/Books/main/resources/books.csv")

books_df.printSchema()

total_books_df = books_df.count()
print(f"Total of books: {total_books_df}")

print("")
print("Best Sellers of All Time:")
best_sellers_df = books_df \
    .filter(col("ratings_count") > 100_000) \
    .orderBy(col("ratings_count").desc()) \
    .cache()

best_sellers_df.show(10)

print("")
print("Critically Acclaimed Books:")
best_ratings_df = books_df \
    .filter(col("average_rating") > 4.5) \
    .orderBy(col("average_rating").desc()) \
    .cache()

best_ratings_df.show(10)

print("")
print("Non English Best Sellers:")
best_sellers_other_language_df = books_df \
    .filter((col("ratings_count") > 10_000)
            & (col("language_code") != "eng") \
            & (col("language_code") != "en-US") \
            & (col("language_code") != "en-GB")) \
    .orderBy(col("ratings_count").desc()) \
    .cache() \

best_sellers_other_language_df.show(10)

print("")
print("Non English Critically Acclaimed Books:")
best_ratings_other_language_df = best_ratings_df \
    .select("bookID", "title", "authors", "average_rating", "language_code") \
    .filter((col("language_code") != "eng") \
            & (col("language_code") != "en-US") \
            & (col("language_code") != "en-GB")
            & (col("language_code") != "zho") \
            & (col("language_code") != "jpn")) \
    .orderBy(col("average_rating").desc()) \
    .cache()

best_ratings_other_language_df.show(10)
