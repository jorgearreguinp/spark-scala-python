package lectures

import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object BooksDF extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Books DataFrame")
      .getOrCreate()

    val booksSchema = StructType(List(
      StructField("bookID", StringType),
      StructField("title", StringType),
      StructField("authors", StringType),
      StructField("average_rating", DoubleType),
      StructField("isbn", LongType),
      StructField("isbn13", LongType),
      StructField("language_code", StringType),
      StructField("num_pages", IntegerType),
      StructField("ratings_count", LongType),
      StructField("text_reviews_count", LongType),
      StructField("publication_date", StringType),
      StructField("publisher", StringType)))

    val booksDF = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(booksSchema)
      .csv("D:/Documentos/Spark/scala/books/input/books.csv")

    booksDF.printSchema()

    val totalBooks = booksDF.count()
    println(s"Total de libros en nuestra biblioteca: $totalBooks")

    val best_condition = booksDF.col("average_rating") > 4.5 && booksDF.col("ratings_count") > 30000

    println("Los mejores libros:")
    val bestBooksDF = booksDF
      .select(
        functions.col("title"),
        functions.col("authors"),
        functions.col("bookID"),
        functions.col("publication_date"),
        functions.col("average_rating"))
      .filter(best_condition)
      .orderBy(functions.desc("ratings_count"))
      .cache()

    bestBooksDF.show()

    println("Libros más votados:")
    val mostVotedDF = booksDF
      .select(
        functions.col("bookID"),
        functions.col("title"),
        functions.col("authors"),
        functions.col("publication_date"),
        functions.col("ratings_count")
      )
      .orderBy(functions.desc("ratings_count"))
      .cache()

    mostVotedDF.show(10)

    spark.stop()

  }

}
