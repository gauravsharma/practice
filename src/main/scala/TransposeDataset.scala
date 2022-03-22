//structured query that “transpose” a dataset so a new dataset uses column names and values from a struct column.
//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/structs-for-column-names-and-values.html

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object TransposeDataset extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class MovieRatings(movieName: String, rating: Double)
  case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "TransposeDataset")
  sparkConf.set("spark.master", "local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val movies_critics = Seq(
    MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
    MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))),
    MovieCritics("Gaurav", Seq(MovieRatings("Logan", 3), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 5))),
    MovieCritics("Neha", Seq(MovieRatings("Logan", 1), MovieRatings("Zoolander", 1), MovieRatings("John Wick", 2)))
  )
  println("original dataset")
  import spark.implicits._
  movies_critics.toDF.show(false)

  println("desired output")
  val ratings = movies_critics.toDF.withColumn("explodedRatings", expr("explode(movieRatings)"))
    .withColumn("rating", col("explodedRatings.rating"))
    .withColumn("movieName", col("explodedRatings.movieName"))
    .drop("movieRatings","explodedRatings")
    .groupBy("name")
    .pivot("movieName")
    .max("rating")

  ratings.show(truncate = false)
}

