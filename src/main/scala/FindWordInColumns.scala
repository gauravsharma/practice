//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html
//Develop a standalone Spark SQL application (using IntelliJ IDEA) that finds the ids of the rows that have values
// of one column in an array column.

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FindWordInColumns extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "ExplodeStruct")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._
  val input = Seq(
    ("1","one,two,three","one"),
    ("2","four,one,five","six"),
    ("3","seven,nine,one,two","eight"),
    ("4","two,three,five","five"),
    ("5","six,five,one","seven")
  ).toDF("id", "words", "word")

  input.printSchema()
  input.show(false)

  input.createOrReplaceTempView("sqlInp")

  spark.sql(
    """
      SELECT id, word from sqlInp WHERE word IN (select (explode(split(words, ','))) FROM sqlInp)
      """)
    .show(false)

  spark.stop()
}
