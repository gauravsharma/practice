//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html
//Develop a standalone Spark SQL application (using IntelliJ IDEA) that finds the ids of the rows that have values
// of one column in an array column.

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array_contains, col, collect_list}

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

//  input.printSchema()
  input.show(false)

  input.createOrReplaceTempView("sqlInp")

  val orgWords = spark.sql(
    """
        SELECT id, explode(split(words, ',')) as splitted_words, word FROM sqlInp
      """).toDF()

//  orgWords.show()

  val sqlForOrgWords = orgWords.groupBy("splitted_words").agg(collect_list("id").as("collected_ids"))

  sqlForOrgWords.createOrReplaceTempView("ft")

  //below query gives the words which are repeating in other column
//  val reqdWords = spark.sql(
//    """
//      SELECT distinct swt.word as dw FROM
//      (SELECT id, explode(split(words, ',')) as splitted_words, word FROM sqlInp) swt
//      WHERE swt.word IN (SELECT explode(split(words, ',')) FROM sqlInp)
//      """).show(false)

  val finalSql = spark.sql(
    """
      SELECT * FROM ft WHERE splitted_words IN (SELECT distinct swt.word as dw FROM
            (SELECT id, explode(split(words, ',')) as splitted_words, word FROM sqlInp) swt
            WHERE swt.word IN (SELECT explode(split(words, ',')) FROM sqlInp)) order by splitted_words
      """
  ).show(false)

  spark.stop()
}
