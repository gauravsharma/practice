//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/split-function-with-variable-delimiter-per-row.html
//split function with variable delimiter per row

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, expr, lit}

object SplitWithVariableDelimiter extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "SplitWithVariableDelimiter")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("Values", "Delimiter");
  dept.show()

  dept.createOrReplaceTempView("splitviews")
  val splitValues = dept
    .withColumn("processedDelimiter", concat(lit("\\"), col("Delimiter")))
    //below line will remove empty elements as well
    .withColumn("splitVals", expr("array_remove(split(Values, processedDelimiter),'')"))
    .select(col("Values"), col("Delimiter"), col("splitVals"));
  splitValues.show(false)
}
