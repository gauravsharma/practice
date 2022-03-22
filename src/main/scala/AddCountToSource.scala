//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/selecting-the-most-important-rows-per-assigned-priority.html
//Selecting the most important rows per assigned priority

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, count, expr}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window

object AddCountToSource extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "AddCountToSource")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val input = Seq(
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
    ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
    ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2))
    .toDF("column0", "column1", "column2", "label")

  input.createOrReplaceTempView("sqlInp")

  //using spark sql
  val addedCol = spark.sql("Select column0, column1, column2, label, count(label) over (partition by column1, column2) as count from sqlInp ")

  //using spark structured api
  val col1and2 = Window.partitionBy("column1", "column2")
  val sql2 = input.withColumn("count", count(col("label")) over col1and2)

  addedCol.show(false)
}
