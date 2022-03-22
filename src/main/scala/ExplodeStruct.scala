//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/explode-structs-array.html
//Write a structured query that “explodes” an array of structs (of open and close hours).

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr, split, struct}
import org.apache.spark.sql.types.StructType

object ExplodeStruct extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "ExplodeStruct")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val lines = spark.read
    .option("multiline",true)
    .json("/home/gaurav/Documents/BigData/datasets/input.json").toDF("business_id","full_address","hours")

  lines.show(false)
  lines.printSchema()

  val splittedHours = lines
    .withColumn("open_time", col("hours._name.open"))
    .withColumn("close_time", col("hours._name.close"))
    .drop("hours")
    .show(false)

//  lines.toDF().select("business_id, full_address")
//    .withColumn("day", "cast(hours as string)")
//    .withColumn("open_time", " hours.Friday.open")
////    .withColumn("explodedHours", expr("explode(hours)"))
//    .show(false)

  spark.stop()

}
