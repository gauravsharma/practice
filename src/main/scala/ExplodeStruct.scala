//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/explode-structs-array.html
//Write a structured query that “explodes” an array of structs (of open and close hours).

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{array, col, expr, split, struct}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

object ExplodeStruct extends App {

  def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    println(schema)
    schema.fields.flatMap(f => {
      println("In flatmap")
      println(f.name)

      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      if (f.name == "Friday") {
  println("in friday hooraayyy")
        println(f.name.apply(0))
      }

      f.dataType match {
        case st: StructType => {
          flattenStructSchema(st, columnName)
        }
        case _ => Array(col(columnName).as(columnName.replace(".","_")))
      }
    })
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "ExplodeStruct")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val arrayStructSchema = new StructType().add("business_id",StringType)
    .add("full_address", StringType)
    .add("day",ArrayType(new StructType()
      .add("open_time",StringType)
      .add("close_time",StringType)
      ))

  val lines = spark.read
    .option("multiline",true)
    .json("/home/gaurav/Documents/BigData/datasets/input.json")
    .toDF("business_id","full_address","day")

//  spark.createDataFrame(
//    spark.sparkContext.parallelize(lines, arrayStructSchema)
//  )

  lines.show(false)
  lines.printSchema()

//  val splittedHours = lines
//    .withColumn("open_time", col("hours.Friday.open"))
//    .withColumn("close_time", col("hours.Friday.close"))
//    .drop("hours")
//    .show(false)

  lines
    .select(col("business_id"), col("full_address"),  col("day"))
//    .withColumn("day", getWeekDayName(col("day"))
//    .withColumn("open_time", "hours.Friday.open")
    .select(flattenStructSchema(lines.schema):_*)
//    .select(creatArrayDaySchema(col("day")):_*)
      .show(false)

  spark.stop()

}
