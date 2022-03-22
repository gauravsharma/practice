import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Groceries extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val fileSchema = StructType(Array(
    StructField("Member_number", IntegerType, true),
    StructField("Date", DateType, true),
    StructField("itemDescription", StringType, true)
  ))

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Groceries")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val lines = spark.read
    .option("header", "true")
    .schema(fileSchema)
    .csv("/home/gaurav/Documents/BigData/datasets/Groceries_dataset.csv")
//  lines.printSchema();
  lines.groupBy("Member_number", "Date")
    .agg(concat_ws(",", collect_list("itemDescription")).alias("ItemDescription"))
    .select(col("Member_number"), date_format(col("Date"), "MM-dd-yyyy").alias("OrderDate"), col("ItemDescription"))
    .orderBy(col("Member_number").desc, col("OrderDate"))
    .show(false)


//  println("hi testing testing one two three")

  spark.stop()
}
