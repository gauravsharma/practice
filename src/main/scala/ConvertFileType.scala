import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object ConvertFileType extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "ConvertFileType")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val lines = spark.read
    .option("header", "true")
    .option("inferSchema", true)
    .option("path", "/home/gaurav/Documents/BigData/datasets/Groceries_dataset.csv")
    .load

  lines.write.partitionBy("subject")
    .parquet("/home/gaurav/Documents/BigData/datasets/Groceries_dataset_parquet")

  scala.io.StdIn.readLine()
}
