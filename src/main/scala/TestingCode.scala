import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType, StringType, StructType}

case class Donut(name:String, tasteLevel:String)

object TestingCode extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Groceries")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val outputDF = Seq(
    Row(List(Row("879", "R Systems")), List(Row("Toyota Truck", "Yes")))
    )
  print(outputDF)

  println("\n")
  val arrayStructSchema = new StructType()
    .add("serviceAppointments",
      ArrayType(new StructType()
        .add("appointmentNumber", StringType)
        .add("companyName", StringType)))
    .add("purchase",
      ArrayType(new StructType()
        .add("make", StringType)
        .add("MailOptOut", BooleanType)))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(outputDF), arrayStructSchema)
  df.printSchema()

  println(df.select("purchase.make"))

  spark.stop()
}
