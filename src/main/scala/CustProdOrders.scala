import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.io
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object CustProdOrders extends App {
//  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Groceries")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("path", "/home/gaurav/Documents/BigData/datasets/products-orders-customers/olist_orders_dataset.csv")
    .load()

//  ordersDf.show()

  val customersDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("path", "/home/gaurav/Documents/BigData/datasets/products-orders-customers/olist_customers_dataset.csv")
    .load()

//  customersDf.show()

  ordersDf.join(customersDf, ordersDf.col("customer_id") === customersDf.col("customer_id"), "inner")
    .select(ordersDf.col("customer_id"), customersDf.col("customer_state"), ordersDf.col("order_id"), ordersDf.col("order_status"))
    .sort("customer_state")
    .show(false)

  scala.io.StdIn.readLine()
  spark.stop()
}
