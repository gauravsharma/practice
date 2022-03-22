//open a CSV file and write a parquet file out of it.

import scala.io.Source
import org.apache.log4j.{Level, Logger}

object CsvToParquet extends App {
  case class Record(order_number:String, order_data:String, item_name:String, quantity:String, product_price:String, total_products:String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val csvFilePath:String = "/home/gaurav/Documents/BigData/datasets/restaurant-1-orders-small.csv"

  println("Order Number, Order Date, Item Name, Quantity, Product Price, Total products")

  val bufferedSource = Source.fromFile(csvFilePath)

  //drop method ignores the first line of csv file.
  for (line <- bufferedSource.getLines.drop(1)) {
    val cols = line.split(",").map(_.trim)
//    val records:Iterator[Record] = cols.map(row => Record(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5)))
    // do whatever you want with the columns here
    //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
    val listDf = Seq(cols)
    println(listDf)
  }

  bufferedSource.close
}
