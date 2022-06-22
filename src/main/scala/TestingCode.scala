import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

case class Donut(name:String, tasteLevel:String)

object TestingCode extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

//  println("Using String interpolation on object properties")
//  val favouriteFood:Donut = Donut("Glazed Donut", "Very Tasty")
//  println(s"my favourite donut name is '${favouriteFood.name}' and it is ${favouriteFood.tasteLevel}")

  val donutName: String = "Vanilla Donut"
  val donutTasteLevel: String = "Tasty"
  println("\nStep 6: Using raw interpolation")
  println(raw"Favorite donut\t$donutName")

  val outputDF = Seq(
    Row(List(Row("879", "R Systems")), List(Row("Toyota", "Dollars")))
    );
  val arrayStructSchema = new StructType()
    .add("serviceAppointments",ArrayType(new StructType().add("appointmentNumber", StringType).add("companyName", StringType)))
    .add("purchase",ArrayType(new StructType().add("make", StringType).add("purchaseDate", LongType)))

  val df = spark.createDataFrame(spark.sparkContext
    .parallelize(arrayStructData),arrayStructSchema)
  df.printSchema()
  df.show()



//  println("Printing the contents of List")
//  val tempList:List[String] = List("word1", "word2", "gaurav", "neha", "Samarth")
//
//  for (tempListVariable <- tempList) {
//    println(tempListVariable.capitalize)
//  }

}
