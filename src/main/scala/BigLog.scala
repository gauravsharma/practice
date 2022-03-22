import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BigLog {
  case class Logging(level:String, datetime:String)

  def mapper(line:String):Logging = {
    val fields = line.split(',')

    val logging:Logging = Logging(fields(0), fields(1))
    logging
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "LoggingLevel")
    sparkConf.set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    val lines = spark.read
      .option("header", "true")
      .csv("/home/gaurav/Documents/BigData/week-12/datasets/biglog-201105-152517.txt")
    //  lines.printSchema();

    //grouping based on logging level and month
    lines.createOrReplaceTempView("logging_table")
    val logs = spark.sql("select level, date_format(datetime, 'MMM') as month, count(1) as total from logging_table group by level, month")
    //logs.show(100, false)

    //grouping based on logging level and month, order by month.
    lines.createOrReplaceTempView("new_logging_table")
    val orderedLogs = spark.sql("""
        select level,
          date_format(datetime, 'MMM') as month,
          cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total
        from
          new_logging_table
        group by level, month
        order by monthnum, level, total
        """).drop("monthnum")
    //orderedLogs.show(100, false)

    //pivot table
    val orderedPivotLogs = spark.sql("""
        select level,
          date_format(datetime, 'MMMM') as month
        from
          new_logging_table
        """).groupBy("level")
      .pivot("month", List("January" , "February" , "March" , "April" , "May" , "June" , "July" , "August" , "September" , "October" , "November" , "December"))
      .count()

    orderedPivotLogs.show(100, false)


    spark.stop()
  }

}
