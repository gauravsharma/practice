//Selecting the most important rows per assigned priority
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, concat, expr, lit}

object SelectImpRows extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "SelectImpRows")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  val priorities = Seq(
    "MV1",
    "MV2",
    "VPV",
    "Others").zipWithIndex.toDF("name", "rank")
  priorities.show(false)

  val input = Seq(
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others")).toDF("id", "value")

  input.show(false)

  val joined = input.join(priorities)
    .where(col("name") === col("value"))
    .select("id", "value", "rank").groupBy("id").agg(functions.min("rank") as "mini")

  val finalDf = joined.join(priorities).where($"mini" === $"rank").select("id", "name");
  finalDf.show(false)
}
