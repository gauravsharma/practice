//Write a structured query that “merges” two rows of the same id (to replace nulls).
//https://jaceklaskowski.github.io/spark-workshop/exercises/sql/merge-two-rows.html

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MergeTwoRows extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "MergeTwoRows")
  sparkConf.set("spark.master", "local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._
  val input = Seq(
    ("100","John", Some(35),None),
    ("100","John", None,Some("Georgia")),
    ("101","Mike", Some(25),None),
    ("101","Mike", None,Some("New York")),
    ("103","Mary", Some(22),None),
    ("103","Mary", None,Some("Texas")),
    ("104","Smith", Some(25),None),
    ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")

  val table = input.createOrReplaceTempView("Records")

  println("original dataset")
  input.show(false)

  println("stack overflow's answer")
  spark.sql(
    """
      select id, name, age, city from (
            select max(age) as age,
             max(city) as city,
             max(name) as name,
             id
      from Records
      group by id
      order by id
      ) finalTT
      """).show(false)

  println("desired output")
  spark.sql(
    """
       select * from (select
         t1.id, t1.name,
         coalesce(t2.age,t1.age),
         coalesce(t2.city,t1.city)
       from
         (select * from Records where age is NULL) t1
         join (select * from Records where city is NULL) t2 on (t1.id = t2.id)
      UNION
        select
          tt1.id, tt1.name,
          tt1.age,
          tt1.city
        from
          Records tt1
        left join (
          select
            t1.id, t1.name,
            coalesce(t2.age,t1.age),
            coalesce(t2.city,t1.city)
          from
            (select * from Records where age is NULL) t1
            join (select * from Records where city is NULL) t2 on (t1.id = t2.id)
        ) tt2 on tt1.id = tt2.id
        where
          tt2.id IS NULL) finalTable order by finalTable.id ASC
      """).show(false)
}
