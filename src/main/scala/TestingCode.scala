//below code converts every first alphabet of every word in list to capital i.e. Word1, Word2

import org.apache.log4j.{Level, Logger}

object TestingCode extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val tempList:List[String] = List("word1", "word2", "gaurav", "neha", "samarth")

  for (tempListVariable <- tempList) {
    println(tempListVariable.capitalize)
  }

}
