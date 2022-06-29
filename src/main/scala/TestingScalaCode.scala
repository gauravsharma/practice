object TestingScalaCode extends App {
  println("Using String interpolation on object properties")
  val favouriteFood: Donut = Donut("Glazed Donut", "Very Tasty")
  println(s"my favourite donut name is '${favouriteFood.name}' and it is ${favouriteFood.tasteLevel}")

  val donutName: String = "Vanilla Donut"
  val donutTasteLevel: String = "Tasty"
  println("\nStep 6: Using raw interpolation")
  println(raw"Favorite donut\t$donutName")

  //  println("Printing the contents of List")
  //  val tempList:List[String] = List("word1", "word2", "gaurav", "neha", "Samarth")
  //
  //  for (tempListVariable <- tempList) {
  //    println(tempListVariable.capitalize)
  //  }


  val array1 = Array.range(10, 60, 10)

  // 50, 20, 30, 40, 10
  for (elem <- array1) {
    println(elem)
  }
}
