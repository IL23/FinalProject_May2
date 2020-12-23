package com.github.IL23.FinalProject

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataProcessing {

  def getMaxPriceDF(df: DataFrame, spark: SparkSession): Grocery = {
    import spark.implicits._
    df.createOrReplaceTempView("DFview")
    val expensive: DataFrame = spark.sql("SELECT * FROM DFview WHERE price = (SELECT MAX(price) FROM DFview)")
    val maxPriceGrocery: Grocery = expensive.as[Grocery].head()
    maxPriceGrocery
  }

  def getTopCountryDF(df: DataFrame, spark: SparkSession): String = {
    val countriesCounted: DataFrame = df.groupBy("country").count
    countriesCounted.show()
    countriesCounted.createOrReplaceTempView("countriesView")
    val topCount: DataFrame = spark.sql("SELECT * FROM countriesView WHERE count = (SELECT MAX(count) FROM countriesView)")
    val topCountry: String = topCount.head().toString()
    topCountry
  }

  def getMaxPrice(groceries: Seq[Grocery]): Grocery = {
//    val groceries: Seq[Grocery] = filepath.split('.').last match {
//      case "json" => FileProcessing.getDataFromJSON(filepath, spark)
//      case "txt" => FileProcessing.getDataFromTxt(filepath)
//    }
    var maxPrice = 0.0
    var maxPriceGrocery = groceries.head
    for (grocery <- groceries) {
      if (grocery.price > maxPrice) {
        maxPrice = grocery.price
        maxPriceGrocery = grocery
      }
    }
    maxPriceGrocery
  }


  def getTopCountry(groceries: Seq[Grocery]): (String, Int) = {
    groceries.map(_.country)
      .groupBy(identity)
      .mapValues(_.size)
      .maxBy(_._2)
  }

}
