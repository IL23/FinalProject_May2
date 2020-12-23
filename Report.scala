package com.github.IL23.FinalProject

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Report(filePath:String = "", txtFile: String, jsonFile: String, spark: SparkSession) {
  val txtFileData: Seq[Grocery] = FileProcessing.getDataFromTxt(txtFile)
  val jsonFileData: Seq[Grocery] = FileProcessing.getDataFromJSON(jsonFile, spark)
  val jsonFileDF: DataFrame = FileProcessing.getDFFromJSON(jsonFile,spark)

  def txtFileResults(): String= {
    val file: String = s"Processing file $txtFile \n"
    val price: String = s"The most expensive product is ${DataProcessing.getMaxPrice(txtFileData)} \n"
    val country: String = s"Top country is ${DataProcessing.getTopCountry(txtFileData)} \n"
    file + price + country
  }

  def jsonFileResults(): String = {
    val file: String = s"Processing file $jsonFile \n"
    val price: String = s"The most expensive product is ${DataProcessing.getMaxPrice(jsonFileData)} \n"
    val country: String = s"Top country is ${DataProcessing.getTopCountry(jsonFileData)} \n"
    val priceDF: String = s"The most expensive product (DF) is ${DataProcessing.getMaxPriceDF(jsonFileDF, spark)} \n"
    val countryDF: String = s"Top country (DF) is ${DataProcessing.getTopCountryDF(jsonFileDF, spark) } \n"
    file + price + country + priceDF + countryDF
  }

  def reportToFile (): Unit = {
    if (filePath=="") {
      println(s"==Final report== \n\nResults from txt file: \n${txtFileResults()}\nResults from JSON file: \n ${jsonFileResults()}")
    } else {
      val report = s"==Final report== \nResults from txt file: \n${txtFileResults()}\nResults from JSON file: \n${jsonFileResults()}"
      FileProcessing.writeFile(filePath, report)
    }
  }

}
