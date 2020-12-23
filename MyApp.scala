package com.github.IL23.FinalProject

import org.apache.spark.sql.SparkSession

object MyApp {
  def main(args: Array[String]): Unit = {
    println(s"Starting the App with ${args.length} arguments.")
    for (arg <- args) {
      println(s"My arg is $arg")
    }

    if (args.length >= 2) {
      val spark = init(Array("", ""))
      val result: Report = Report("c:/temp/MyReport2.txt", args(1), args(0),spark)
      result.reportToFile()
      FileProcessing.writeDFtoCSV(result.jsonFileDF,"c:/temp/GroceriesDF.csv")
      FileProcessing.writeFile("c:/temp/GroceriesTXT.csv", result.txtFileData.map(d=> s"${d.id.toString},${d.product},${d.country},${d.price.toString}").mkString("\n"))
    }

    def init (configArguments: Array[String]): SparkSession ={
      val spark: SparkSession = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
      spark
    }

  }

}
