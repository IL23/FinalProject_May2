package com.github.IL23.FinalProject

import scala.io.Source
import org.apache.spark.sql.{DataFrame, SparkSession}


object FileProcessing {

  def getDFFromJSON(filePath: String, spark: SparkSession): DataFrame = {
    println(s"Opening JSON file: $filePath")
    val DF: DataFrame = spark.read.option("multiline", "true").json(filePath)
    DF
  }

  def getDataFromJSON(filePath: String, spark: SparkSession): Seq[Grocery] = {
    val DF = getDFFromJSON(filePath, spark)
    import spark.implicits._
    DF.as[Grocery].collect().toSeq
  }

  def textToGrocery (txt: String): Grocery = {
    val tokens = txt.split(",")
    val sz = tokens.size
    val id = tokens(0).toInt
    val product = (1 to sz-3).map(tokens(_)).mkString(",")
    val country =  tokens(sz-2)
    val price = tokens(sz-1).toDouble
    Grocery(id, product, country, price)
  }

  def getDataFromTxt (filePath: String): Seq[Grocery] = {
    println(s"Opening txt file: $filePath")
    val lines: Seq[String] = Source.fromFile(filePath).getLines.toSeq
    lines.map(textToGrocery)
  }

  def writeFile(filename:String, s:String): Unit = {
    val file = new java.io.File(filename)
    val bw = new java.io.BufferedWriter(new java.io.FileWriter(file))
    bw.write(s)
    bw.close()
  }

  def writeDFtoCSV(df: DataFrame, filepath:String): Unit = {
    df.write.format("csv").save(filepath)
  }

}
