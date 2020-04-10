package com.json.read

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import java.sql.Date
import java.io._
object SparkCovidDataProcessing {
  def main(args: Array[String]): Unit = {
    val url= "https://api.covid19api.com/dayone/country/india/status/confirmed/live"
    //val url = "https://api.covid19india.org/data.json"
    val result = scala.io.Source.fromURL(url).mkString
println(result)

    // FileWriter
    val file = new File("C:/Users/Aigometri/Desktop/Test/test.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(result)
    bw.close()
    System.setProperty("hadoop.home.dir","C:\\hadoop" )
    val spark=SparkSession.builder().master("local").appName("readJson").getOrCreate()
    val df=spark.read.json("C:/Users/Aigometri/Desktop/Test/test.json")
    df.printSchema()
df.registerTempTable("data")

    val a=spark.sql("select * from data")
    a.write.csv("C:/Users/Aigometri/Desktop/Test/csv")
    //val df2=df.select("Cases","Country","CountryCode","Date","Status").show(100)
    spark.stop()
  }
}
