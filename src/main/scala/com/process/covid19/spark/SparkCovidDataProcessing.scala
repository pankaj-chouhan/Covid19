package com.process.covid19.spark

import org.apache.spark.sql.SparkSession

object SparkCovidDataProcessing {
  def main(args: Array[String]): Unit = {
    //val url= "https://api.covid19api.com/dayone/country/sweden/status/confirmed/live"
    //val url="https://api.covid19india.org/travel_history.json"
    //val url="https://api.covid19api.com/all"
    //val url = "https://api.covid19india.org/data.json"
    val result = RestAPICall.covid19apiIndiaStateDistrict()


    // FileWriter
    /*val file = new File("C:/Users/Aigometri/Desktop/Test/test.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(result)
    bw.close()*/
     System.setProperty("hadoop.home.dir","C:\\hadoop" )
    val spark=SparkSession.builder().master("local").appName("readJson").getOrCreate()
    import spark.implicits._
    val df = spark.read.json(Seq(result).toDS)
    //val df=spark.read.json("C:/Users/Aigometri/Desktop/Test/test.json")
    df.printSchema()
    df.registerTempTable("data")

    //val a=spark.sql("select  data.Andaman from data").show(50,false)
    val b=df.select("Andaman and Nicobar Islands").explode().show(50,false)

   // a.write.csv("C:/Users/Aigometri/Desktop/Test/csv")
    //val df2=df.select("Active","Country","Confirmed","Deaths","Recovered")//.show(100)

   //val df2=df.select.("statewise").explode().show(false)
    //val df3=df.select().
    //df2.write.csv("C:/Users/Aigometri/Desktop/Test/csv")
    //df2.coalesce(1).write.csv("Data/csv")
    //df2.coalesce(1).write.option("header", "true").csv("Data/sample_file.csv")
    spark.stop()
  }
}
