package com.process.covid19.spark

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructType}

object SparkCovidDataProcessing {

  def main(args: Array[String]): Unit = {

    val SwedenConfirmedCase = RestAPICall.covid19apiSwedenStatusLive()
    val IndiaConfirmedCase = RestAPICall.covid19apiIndiaStatusLive()
    val AllConfirmedCase = RestAPICall.covid19apiAllCountriesTimeSeriesData()
    val StatewiseIndia = RestAPICall.covid19apiIndiaStateDistrict()
    val IndaiTravelHistory=RestAPICall.covid19apiIndiaTravelHistory()

    // FileWriter if want to write te response in directory
    /*val file = new File("C:/Users/Aigometri/Desktop/Test/test.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(result)
    bw.close()*/


    System.setProperty("hadoop.home.dir","C:\\hadoop" )
    val spark=SparkSession.builder().master("local").appName("readJson").getOrCreate()
    //def json(jsonDataset: Dataset[String]): DataFrame
    import spark.implicits._
    val SwedDF = spark.read.json(Seq(SwedenConfirmedCase).toDS())
    val IndDF = spark.read.json(Seq(IndiaConfirmedCase).toDS())
    val All = spark.read.json(Seq(AllConfirmedCase).toDS())
    val IndTravelHist=spark.read.json(Seq(IndaiTravelHistory).toDS())
    import spark.implicits._


    SwedDF.createOrReplaceTempView("SwedenData")
    IndDF.createOrReplaceTempView("IndData") //schema (Cases,City,CityCode,Country,CountryCode,Date,Lat,Lon,Province,Status)
    All.createOrReplaceTempView("AllCountry") //Schema (Active,City,CityCode,Confirmed,Country,CountryCode,Date,Deaths,Lat,Lon,Province,Recovered)
    IndTravelHist.createOrReplaceTempView("TravelHistory")

    val sw=spark.sql("select * from SwedenData")
    val In=spark.sql("select * from IndData")
   val AllCountries=spark.sql("select * from AllCountry")


    val df=spark.sql("select explode(raw_data) from TravelHistory").toDF("data")
    df.createOrReplaceTempView("travel")

    df.printSchema()
    val travelDF=spark.sql("select data.* from travel")////Schema (agebracket,backupnotes,contractedfromwhichpatientsuspected,currentstatus,dateannounced,detectedcity,detecteddistrict,detectedstate,estimatedonsetdate,gender,nationality,notes,patientnumber,source1,source2,source3,statecode,statepatientnumber,statuschangedate,typeoftransmission)

    val window = Window.orderBy("Date")
    val leadCol = lead(col("Cases"), 1).over(window)
    val lagCol = lag(col("Cases"), 1).over(window)
    val InDFSelected=IndDF.select("Country","Date","Cases")
    val newCases=InDFSelected.withColumn("NewCases", leadCol-lagCol)//.orderBy(desc("NewCases"))//.show(50)
    //Displaying the maximum number of new cases in date
    newCases.select("Date","Country","NewCases").orderBy(desc("NewCases")).show(50)

    //Displaying the current status all over the world
    val currentStatus=spark.sql("select Date,Country,Confirmed,Deaths,Recovered from AllCountry where Date = (select Max(Date) from AllCountry)")
    currentStatus.show()
    currentStatus.createOrReplaceTempView("CountryStatus")

    //Displaying death and recovery rate country wise in %
    val DeathRecoveredRate=spark.sql("select Date,Country,Deaths/Confirmed as Death_Rate,Recovered/Confirmed as Recovery_Rate from CountryStatus").show()
    //val DeathRecoveredRate=currentStatus.select("Country").withColumn("Death Rate %",col("Confirmed")/col("Deaths")).show()
    travelDF.cache()
    // Average time to recover in age group
   val withAgegroup = travelDF.withColumn("Age_group", expr("case when agebracket >=0 and agebracket <=10 then '0-10' " +
      "when agebracket >=11 and agebracket <= 20 then '11-20' " +
      "when agebracket >=21 and agebracket <=30 then '21-30' " +
      "when agebracket >=31 and agebracket <= 40 then '31-40' " +
      "when agebracket >=41 and agebracket <= 50 then '41-50' " +
      "else '51-100' end"))
    val withRecoverTime=withAgegroup.withColumn("datesDiff", datediff(to_date(col("statuschangedate"),"dd/MM/yyyy"),to_date(col("dateannounced"),"dd/MM/yyyy")))
    withRecoverTime.createOrReplaceTempView("finalTable")
    val AvgRecoverTime=spark.sql("select Age_group,avg(datesDiff) as Recovery_Average_days from finalTable where currentstatus='Recovered' group by Age_group order by Recovery_Average_days desc").show()


    //Saving the file in csv for the Dashboard creation

    sw.coalesce(1).write.option("header", "true").mode(SaveMode.Append).csv("C:/Users/Aigometri/Desktop/Test/csv")
    In.coalesce(1).write.option("header", "true").mode(SaveMode.Append).csv("C:/Users/Aigometri/Desktop/Test/csv")
    AllCountries.coalesce(1).write.option("header", "true").mode(SaveMode.Append).csv("C:/Users/Aigometri/Desktop/Test/Allcsv")
    travelDF.coalesce(1).write.option("header", "true").mode(SaveMode.Append).csv("C:/Users/Aigometri/Desktop/Test/travel")

    spark.stop()

  }
}