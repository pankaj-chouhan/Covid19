package com.process.covid19.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructType}
object SparkCovidDataProcessing {

  def main(args: Array[String]): Unit = {

    val result = RestAPICall.covid19apiIndiaStateDistrict()

    // FileWriter
    /*val file = new File("C:/Users/Aigometri/Desktop/Test/test.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(result)
    bw.close()*/
    val structureSchema = new StructType()
      .add("Country",new ArrayType())
        .add("DistrictName",new StructType())
          .add("Confirmed",LongType)
            .add("Delta",new StructType())
              .add("confirmed",StringType)
          .add("LastUpdated",StringType))

    structureSchema.printTreeString()




    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

     System.setProperty("hadoop.home.dir","C:\\hadoop" )
    val spark=SparkSession.builder().master("local").appName("readJson").getOrCreate()
    //def json(jsonDataset: Dataset[String]): DataFrame
    import spark.implicits._


    val jsonStringDs = spark.createDataset[String](
      Seq(result))
      jsonStringDs.show(false)
    //val df = spark.read.schema(structureSchema).json(Seq(result).toDS)

    val df = spark.read.json(jsonStringDs)   //(jsonStringDs)
    //val df=spark.read.json("C:/Users/Aigometri/Desktop/Test/test.json")
    df.printSchema()
    df.show(false)
    df.registerTempTable("data")

    //val a=spark.sql("select  data.Andaman from data").show(50,false)
    //val b=df.select("Country.").show(10,false)
    //val c=df.withColumn("confirmed",col("Andaman and Nicobar Islands.districtData.North and Middle Andaman.confirmed"))
    //b.printSchema()
    //df.withColumn("vars", explode(arrays_zip($"varA", $"varB")))//.select(

   // a.write.csv("C:/Users/Aigometri/Desktop/Test/csv")
    //val df2=df.select("Active","Country","Confirmed","Deaths","Recovered")//.show(100)

   //val df2=spark.sql("select * from data").show(10,false)
    //val df3=df.select().
    //df2.write.csv("C:/Users/Aigometri/Desktop/Test/csv")
    //df2.coalesce(1).write.csv("Data/csv")
    //df2.coalesce(1).write.option("header", "true").csv("Data/sample_file.csv")
    spark.stop()
  }
}
