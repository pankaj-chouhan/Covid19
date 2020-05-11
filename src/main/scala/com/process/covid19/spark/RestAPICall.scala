package com.process.covid19.spark

object RestAPICall {

    def covid19apiSwedenStatusLive (): String = {
      val url= "https://api.covid19api.com/dayone/country/sweden/status/confirmed/live"
      val result = scala.io.Source.fromURL(url).mkString.trim.stripLeading().stripTrailing().replaceAll("\n","")
      return result
    }
  def covid19apiIndiaStatusLive (): String = {
    val url= "https://api.covid19api.com/dayone/country/india/status/confirmed/live"
    val result = scala.io.Source.fromURL(url).mkString.trim.stripLeading().stripTrailing().replaceAll("\n","")
    return result
  }
  def covid19apiIndiaStateDistrict (): String = {
    val url= "https://api.covid19india.org/state_district_wise.json"
    val result = scala.io.Source.fromURL(url).mkString.trim.stripLeading().stripTrailing().replaceAll("\n","")
    return result
  }
  def covid19apiAllCountriesTimeSeriesData (): String = {
    val url= "https://api.covid19api.com/all"
    val result = scala.io.Source.fromURL(url).mkString.trim.stripLeading().stripTrailing().replaceAll("\n","")
    return result
  }
  def covid19apiIndiaTravelHistory (): String = {
    val url= "https://api.covid19india.org/raw_data.json"
    val result = scala.io.Source.fromURL(url).mkString.trim.stripLeading().stripTrailing().replaceAll("\n","")
    return result
  }


}
